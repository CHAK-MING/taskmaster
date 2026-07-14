#include "dagforge/app/application.hpp"

#include "dagforge/app/api/api_server.hpp"
#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/executor/command_executor.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/executors/command_adapter.hpp"
#include "dagforge/workflow/executors/http_adapter.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <utility>

namespace dagforge {

Application::Application() : Application(SystemConfig{}) {}

Application::Application(SystemConfig config) : config_(std::move(config)) {
  (void)rebuild_components();
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  if (is_running()) {
    return fail(Error::InvalidState);
  }
  auto loaded = SystemConfigLoader::load_from_file(path);
  if (!loaded) {
    return fail(loaded.error());
  }
  config_ = std::move(*loaded);
  return rebuild_components();
}

auto Application::config() const noexcept -> const SystemConfig & {
  return config_;
}

auto Application::config() noexcept -> SystemConfig & { return config_; }

auto Application::rebuild_components() -> Result<void> {
  api_.reset();
  workflow_runtime_.reset();
  workflow_control_plane_.reset();
  executor_registry_.reset();
  command_executor_.reset();
  runtime_.reset();

  const auto shard_count =
      config_.runtime.shards > 0
          ? static_cast<unsigned>(config_.runtime.shards)
          : std::max(1U, std::thread::hardware_concurrency());
  runtime_ = std::make_unique<Runtime>(
      shard_count, config_.runtime.pin_shards_to_cores,
      static_cast<unsigned>(config_.runtime.cpu_affinity_offset));
  auto command_executor = create_command_executor(*runtime_, config_.sandbox);
  if (!command_executor) {
    return fail(command_executor.error());
  }
  command_executor_ = std::move(*command_executor);
  executor_registry_ = std::make_unique<workflow::ExecutorRegistry>();
  auto command_adapter = workflow::create_command_executor_adapter(
      *command_executor_, config_.sandbox);
  if (!command_adapter) {
    return fail(command_adapter.error());
  }
  auto command_registered =
      executor_registry_->register_executor(std::move(*command_adapter));
  if (!command_registered) {
    return fail(command_registered.error());
  }
  if (config_.http_executor.enabled) {
    auto http_executor = workflow::create_http_executor_adapter(
        *runtime_, config_.http_executor);
    if (!http_executor) {
      return fail(http_executor.error());
    }
    auto http_registered =
        executor_registry_->register_executor(std::move(*http_executor));
    if (!http_registered) {
      return fail(http_registered.error());
    }
  }

  if (config_.workflow.enabled) {
    std::shared_ptr<workflow::IArtifactStore> artifact_store;
    std::shared_ptr<workflow::EvidenceLedger> evidence_ledger;
    std::shared_ptr<workflow::CheckpointStore> checkpoint_store;
    if (config_.storage.enabled) {
      const auto root = std::filesystem::path(config_.storage.directory);
      artifact_store =
          std::make_shared<workflow::FileArtifactStore>(root / "artifacts");
      evidence_ledger = std::make_shared<workflow::EvidenceLedger>(
          root / "evidence.jsonl", config_.storage.max_evidence_records);
      checkpoint_store =
          std::make_shared<workflow::CheckpointStore>(root / "runs");
    } else {
      artifact_store = std::make_shared<workflow::InMemoryArtifactStore>();
      evidence_ledger = std::make_shared<workflow::EvidenceLedger>(
          config_.storage.max_evidence_records);
      checkpoint_store = std::make_shared<workflow::CheckpointStore>();
    }

    workflow_control_plane_ = std::make_unique<workflow::WorkflowControlPlane>(
        *executor_registry_,
        workflow::AdmissionPolicy{config_.admission});
    workflow_runtime_ = std::make_unique<workflow::WorkflowRuntime>(
        *runtime_, *executor_registry_, std::move(artifact_store),
        std::move(evidence_ledger), checkpoint_store,
        config_.storage.max_completed_runs);

    auto checkpoints = checkpoint_store->list();
    if (!checkpoints) {
      return fail(checkpoints.error());
    }
    for (auto &checkpoint : *checkpoints) {
      auto plan = workflow_control_plane_->restore_plan(
          checkpoint.plan, checkpoint.snapshot.plan_id);
      if (!plan) {
        return fail(plan.error());
      }
      auto restored =
          workflow_runtime_->restore(*plan, std::move(checkpoint));
      if (!restored) {
        return fail(restored.error());
      }
    }
  }
  if (config_.api.enabled) {
    api_ = std::make_unique<ApiServer>(*this);
  }
  return ok();
}

auto Application::init() -> Result<void> {
  const auto api_configuration_changed =
      config_.api.enabled != static_cast<bool>(api_);
  const auto workflow_configuration_changed =
      config_.workflow.enabled != static_cast<bool>(workflow_runtime_) ||
      config_.workflow.enabled != static_cast<bool>(workflow_control_plane_);
  const auto http_executor_configuration_changed =
      executor_registry_ != nullptr &&
      config_.http_executor.enabled != executor_registry_->contains("http");
  if (!runtime_ || !command_executor_ || !executor_registry_ ||
      api_configuration_changed || workflow_configuration_changed ||
      http_executor_configuration_changed) {
    return rebuild_components();
  }
  return ok();
}

auto Application::start() -> Result<void> {
  if (running_.exchange(true, std::memory_order_acq_rel)) {
    return ok();
  }
  const auto api_configuration_changed =
      config_.api.enabled != static_cast<bool>(api_);
  const auto workflow_configuration_changed =
      config_.workflow.enabled != static_cast<bool>(workflow_runtime_) ||
      config_.workflow.enabled != static_cast<bool>(workflow_control_plane_);
  const auto http_executor_configuration_changed =
      executor_registry_ != nullptr &&
      config_.http_executor.enabled != executor_registry_->contains("http");
  if (!runtime_ || !command_executor_ || !executor_registry_ ||
      api_configuration_changed || workflow_configuration_changed ||
      http_executor_configuration_changed) {
    auto initialized = init();
    if (!initialized) {
      running_.store(false, std::memory_order_release);
      return fail(initialized.error());
    }
  }

  auto runtime_started = runtime_->start();
  if (!runtime_started) {
    running_.store(false, std::memory_order_release);
    return fail(runtime_started.error());
  }

  if (config_.api.enabled && api_) {
    auto api_started = api_->start();
    if (!api_started) {
      runtime_->stop();
      running_.store(false, std::memory_order_release);
      return fail(api_started.error());
    }
  }
  return ok();
}

auto Application::stop() noexcept -> void {
  if (!running_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  if (api_) {
    api_->stop();
  }
  if (workflow_runtime_ && runtime_ && runtime_->is_running()) {
    auto quiesced = workflow_runtime_->quiesce(std::chrono::seconds(10));
    if (!quiesced) {
      log::error("Workflow runtime shutdown did not quiesce: {}",
                 quiesced.error().message());
    }
  }
  if (command_executor_) {
    command_executor_->shutdown();
  }
  if (runtime_) {
    runtime_->stop();
  }
  workflow_runtime_.reset();
  workflow_control_plane_.reset();
}

auto Application::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Application::api_server() -> ApiServer * { return api_.get(); }

auto Application::api_server() const -> const ApiServer * { return api_.get(); }

auto Application::runtime() -> Runtime & { return *runtime_; }

auto Application::runtime() const -> const Runtime & { return *runtime_; }

auto Application::workflow_runtime() -> workflow::WorkflowRuntime * {
  return workflow_runtime_.get();
}

auto Application::workflow_runtime() const
    -> const workflow::WorkflowRuntime * {
  return workflow_runtime_.get();
}

auto Application::workflow_control_plane()
    -> workflow::WorkflowControlPlane * {
  return workflow_control_plane_.get();
}

auto Application::workflow_control_plane() const
    -> const workflow::WorkflowControlPlane * {
  return workflow_control_plane_.get();
}

} // namespace dagforge
