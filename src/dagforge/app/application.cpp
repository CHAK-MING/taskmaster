#include "dagforge/app/application.hpp"

#include "dagforge/app/api/api_server.hpp"
#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/executors/command/executor.hpp"
#include "dagforge/executors/http/executor.hpp"
#include "dagforge/workflow/artifact_store.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"
#include "dagforge/workflow/evidence_ledger.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_store.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"
#include "dagforge/util/log.hpp"

#include "../workflow/storage/detail/storage_directory_lock.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>
#include <utility>

namespace dagforge {
namespace {

struct WorkflowStores {
  std::shared_ptr<workflow::IArtifactStore> artifacts;
  std::shared_ptr<workflow::EvidenceLedger> evidence;
  std::shared_ptr<workflow::CheckpointStore> checkpoints;
  std::shared_ptr<workflow::PlanStore> plans;
};

[[nodiscard]] auto make_workflow_stores(const config::SystemConfig &config)
    -> Result<WorkflowStores> {
  if (config.storage.enabled) {
    const auto root = std::filesystem::path(config.storage.directory);
    auto artifacts = std::make_shared<workflow::FileArtifactStore>(
        root / "artifacts", config.storage.max_artifact_metadata_bytes,
        config.storage.max_artifact_bytes);
    auto reconciliation = artifacts->reconcile();
    if (!reconciliation) {
      return fail(reconciliation.error());
    }
    if (!reconciliation->clean()) {
      using State = workflow::ArtifactReconciliationState;
      log::warn(
          "Artifact reconciliation found cleanup debt: orphan_data={} "
          "orphan_metadata={} malformed_metadata={} content_mismatch={} "
          "invalid_entries={}",
          reconciliation->count(State::OrphanData),
          reconciliation->count(State::OrphanMetadata),
          reconciliation->count(State::MalformedMetadata),
          reconciliation->count(State::ContentMismatch),
          reconciliation->count(State::InvalidEntry));
    }
    auto evidence = workflow::EvidenceLedger::open(
        root / "evidence.jsonl", config.storage.max_evidence_records,
        config.storage.max_evidence_file_bytes,
        config.storage.max_evidence_record_bytes);
    if (!evidence) {
      return fail(evidence.error());
    }
    return ok(WorkflowStores{
        .artifacts = std::move(artifacts),
        .evidence = std::move(*evidence),
        .checkpoints = std::make_shared<workflow::CheckpointStore>(
            root / "runs", config.storage.max_checkpoint_bytes),
        .plans = std::make_shared<workflow::PlanStore>(
            root / "plans", config.storage.max_plan_bytes),
    });
  }
  return ok(WorkflowStores{
      .artifacts = std::make_shared<workflow::InMemoryArtifactStore>(),
      .evidence = std::make_shared<workflow::EvidenceLedger>(
          config.storage.max_evidence_records),
      .checkpoints = std::make_shared<workflow::CheckpointStore>(),
      .plans = std::make_shared<workflow::PlanStore>(),
  });
}

auto restore_workflow_state(workflow::WorkflowControlPlane &control,
                            workflow::WorkflowRuntime &runtime,
                            workflow::PlanStore &plans,
                            workflow::CheckpointStore &checkpoints)
    -> Result<void> {
  auto stored_plans = plans.list();
  if (!stored_plans) {
    return fail(stored_plans.error());
  }
  for (auto &stored : *stored_plans) {
    auto restored = control.restore_plan(std::move(stored.plan), stored.plan_id,
                                         stored.digest);
    if (!restored) {
      return fail(restored.error());
    }
  }

  auto stored_runs = checkpoints.list();
  if (!stored_runs) {
    return fail(stored_runs.error());
  }
  for (auto &checkpoint : *stored_runs) {
    auto plan = control.get_plan(checkpoint.snapshot.plan_id);
    if (!plan) {
      plan = control.restore_plan(checkpoint.plan,
                                  checkpoint.snapshot.plan_id);
      if (plan) {
        auto persisted = plans.save(**plan);
        if (!persisted) {
          return fail(persisted.error());
        }
        if (persisted->durability_deferred) {
          log::warn(
              "Restored Plan {} is visible but directory durability is deferred",
              (*plan)->plan_id);
        }
      }
    }
    if (!plan) {
      return fail(plan.error());
    }
    auto restored = runtime.restore(*plan, std::move(checkpoint));
    if (!restored) {
      return fail(restored.error());
    }
  }
  return ok();
}

} // namespace

Application::Application() : Application(config::SystemConfig{}) {}

Application::Application(config::SystemConfig config)
    : config_(std::move(config)) {
  auto rebuilt = rebuild_components();
  if (!rebuilt) {
    initialization_error_ = rebuilt.error();
  }
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  auto loaded = config::SystemConfigLoader::load_from_file(path);
  if (!loaded) {
    return fail(loaded.error());
  }
  return apply_config(std::move(*loaded));
}

auto Application::apply_config(config::SystemConfig config) -> Result<void> {
  if (is_running()) {
    return fail(Error::InvalidState);
  }
  auto previous = config_;
  config_ = std::move(config);
  auto rebuilt = rebuild_components();
  if (!rebuilt) {
    const auto configuration_error = rebuilt.error();
    config_ = std::move(previous);
    auto restored = rebuild_components();
    if (!restored) {
      initialization_error_ = restored.error();
      return fail(restored.error());
    }
    initialization_error_.reset();
    return fail(configuration_error);
  }
  initialization_error_.reset();
  return ok();
}

auto Application::config() const noexcept -> const config::SystemConfig & {
  return config_;
}

auto Application::rebuild_components() -> Result<void> {
  shutdown_components();

  const auto shard_count =
      config_.runtime.shards > 0
          ? static_cast<unsigned>(config_.runtime.shards)
          : std::max(1U, std::thread::hardware_concurrency());
  runtime_ = std::make_unique<Runtime>(
      shard_count, config_.runtime.pin_shards_to_cores,
      static_cast<unsigned>(config_.runtime.cpu_affinity_offset));
  executor_registry_ = std::make_unique<workflow::ExecutorRegistry>();
  auto command_executor =
      executors::command::create_task_executor(
          *runtime_, config_.executors.command);
  if (!command_executor) {
    return fail(command_executor.error());
  }
  auto command_registered =
      executor_registry_->register_executor(std::move(*command_executor));
  if (!command_registered) {
    return fail(command_registered.error());
  }
  if (config_.executors.http.enabled) {
    auto http_executor = executors::http::create_task_executor(
        *runtime_, config_.executors.http.egress);
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
    std::unique_ptr<workflow::StorageDirectoryLock> storage_lock;
    if (config_.storage.enabled) {
      auto acquired = workflow::StorageDirectoryLock::acquire(
          std::filesystem::path{config_.storage.directory});
      if (!acquired) {
        return fail(acquired.error());
      }
      storage_lock = std::move(*acquired);
    }
    auto stores = make_workflow_stores(config_);
    if (!stores) {
      return fail(stores.error());
    }

    workflow_control_plane_ = std::make_unique<workflow::WorkflowControlPlane>(
        *executor_registry_,
        workflow::PlanValidator{config_.admission}, stores->plans);
    workflow_runtime_ = std::make_unique<workflow::WorkflowRuntime>(
        *runtime_, *executor_registry_, std::move(stores->artifacts),
        std::move(stores->evidence), stores->checkpoints,
        config_.storage.max_completed_runs);

    auto restored = restore_workflow_state(
        *workflow_control_plane_, *workflow_runtime_, *stores->plans,
        *stores->checkpoints);
    if (!restored) {
      return restored;
    }
    storage_lock_ = std::move(storage_lock);
  }
  if (config_.api.enabled) {
    api_ = std::make_unique<ApiServer>(*this);
  }
  return ok();
}

auto Application::init() -> Result<void> {
  if (initialization_error_ || !runtime_ || !executor_registry_) {
    auto rebuilt = rebuild_components();
    if (!rebuilt) {
      initialization_error_ = rebuilt.error();
      return fail(rebuilt.error());
    }
    initialization_error_.reset();
  }
  return ok();
}

auto Application::start() -> Result<void> {
  if (running_.load(std::memory_order_acquire)) {
    return ok();
  }
  auto initialized = init();
  if (!initialized) {
    return fail(initialized.error());
  }

  auto runtime_started = runtime_->start();
  if (!runtime_started) {
    shutdown_components();
    return fail(runtime_started.error());
  }

  if (workflow_runtime_) {
    auto activated = workflow_runtime_->activate_restored();
    if (!activated) {
      shutdown_components();
      return fail(activated.error());
    }
  }

  if (config_.api.enabled && api_) {
    auto api_started = api_->start();
    if (!api_started) {
      shutdown_components();
      return fail(api_started.error());
    }
  }
  running_.store(true, std::memory_order_release);
  return ok();
}

auto Application::shutdown_components() noexcept -> void {
  running_.store(false, std::memory_order_release);
  if (api_) {
    api_->stop();
    api_.reset();
  }
  const bool runtime_running = runtime_ && runtime_->is_running();
  if (workflow_runtime_ && runtime_running) {
    auto quiesced = workflow_runtime_->quiesce(std::chrono::seconds(10));
    if (!quiesced) {
      log::error("Workflow runtime shutdown did not quiesce: {}",
                 quiesced.error().message());
    }
  }
  if (executor_registry_ && runtime_running) {
    auto quiesced = executor_registry_->quiesce(std::chrono::seconds(10));
    if (!quiesced) {
      log::error("Task executors did not quiesce: {}",
                 quiesced.error().message());
    }
  }
  if (runtime_) {
    runtime_->stop();
  }
  workflow_runtime_.reset();
  workflow_control_plane_.reset();
  executor_registry_.reset();
  runtime_.reset();
  storage_lock_.reset();
}

auto Application::stop() noexcept -> void {
  shutdown_components();
}

auto Application::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Application::readiness() const noexcept -> ApplicationReadiness {
  const auto running = is_running();
  const auto runtime_ready =
      running && runtime_ != nullptr && runtime_->is_running();
  const auto workflow_ready =
      !config_.workflow.enabled ||
      (workflow_runtime_ != nullptr && workflow_runtime_->accepting_runs());
  const auto storage_ready =
      !config_.storage.enabled || storage_lock_ != nullptr;
  const auto api_ready =
      !config_.api.enabled || (api_ != nullptr && api_->is_running());
  return ApplicationReadiness{
      .ready = runtime_ready && workflow_ready && storage_ready && api_ready,
      .runtime = runtime_ready,
      .workflow = workflow_ready,
      .storage = storage_ready,
      .api = api_ready,
  };
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
