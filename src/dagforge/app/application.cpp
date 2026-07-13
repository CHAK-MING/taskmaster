#include "dagforge/app/application.hpp"

#include "dagforge/app/api/api_server.hpp"
#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <algorithm>
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
  executor_.reset();
  runtime_.reset();

  const auto shard_count =
      config_.runtime.shards > 0
          ? static_cast<unsigned>(config_.runtime.shards)
          : std::max(1U, std::thread::hardware_concurrency());
  runtime_ = std::make_unique<Runtime>(
      shard_count, config_.runtime.pin_shards_to_cores,
      static_cast<unsigned>(config_.runtime.cpu_affinity_offset),
      ComputePoolConfig{
          .thread_count = static_cast<std::size_t>(config_.compute.threads),
          .queue_capacity =
              static_cast<std::size_t>(config_.compute.queue_capacity),
          .pin_threads_to_cores = config_.compute.pin_threads_to_cores,
          .cpu_affinity_offset =
              static_cast<unsigned>(config_.compute.cpu_affinity_offset),
      });
  executor_ = create_command_executor(*runtime_, config_.sandbox);
  if (!executor_) {
    return fail(Error::InvalidState);
  }

  if (config_.workflow.enabled) {
    workflow_control_plane_ =
        std::make_unique<workflow::WorkflowControlPlane>();
    workflow_runtime_ = std::make_unique<workflow::WorkflowRuntime>(
        *runtime_, *executor_, std::make_shared<workflow::InMemoryArtifactStore>(),
        std::make_shared<workflow::EvidenceLedger>(),
        std::make_shared<workflow::CheckpointStore>());
  }
  if (config_.api.enabled) {
    api_ = std::make_unique<ApiServer>(*this);
  }
  return ok();
}

auto Application::init() -> Result<void> {
  const auto api_configuration_changed =
      config_.api.enabled != static_cast<bool>(api_);
  if (!runtime_ || !executor_ || api_configuration_changed) {
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
  if (!runtime_ || !executor_ || api_configuration_changed) {
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
  workflow_runtime_.reset();
  workflow_control_plane_.reset();
  if (runtime_) {
    runtime_->stop();
  }
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
