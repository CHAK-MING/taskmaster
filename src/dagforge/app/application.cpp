#include "dagforge/app/application.hpp"
#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/lifecycle_manager.hpp"
#include "dagforge/app/services/dag_catalog_service.hpp"
#include "dagforge/app/services/dag_orchestrator.hpp"
#include "dagforge/app/services/execution_event_bridge.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/config/config_watcher.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/util/log.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>

#include <csignal>

namespace dagforge {
namespace {

[[nodiscard]] auto dag_run_terminal_state_index(DAGRunState state)
    -> std::optional<std::size_t> {
  switch (state) {
  case DAGRunState::Success:
    return 0;
  case DAGRunState::Failed:
    return 1;
  case DAGRunState::Skipped:
    return 2;
  case DAGRunState::Cancelled:
    return 3;
  case DAGRunState::Queued:
  case DAGRunState::Running:
    return std::nullopt;
  }
  return std::nullopt;
}

} // namespace

Application::Application() {
  std::signal(SIGPIPE, SIG_IGN);
  rebuild_services_from_config();
}

Application::Application(SystemConfig config) : config_(std::move(config)) {
  std::signal(SIGPIPE, SIG_IGN);
  rebuild_services_from_config();
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  if (is_running()) {
    return fail(Error::InvalidState);
  }
  return SystemConfigLoader::load_from_file(path).transform([this](SystemConfig &&cfg) {
    config_ = std::move(cfg);
    rebuild_services_from_config();
    return;
  });
}

auto Application::config() const noexcept -> const SystemConfig & {
  return config_;
}

auto Application::config() noexcept -> SystemConfig & { return config_; }

auto Application::rebuild_services_from_config() -> void {
  dag_catalog_.reset();
  dag_orchestrator_.reset();
  lifecycle_manager_.reset();
  execution_event_bridge_.reset();
  config_watcher_.reset();
  api_.reset();
  execution_.reset();
  scheduler_.reset();
  persistence_.reset();
  executor_.reset();
  runtime_.reset();

  runtime_.emplace(
      config_.scheduler.shards, config_.scheduler.pin_shards_to_cores,
      static_cast<unsigned>(config_.scheduler.cpu_affinity_offset));
  executor_ = create_composite_executor(*runtime_);
  persistence_ =
      std::make_unique<PersistenceService>(*runtime_, config_.database);
  scheduler_ = std::make_unique<SchedulerService>(
      *runtime_, static_cast<unsigned>(config_.scheduler.scheduler_shards));
  execution_ = std::make_unique<ExecutionService>(*runtime_, *executor_);
  dag_catalog_ = std::make_unique<DagCatalogService>(
      dag_manager_, persistence_.get(), scheduler_.get());
  dag_orchestrator_ = std::make_unique<DAGOrchestrator>(
      DAGOrchestrator::Dependencies{.dag_manager = dag_manager_,
                                    .persistence = *persistence_,
                                    .execution = *execution_,
                                    .scheduler = *scheduler_,
                                    .runtime = *runtime_,
                                    .owner_shard_count =
                                        std::max(1U, runtime_->shard_count())});

  dag_manager_.set_persistence_service(persistence_.get());
  dag_manager_.set_runtime(runtime_ ? &*runtime_ : nullptr);
  execution_->set_max_concurrency(config_.scheduler.max_concurrency);
  execution_->set_local_task_lease_timeout(
      std::chrono::seconds(config_.scheduler.zombie_heartbeat_timeout_sec));
  lifecycle_manager_ = std::make_unique<LifecycleManager>(
      LifecycleManager::Dependencies{
          .running = running_,
          .config = config_,
          .runtime = *runtime_,
          .persistence = *persistence_,
          .scheduler = *scheduler_,
          .execution = *execution_,
          .dag_catalog = *dag_catalog_,
          .dag_manager = dag_manager_,
          .ensure_api_initialized = [this]() -> Result<void> {
            if (!api_) {
              api_ = std::make_unique<ApiServer>(*this);
            }
            return ok();
          },
          .start_api = [this]() -> Result<void> {
            if (!api_) {
              api_ = std::make_unique<ApiServer>(*this);
            }
            return api_->start();
          },
          .stop_api = [this]() {
            if (api_) {
              api_->stop();
              api_.reset();
            }
          },
          .start_config_watcher = [this]() { setup_config_watcher(); },
          .stop_config_watcher = [this](std::chrono::steady_clock::time_point deadline)
              -> Result<void> {
            if (!config_watcher_) {
              return ok();
            }
            auto result = config_watcher_->stop(deadline);
            config_watcher_.reset();
            return result;
          },
          .stop_scheduler =
              [this](std::chrono::steady_clock::time_point deadline)
                  -> Result<void> {
            if (!scheduler_) {
              return ok();
            }
            return scheduler_->stop(deadline);
          },
          .wait_for_execution_quiesced =
              [this](std::chrono::steady_clock::time_point deadline)
                  -> Result<void> {
            if (!execution_ || !runtime_) {
              return ok();
            }
            const auto now = std::chrono::steady_clock::now();
            const auto timeout_ms = deadline <= now
                                        ? 0
                                        : static_cast<int>(std::chrono::duration_cast<
                                                  std::chrono::milliseconds>(
                                                  deadline - now)
                                                  .count());
            return sync_wait_on_runtime(*runtime_,
                                        execution_->wait_for_completion_async(
                                            timeout_ms));
          },
          .close_persistence =
              [this](std::chrono::steady_clock::time_point deadline)
                  -> Result<void> {
            if (!persistence_ || !runtime_ || !runtime_->is_running()) {
              return ok();
            }
            return persistence_->sync_wait(persistence_->close(deadline));
          },
      });
  rebuild_execution_event_bridge();
}

auto Application::ensure_services_initialized() -> void {
  if (!runtime_ || !executor_ || !persistence_ || !scheduler_ || !execution_ ||
      !dag_catalog_ || !lifecycle_manager_) {
    rebuild_services_from_config();
  }
}

auto Application::init() -> Result<void> {
  ensure_services_initialized();
  return lifecycle_manager_->init();
}

auto Application::init_db_only() -> Result<void> {
  ensure_services_initialized();
  return lifecycle_manager_->init_db_only();
}

auto Application::load_dags_from_directory(std::string_view dags_dir)
    -> Result<bool> {
  ensure_services_initialized();
  if (!dag_catalog_) {
    return fail(Error::InvalidState);
  }
  return dag_catalog_->load_directory(dags_dir);
}

auto Application::get_run_state(const DAGRunId &dag_run_id) const
    -> Result<DAGRunState> {
  return sync_wait_on_runtime(*runtime_, get_run_state_async(dag_run_id));
}

auto Application::get_run_state_async(const DAGRunId &run_id) const
    -> task<Result<DAGRunState>> {
  if (!dag_orchestrator_) {
    co_return fail(Error::InvalidState);
  }
  auto state_res = co_await dag_orchestrator_->get_run_state_async(run_id);
  if (!state_res) {
    co_return fail(state_res.error());
  }
  co_return ok(*state_res);
}

auto Application::start() -> Result<void> {
  ensure_services_initialized();
  return lifecycle_manager_->start();
}

auto Application::stop() noexcept -> void {
  if (!lifecycle_manager_) {
    return;
  }

  lifecycle_manager_->stop();

  // Destroy services that may still own executors or async work while the
  // runtime is still valid.
  api_.reset();
  execution_event_bridge_.reset();
  dag_catalog_.reset();
  dag_orchestrator_.reset();
  lifecycle_manager_.reset();
  scheduler_.reset();
  persistence_.reset();
  dag_manager_.set_persistence_service(nullptr);

  execution_.reset();
  executor_.reset();
  dag_manager_.set_runtime(nullptr);
}

auto Application::is_running() const noexcept -> bool {
  return running_.load();
}

auto Application::rebuild_execution_event_bridge() -> void {
  execution_event_bridge_ =
      std::make_unique<ExecutionEventBridge>(ExecutionEventBridge::Dependencies{
          .runtime = runtime_ ? &*runtime_ : nullptr,
          .execution = execution_.get(),
          .scheduler = scheduler_.get(),
          .persistence = persistence_.get(),
          .api_server = [this]() -> ApiServer * { return api_.get(); },
          .resolve_dag_id = [this](const DAGRunId &dag_run_id)
              -> std::optional<DAGId> {
            return resolve_dag_id_for_event(dag_run_id);
          },
          .on_run_status = [this](const DAGRunId &dag_run_id, DAGRunState status) {
            if (dag_orchestrator_) {
              dag_orchestrator_->on_run_finished(dag_run_id, status);
            }
          },
          .on_run_completed =
              [this](const DAGRunId &dag_run_id, const DAGRun &run) {
                record_dag_run_metrics(dag_run_id, run);
              },
          .get_max_retries =
              [this](const DAGRunId &dag_run_id, NodeIndex idx) {
                return dag_orchestrator_ ? dag_orchestrator_->get_max_retries(dag_run_id, idx)
                                         : 3;
              },
          .get_retry_interval =
              [this](const DAGRunId &dag_run_id, NodeIndex idx) {
                return dag_orchestrator_
                           ? dag_orchestrator_->get_retry_interval(dag_run_id, idx)
                           : std::chrono::seconds(60);
              },
          .on_scheduler_trigger =
              [this](const DAGId &dag_id,
                     std::chrono::system_clock::time_point execution_date) {
                if (!dag_orchestrator_ || !runtime_) {
                  return;
                }
                runtime_->spawn(dag_orchestrator_->trigger_scheduled(
                    dag_id.clone(), execution_date));
              },
          .dropped_persistence_events = &dropped_persistence_events_,
          .mysql_batch_write_ops = &mysql_batch_write_ops_,
      });
  execution_event_bridge_->wire();
  if (config_.scheduler.zombie_heartbeat_timeout_sec > 0) {
    scheduler_->set_zombie_reaper_config(0, 0);
  } else {
    scheduler_->set_zombie_reaper_config(
        config_.scheduler.zombie_reaper_interval_sec,
        config_.scheduler.zombie_heartbeat_timeout_sec);
  }
}

auto Application::trigger_scheduled(
    DAGId dag_id, std::chrono::system_clock::time_point execution_date)
    -> task<void> {
  if (!dag_orchestrator_) {
    co_return;
  }
  co_await dag_orchestrator_->trigger_scheduled(std::move(dag_id), execution_date);
}

auto Application::trigger_run(
    DAGId dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date)
    -> task<Result<DAGRunId>> {
  if (!running_.load(std::memory_order_acquire) || !dag_orchestrator_) {
    co_return fail(Error::InvalidState);
  }
  auto run_res =
      co_await dag_orchestrator_->trigger_run(std::move(dag_id), trigger,
                                              execution_date);
  if (!run_res) {
    co_return fail(run_res.error());
  }
  co_return ok(std::move(*run_res));
}

auto Application::trigger_run_blocking(
    const DAGId &dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date)
    -> Result<DAGRunId> {
  if (!running_.load(std::memory_order_acquire) || !dag_orchestrator_) {
    return fail(Error::InvalidState);
  }

  return dag_orchestrator_->trigger_run_blocking(dag_id, trigger, execution_date);
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  auto wait_res = sync_wait_on_runtime(*runtime_, wait_for_completion_async(timeout_ms));
  if (!wait_res && dag_orchestrator_ && dag_orchestrator_->has_active_runs()) {
    log::warn("wait_for_completion timed out: {} run(s) still active",
              active_coroutines());
  }
}

auto Application::wait_for_completion_async(int timeout_ms)
    -> task<Result<void>> {
  if (!dag_orchestrator_) {
    co_return ok();
  }
  auto wait_res = co_await dag_orchestrator_->wait_for_completion_async(timeout_ms);
  if (!wait_res) {
    co_return fail(wait_res.error());
  }
  co_return ok();
}

auto Application::has_active_runs() const -> bool {
  return dag_orchestrator_ && dag_orchestrator_->has_active_runs();
}

auto Application::active_coroutines() const -> int {
  return execution_ ? execution_->coro_count() : 0;
}

auto Application::mysql_batch_write_ops() const -> std::uint64_t {
  return mysql_batch_write_ops_.load(std::memory_order_relaxed);
}

auto Application::dropped_persistence_events() const -> std::uint64_t {
  return dropped_persistence_events_.load(std::memory_order_relaxed);
}

auto Application::event_bus_queue_length() const -> std::size_t {
  return runtime_->pending_cross_shard_queue_length();
}

auto Application::trigger_batch_queue_depth() const -> std::size_t {
  return persistence_ ? persistence_->trigger_batch_queue_depth() : 0;
}

auto Application::trigger_batch_last_size() const -> std::size_t {
  return persistence_ ? persistence_->trigger_batch_last_size() : 0;
}

auto Application::trigger_batch_last_linger_us() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_last_linger_us() : 0;
}

auto Application::trigger_batch_last_flush_ms() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_last_flush_ms() : 0;
}

auto Application::trigger_batch_requests_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_requests_total() : 0;
}

auto Application::trigger_batch_commits_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_commits_total() : 0;
}

auto Application::trigger_batch_fallback_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_fallback_total() : 0;
}

auto Application::trigger_batch_rejected_total() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_rejected_total() : 0;
}

auto Application::trigger_batch_wakeup_lag_us() const -> std::uint64_t {
  return persistence_ ? persistence_->trigger_batch_wakeup_lag_us() : 0;
}

auto Application::task_update_batch_queue_depth() const -> std::size_t {
  return persistence_ ? persistence_->task_update_batch_queue_depth() : 0;
}

auto Application::task_update_batch_last_size() const -> std::size_t {
  return persistence_ ? persistence_->task_update_batch_last_size() : 0;
}

auto Application::task_update_batch_last_linger_us() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_last_linger_us() : 0;
}

auto Application::task_update_batch_last_flush_ms() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_last_flush_ms() : 0;
}

auto Application::task_update_batch_requests_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_requests_total() : 0;
}

auto Application::task_update_batch_commits_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_commits_total() : 0;
}

auto Application::task_update_batch_fallback_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_fallback_total() : 0;
}

auto Application::task_update_batch_rejected_total() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_rejected_total() : 0;
}

auto Application::task_update_batch_wakeup_lag_us() const -> std::uint64_t {
  return persistence_ ? persistence_->task_update_batch_wakeup_lag_us() : 0;
}

auto Application::resolve_dag_id_for_event(const DAGRunId &dag_run_id) const
    -> std::optional<DAGId> {
  if (execution_) {
    if (auto dag_id = execution_->get_cached_dag_id(dag_run_id)) {
      return dag_id;
    }
  }
  if (dag_orchestrator_) {
    if (auto dag_id = dag_orchestrator_->resolve_dag_id(dag_run_id)) {
      return dag_id;
    }
  }
  return std::nullopt;
}

auto Application::dag_run_metrics() const
    -> std::vector<DagRunMetricsSnapshot> {
  return dag_run_metrics_.snapshot();
}

auto Application::shard_stall_age_ms(shard_id id) const -> std::uint64_t {
  return runtime_->stall_age_ms(id);
}

auto Application::record_dag_run_metrics(const DAGRunId &dag_run_id,
                                         const DAGRun &run) -> void {
  const auto dag_id = dag_id_from_run_id(dag_run_id);
  if (!dag_id) {
    return;
  }

  const auto state_index = dag_run_terminal_state_index(run.state());
  if (!state_index) {
    return;
  }

  std::uint64_t duration_ns = 0;
  const auto started_at = run.started_at();
  const auto finished_at = run.finished_at();
  if (finished_at > started_at) {
    duration_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished_at -
                                                             started_at)
            .count());
  }

  dag_run_metrics_.record(*dag_id, *state_index, duration_ns);
}

auto Application::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  if (!dag_orchestrator_) {
    co_return fail(Error::InvalidState);
  }
  auto pause_res = co_await dag_orchestrator_->set_dag_paused(dag_id, paused);
  if (!pause_res) {
    co_return fail(pause_res.error());
  }
  co_return ok();
}

auto Application::register_dag_cron(DAGId dag_id,
                                    std::string_view cron_expr)
    -> Result<void> {
  if (!dag_orchestrator_) {
    return fail(Error::InvalidState);
  }
  return dag_orchestrator_->register_dag_cron(std::move(dag_id), cron_expr);
}

auto Application::unregister_dag_cron(const DAGId &dag_id) -> void {
  if (dag_orchestrator_) {
    dag_orchestrator_->unregister_dag_cron(dag_id);
  }
}

auto Application::update_dag_cron(const DAGId &dag_id,
                                  std::string_view cron_expr, bool is_active)
    -> Result<void> {
  if (!dag_orchestrator_) {
    return fail(Error::InvalidState);
  }
  return dag_orchestrator_->update_dag_cron(dag_id, cron_expr, is_active);
}

auto Application::recover_from_crash() -> Result<void> {
  if (!persistence_ || !persistence_->is_open()) {
    return fail(Error::DatabaseError);
  }
  if (auto load_res = dag_manager_.load_from_database(); !load_res) {
    return fail(load_res.error());
  }
  // MySQL migration phase: we only ensure dangling in-flight runs are marked
  // failed at startup. Detailed DAGRun in-memory reconstruction can be added
  // back as a separate step.
  auto marked =
      persistence_->sync_wait(persistence_->mark_incomplete_runs_failed());
  if (!marked) {
    return fail(marked.error());
  }
  log::info("Recovery: marked {} incomplete run(s) failed", *marked);
  return ok();
}

auto Application::dag_manager() -> DAGManager & { return dag_manager_; }

auto Application::dag_manager() const -> const DAGManager & {
  return dag_manager_;
}

auto Application::execution_service() -> ExecutionService * {
  return execution_.get();
}

auto Application::execution_service() const -> const ExecutionService * {
  return execution_.get();
}

auto Application::scheduler_service() -> SchedulerService * {
  return scheduler_.get();
}

auto Application::scheduler_service() const -> const SchedulerService * {
  return scheduler_.get();
}

auto Application::persistence_service() -> PersistenceService * {
  return persistence_.get();
}

auto Application::persistence_service() const -> const PersistenceService * {
  return persistence_.get();
}

auto Application::api_server() -> ApiServer * { return api_.get(); }

auto Application::api_server() const -> const ApiServer * { return api_.get(); }

auto Application::runtime() -> Runtime & { return *runtime_; }

auto Application::runtime() const -> const Runtime & { return *runtime_; }

auto Application::setup_config_watcher() -> void {
  if (config_.dag_source.directory.empty()) {
    return;
  }

  const std::filesystem::path dag_dir = config_.dag_source.directory;
  if (!std::filesystem::exists(dag_dir)) {
    log::warn("Skip ConfigWatcher: DAG directory does not exist: {}",
              dag_dir.string());
    return;
  }

  if (config_watcher_) {
    (void)config_watcher_->stop();
    config_watcher_.reset();
  }

  config_watcher_ =
      std::make_unique<ConfigWatcher>(*runtime_, config_.dag_source.directory);
  config_watcher_->set_on_file_changed([this](
                                           const std::filesystem::path &path) {
    if (!dag_catalog_) {
      log::warn("Ignoring DAG file change for {}: DagCatalogService missing",
                path.string());
      return;
    }
    (void)dag_catalog_->handle_file_change(config_.dag_source.directory, path);
  });
  config_watcher_->set_on_file_removed([this](
                                           const std::filesystem::path &path) {
    if (!dag_catalog_) {
      log::warn("Ignoring DAG file removal for {}: DagCatalogService missing",
                path.string());
      return;
    }
    (void)dag_catalog_->handle_file_change(config_.dag_source.directory, path);
  });
  if (auto r = config_watcher_->start(); !r) {
    log::error("Failed to start ConfigWatcher for {}: {}",
               config_.dag_source.directory, r.error().message());
    config_watcher_.reset();
  }
}

} // namespace dagforge
