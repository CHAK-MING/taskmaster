#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/app/metrics_registry.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/config/dag_info_loader.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/executor/executor.hpp"
#endif

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>


namespace dagforge {

class ApiServer;
class ConfigWatcher;
class DAGRun;
class DagCatalogService;
class DAGOrchestrator;
class ExecutionEventBridge;
class ExecutionService;
class LifecycleManager;
class PersistenceService;
class SchedulerService;

// Application facade - coordinates all services
class Application {
public:
  Application();
  explicit Application(SystemConfig config);
  ~Application();

  Application(const Application &) = delete;
  auto operator=(const Application &) -> Application & = delete;

  // Configuration
  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto config() const noexcept -> const SystemConfig &;
  [[nodiscard]] auto config() noexcept -> SystemConfig &;

  // Lifecycle
  [[nodiscard]] auto init() -> Result<void>;
  [[nodiscard]] auto init_db_only() -> Result<void>;
  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  // DAG loading
  [[nodiscard]] auto load_dags_from_directory(std::string_view dags_dir)
      -> Result<bool>;

  // DAG operations
  [[nodiscard]] auto trigger_run(
      DAGId dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt) -> task<Result<DAGRunId>>;

  [[nodiscard]] auto
  trigger_scheduled(DAGId dag_id,
                    std::chrono::system_clock::time_point execution_date)
      -> spawn_task;

  [[nodiscard]] auto trigger_run_blocking(
      const DAGId &dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt) -> Result<DAGRunId>;
  [[nodiscard]] auto wait_for_completion_async(int timeout_ms = 60000)
      -> task<Result<void>>;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;
  [[nodiscard]] auto active_coroutines() const -> int;
  [[nodiscard]] auto mysql_batch_write_ops() const -> std::uint64_t;
  [[nodiscard]] auto dropped_persistence_events() const -> std::uint64_t;
  [[nodiscard]] auto event_bus_queue_length() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_queue_depth() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_size() const -> std::size_t;
  [[nodiscard]] auto trigger_batch_last_linger_us() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_last_flush_ms() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_requests_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_commits_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_fallback_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_rejected_total() const -> std::uint64_t;
  [[nodiscard]] auto trigger_batch_wakeup_lag_us() const -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_queue_depth() const -> std::size_t;
  [[nodiscard]] auto task_update_batch_last_size() const -> std::size_t;
  [[nodiscard]] auto task_update_batch_last_linger_us() const
      -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_last_flush_ms() const -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_requests_total() const
      -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_commits_total() const
      -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_fallback_total() const
      -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_rejected_total() const
      -> std::uint64_t;
  [[nodiscard]] auto task_update_batch_wakeup_lag_us() const
      -> std::uint64_t;
  using DagRunMetricsSnapshot = detail::DagRunMetricsSnapshot;
  [[nodiscard]] auto dag_run_metrics() const
      -> std::vector<DagRunMetricsSnapshot>;
  [[nodiscard]] auto shard_stall_age_ms(shard_id id) const -> std::uint64_t;
  [[nodiscard]] auto get_run_state(const DAGRunId &dag_run_id) const
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_run_state_async(const DAGRunId &run_id) const
      -> task<Result<DAGRunState>>;

  [[nodiscard]] auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>>;

  // Cron schedule management
  [[nodiscard]] auto register_dag_cron(DAGId dag_id, std::string_view cron_expr)
      -> Result<void>;
  auto unregister_dag_cron(const DAGId &dag_id) -> void;
  [[nodiscard]] auto update_dag_cron(const DAGId &dag_id,
                                     std::string_view cron_expr, bool is_active)
      -> Result<void>;

  // Recovery
  [[nodiscard]] auto recover_from_crash() -> Result<void>;
  // Service access
  [[nodiscard]] auto dag_manager() -> DAGManager &;
  [[nodiscard]] auto dag_manager() const -> const DAGManager &;
  [[nodiscard]] auto execution_service() -> ExecutionService *;
  [[nodiscard]] auto execution_service() const -> const ExecutionService *;
  [[nodiscard]] auto scheduler_service() -> SchedulerService *;
  [[nodiscard]] auto scheduler_service() const -> const SchedulerService *;
  [[nodiscard]] auto persistence_service() -> PersistenceService *;
  [[nodiscard]] auto persistence_service() const
      -> const PersistenceService *;
  [[nodiscard]] auto api_server() -> ApiServer *;
  [[nodiscard]] auto api_server() const -> const ApiServer *;
  [[nodiscard]] auto runtime() -> Runtime &;
  [[nodiscard]] auto runtime() const -> const Runtime &;

private:
  [[nodiscard]] auto resolve_dag_id_for_event(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;
  auto rebuild_execution_event_bridge() -> void;
  auto record_dag_run_metrics(const DAGRunId &dag_run_id,
                              const DAGRun &run) -> void;
  auto setup_config_watcher() -> void;
  auto rebuild_services_from_config() -> void;
  auto ensure_services_initialized() -> void;
  std::atomic<bool> running_{false};
  SystemConfig config_;

  // Core runtime
  std::optional<Runtime> runtime_;
  std::unique_ptr<IExecutor> executor_;

  // Services
  std::atomic<std::uint64_t> dropped_persistence_events_{0};
  std::atomic<std::uint64_t> mysql_batch_write_ops_{0};
  std::unique_ptr<PersistenceService> persistence_;
  std::unique_ptr<SchedulerService> scheduler_;
  std::unique_ptr<ExecutionService> execution_;
  std::unique_ptr<LifecycleManager> lifecycle_manager_;
  std::unique_ptr<ExecutionEventBridge> execution_event_bridge_;
  std::unique_ptr<DagCatalogService> dag_catalog_;
  std::unique_ptr<DAGOrchestrator> dag_orchestrator_;
  detail::DagRunMetricsRegistry dag_run_metrics_;

  DAGManager dag_manager_;

  std::unique_ptr<ApiServer> api_;

  // Config file watching
  std::unique_ptr<ConfigWatcher> config_watcher_;

};

} // namespace dagforge
