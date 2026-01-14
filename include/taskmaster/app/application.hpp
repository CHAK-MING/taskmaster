#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/config/config.hpp"
#include "taskmaster/config/dag_definition.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace taskmaster {

class ApiServer;
class DAGRun;
class Engine;
class EventService;
class ExecutionService;
class PersistenceService;
class SchedulerService;
class Persistence;
struct TaskInstance;

// Application facade - coordinates all services
class Application {
public:
  Application();
  explicit Application(std::string_view db_path);
  explicit Application(Config config);
  ~Application();

  Application(const Application&) = delete;
  auto operator=(const Application&) -> Application& = delete;

  // Configuration
  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto config() const noexcept -> const Config&;
  [[nodiscard]] auto config() noexcept -> Config&;

  // Lifecycle
  [[nodiscard]] auto init() -> Result<void>;
  [[nodiscard]] auto init_db_only() -> Result<void>;
  [[nodiscard]] auto start() -> Result<void>;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  // DAG loading
  [[nodiscard]] auto load_dags_from_directory(std::string_view dags_dir) -> Result<bool>;

  // DAG operations
  [[nodiscard]] auto trigger_dag_by_id(DAGId dag_id,
                         TriggerType trigger = TriggerType::Manual) -> std::optional<DAGRunId>;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;
  [[nodiscard]] auto get_run_state(DAGRunId dag_run_id) const -> std::optional<DAGRunState>;

  // Cron schedule management
  auto register_dag_cron(DAGId dag_id, std::string_view cron_expr)
      -> bool;
  auto unregister_dag_cron(DAGId dag_id) -> void;
  auto update_dag_cron(DAGId dag_id, std::string_view cron_expr,
                       bool is_active) -> void;

  // Recovery
  [[nodiscard]] auto recover_from_crash() -> Result<void>;

  // Service access
  [[nodiscard]] auto dag_manager() -> DAGManager&;
  [[nodiscard]] auto dag_manager() const -> const DAGManager&;
  [[nodiscard]] auto engine() -> Engine&;
  [[nodiscard]] auto persistence() -> Persistence*;
  [[nodiscard]] auto api_server() -> ApiServer*;
  [[nodiscard]] auto get_active_dag_run(DAGRunId dag_run_id) -> DAGRun*;
  [[nodiscard]] auto runtime() -> Runtime&;

  // Debug
  auto list_tasks() const -> void;
  auto show_status() const -> void;

private:
  auto setup_callbacks() -> void;
  auto setup_config_watcher() -> void;
  auto config_watcher_loop(std::stop_token stop) -> void;
  auto handle_file_change(const std::string& filename) -> void;
  auto get_max_retries(DAGRunId dag_run_id, NodeIndex idx) -> int;
  auto get_retry_interval(DAGRunId dag_run_id, NodeIndex idx) -> std::chrono::seconds;

  [[nodiscard]] auto validate_dag_info(const DAGInfo& info) -> Result<void>;
  [[nodiscard]] auto create_dag_atomically(DAGId dag_id,
                                           const DAGInfo& info) -> Result<void>;
  [[nodiscard]] auto reload_single_dag(const DAGDefinition& def) -> Result<void>;

  std::atomic<bool> running_{false};
  Config config_;

  // Core runtime
  Runtime runtime_{0};
  std::unique_ptr<IExecutor> executor_;

  // Services
  std::unique_ptr<PersistenceService> persistence_;
  std::unique_ptr<EventService> events_;
  std::unique_ptr<SchedulerService> scheduler_;
  std::unique_ptr<ExecutionService> execution_;

  DAGManager dag_manager_{nullptr};

  std::unique_ptr<ApiServer> api_;

  // Config file watching
  int inotify_fd_ = -1;
  int watch_descriptor_ = -1;
  std::jthread watcher_thread_;
  std::atomic<bool> watching_{false};
};

}  // namespace taskmaster
