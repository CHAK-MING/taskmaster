#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/storage/config.hpp"

#include <atomic>
#include <memory>
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
  ~Application();

  Application(const Application&) = delete;
  auto operator=(const Application&) -> Application& = delete;

  // Configuration
  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto load_config_string(std::string_view json) -> Result<void>;
  [[nodiscard]] auto config() const noexcept -> const Config&;
  [[nodiscard]] auto config() noexcept -> Config&;

  // Lifecycle
  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  // DAG operations
  auto trigger_dag(std::string_view dag_id) -> void;
  auto trigger_dag_by_id(std::string_view dag_id,
                         TriggerType trigger = TriggerType::Manual) -> void;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;

  // Task operations
  auto register_task_with_engine(std::string_view dag_id,
                                 const TaskConfig& task) -> void;
  auto unregister_task_from_engine(std::string_view dag_id,
                                   std::string_view task_id) -> void;
  [[nodiscard]] auto set_task_enabled(std::string_view dag_id,
                                      std::string_view task_id, bool enabled)
      -> Result<void>;
  [[nodiscard]] auto trigger_task(std::string_view dag_id,
                                  std::string_view task_id) -> Result<void>;

  // Cron schedule management
  auto register_dag_cron(std::string_view dag_id, std::string_view cron_expr)
      -> bool;
  auto unregister_dag_cron(std::string_view dag_id) -> void;
  auto update_dag_cron(std::string_view dag_id, std::string_view cron_expr,
                       bool is_active) -> void;

  // Recovery
  [[nodiscard]] auto recover_from_crash() -> Result<void>;

  // Service access
  [[nodiscard]] auto dag_manager() -> DAGManager&;
  [[nodiscard]] auto dag_manager() const -> const DAGManager&;
  [[nodiscard]] auto engine() -> Engine&;
  [[nodiscard]] auto persistence() -> Persistence*;
  [[nodiscard]] auto api_server() -> ApiServer*;
  [[nodiscard]] auto get_active_dag_run(std::string_view run_id) -> DAGRun*;

  // Debug
  auto list_tasks() const -> void;
  auto show_status() const -> void;

private:
  auto setup_callbacks() -> void;
  auto init_from_config() -> Result<void>;
  auto on_engine_ready(const TaskInstance& inst) -> void;
  auto run_cron_task(std::string dag_id, std::string task_id,
                     std::string inst_id, ExecutorConfig cfg) -> void;
  [[nodiscard]] auto get_max_retries(std::string_view run_id, NodeIndex idx)
      -> int;

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

  // DAG management
  DAGManager dag_manager_{nullptr};
  DAG config_dag_;
  std::vector<TaskConfig> config_tasks_;
  std::vector<ExecutorConfig> config_cfgs_;

  // API server
  std::unique_ptr<ApiServer> api_;
};

}  // namespace taskmaster
