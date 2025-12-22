#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "taskmaster/config.hpp"
#include "taskmaster/coroutine.hpp"
#include "taskmaster/dag.hpp"
#include "taskmaster/dag_manager.hpp"
#include "taskmaster/dag_run.hpp"
#include "taskmaster/engine.hpp"
#include "taskmaster/error.hpp"
#include "taskmaster/executor.hpp"
#include "taskmaster/persistence.hpp"

namespace taskmaster {

class ApiServer;

class Application {
public:
  Application();
  explicit Application(std::string_view db_path);
  ~Application();

  Application(const Application &) = delete;
  auto operator=(const Application &) -> Application & = delete;

  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto load_config_string(std::string_view json_str)
      -> Result<void>;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load();
  }

  auto trigger_dag(std::string_view dag_id) -> void;
  auto trigger_dag_by_id(std::string_view dag_id) -> void;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;
  auto list_tasks() const -> void;
  auto show_status() const -> void;

  [[nodiscard]] auto config() const noexcept -> const Config & {
    return config_;
  }

  [[nodiscard]] auto config() noexcept -> Config & { return config_; }

  [[nodiscard]] auto dag_manager() -> DAGManager & { return dag_manager_; }
  [[nodiscard]] auto dag_manager() const -> const DAGManager & {
    return dag_manager_;
  }

  // Register a task with cron schedule to the engine
  auto register_task_with_engine(std::string_view dag_id,
                                 const TaskConfig &task) -> void;
  auto unregister_task_from_engine(std::string_view dag_id,
                                   std::string_view task_id) -> void;

  [[nodiscard]] auto engine() -> Engine & { return engine_; }

  [[nodiscard]] auto recover_from_crash() -> Result<void>;

  [[nodiscard]] auto api_server() -> ApiServer * { return api_server_.get(); }

private:
  auto setup_dag() -> Result<void>;
  auto setup_engine() -> Result<void>;
  auto on_task_ready(const TaskInstance &inst) -> void;
  auto on_dag_run_task_completed(std::string_view dag_run_id,
                                 NodeIndex task_idx) -> void;
  auto execute_ready_tasks(const std::string &dag_run_id) -> void;

  auto execute_task_coro(std::string dag_run_id, NodeIndex task_idx,
                         std::string task_name, std::string instance_id,
                         ExecutorConfig config) -> spawn_task;

  auto execute_single_task_coro(std::string dag_id, std::string task_id,
                                std::string instance_id, ExecutorConfig config)
      -> spawn_task;

  std::atomic<bool> running_{false};
  Config config_;
  Engine engine_;
  std::unique_ptr<IExecutor> executor_;
  DAGManager dag_manager_;
  std::unique_ptr<Persistence> persistence_;
  std::unique_ptr<ApiServer> api_server_;

  DAG dag_;
  std::vector<TaskConfig> task_configs_;
  std::vector<ExecutorConfig> executor_configs_;
  std::unordered_map<std::string, std::unique_ptr<DAGRun>> dag_runs_;
  std::unordered_map<std::string, std::string> instance_to_dag_run_;
  std::unordered_map<std::string, NodeIndex> instance_to_task_;

  mutable std::mutex dag_runs_mu_;
  mutable std::mutex instance_map_mu_;
  std::atomic<int> active_coroutines_{0};

  // Completion notification for wait_for_completion
  mutable std::mutex completion_mu_;
  std::condition_variable completion_cv_;

  // Helper for restoring DAG runs (used by recover_from_crash and trigger_dag)
  auto restore_dag_run(std::string dag_run_id, std::unique_ptr<DAGRun> run)
      -> void;
};

} // namespace taskmaster
