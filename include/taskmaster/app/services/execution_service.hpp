#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/executor/executor.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace taskmaster {

class Runtime;
class IExecutor;
struct TaskConfig;

// Callbacks for execution events
struct ExecutionCallbacks {
  std::function<void(std::string_view run_id, std::string_view task,
                     std::string_view status)>
      on_task_status;
  std::function<void(std::string_view run_id, std::string_view status)>
      on_run_status;
  std::function<void(std::string_view run_id, std::string_view task,
                     std::string_view stream, std::string msg)>
      on_log;
  std::function<void(const DAGRun& run)> on_persist_run;
  std::function<void(std::string_view run_id, const TaskInstanceInfo& info)>
      on_persist_task;
  std::function<void(std::string_view run_id, std::string_view task,
                     int attempt, std::string_view level, std::string_view msg)>
      on_persist_log;
  std::function<int(std::string_view run_id, NodeIndex idx)> get_max_retries;
};

// Manages DAG run execution
class ExecutionService {
public:
  ExecutionService(Runtime& runtime, IExecutor& executor);
  ~ExecutionService();

  ExecutionService(const ExecutionService&) = delete;
  auto operator=(const ExecutionService&) -> ExecutionService& = delete;

  auto set_callbacks(ExecutionCallbacks callbacks) -> void;

  // Start a new DAG run
  auto start_run(std::string run_id, std::unique_ptr<DAGRun> run,
                 std::vector<ExecutorConfig> configs) -> void;

  // Get active run
  [[nodiscard]] auto get_run(std::string_view run_id) -> DAGRun*;

  // Check if any runs are active
  [[nodiscard]] auto has_active_runs() const -> bool;

  // Wait for all runs to complete
  auto wait_for_completion(int timeout_ms) -> void;

  // Get coroutine count (for shutdown)
  [[nodiscard]] auto coro_count() const -> int;

private:
  struct TaskJob {
    NodeIndex idx;
    std::string name;
    std::string inst_id;
    ExecutorConfig cfg;
    int attempt{0};
  };

  auto dispatch(const std::string& run_id) -> void;
  auto run_task(std::string run_id, TaskJob job) -> spawn_task;
  auto on_task_success(const std::string& run_id, NodeIndex idx) -> void;
  auto on_task_failure(const std::string& run_id, NodeIndex idx,
                       const std::string& error) -> bool;
  auto on_run_complete(DAGRun& run, const std::string& run_id) -> void;

  Runtime& runtime_;
  IExecutor& executor_;
  ExecutionCallbacks callbacks_;

  std::unordered_map<std::string, std::unique_ptr<DAGRun>> runs_;
  std::unordered_map<std::string, std::vector<ExecutorConfig>> run_cfgs_;

  mutable std::mutex mu_;
  std::atomic<int> coro_count_{0};
  std::condition_variable done_cv_;
};

}  // namespace taskmaster
