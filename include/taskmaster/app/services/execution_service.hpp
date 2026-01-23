#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <condition_variable>
#include <flat_map>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace taskmaster {

class Runtime;
class IExecutor;
struct TaskConfig;
struct XComPushConfig;

struct ExecutionCallbacks {
  std::move_only_function<void(const DAGRunId& dag_run_id, const TaskId& task,
                               std::string_view status)>
      on_task_status;
  std::move_only_function<void(const DAGRunId& dag_run_id,
                               std::string_view status)>
      on_run_status;
  std::move_only_function<void(const DAGRunId& dag_run_id, const TaskId& task,
                               std::string_view stream, std::string msg)>
      on_log;
  std::move_only_function<void(const DAGRun& run)> on_persist_run;
  std::move_only_function<void(const DAGRunId& dag_run_id,
                               const TaskInstanceInfo& info)>
      on_persist_task;
  std::move_only_function<void(const DAGRunId& dag_run_id, const TaskId& task,
                               int attempt, std::string_view level,
                               std::string_view msg)>
      on_persist_log;
  std::move_only_function<void(const DAGRunId& dag_run_id, const TaskId& task,
                               std::string_view key, const nlohmann::json& value)>
      on_persist_xcom;
  std::move_only_function<Result<nlohmann::json>(const DAGRunId& dag_run_id,
                                                  const TaskId& task,
                                                  std::string_view key)>
      get_xcom;
  std::move_only_function<int(const DAGRunId& dag_run_id, NodeIndex idx)>
      get_max_retries;
  std::move_only_function<std::chrono::seconds(const DAGRunId& dag_run_id, NodeIndex idx)>
      get_retry_interval;
  std::move_only_function<bool(const DAGRunId& dag_run_id, NodeIndex idx)>
      get_depends_on_past;
  std::move_only_function<Result<std::optional<TaskState>>(
      const DAGRunId& dag_run_id, NodeIndex idx,
      std::chrono::system_clock::time_point execution_date,
      std::string_view current_dag_run_id)>
      check_previous_task_state;
};

class ExecutionService {
public:
  ExecutionService(Runtime& runtime, IExecutor& executor);
  ~ExecutionService();

  ExecutionService(const ExecutionService&) = delete;
  auto operator=(const ExecutionService&) -> ExecutionService& = delete;

  auto set_callbacks(ExecutionCallbacks callbacks) -> void;

  auto start_run(DAGRunId dag_run_id, std::unique_ptr<DAGRun> run,
                 std::vector<ExecutorConfig> configs,
                 std::vector<TaskConfig> task_configs = {}) -> void;

  [[nodiscard]] auto get_run(const DAGRunId& dag_run_id) -> DAGRun*;

  [[nodiscard]] auto has_active_runs() const -> bool;

  auto wait_for_completion(int timeout_ms) -> void;

  [[nodiscard]] auto coro_count() const -> int;

  auto set_max_concurrency(int max_concurrency) -> void;

private:
  struct TaskJob {
    NodeIndex idx;
    TaskId task_id;
    InstanceId inst_id;
    ExecutorConfig cfg;
    std::vector<XComPushConfig> xcom_push;
    std::vector<XComPullConfig> xcom_pull;
    int attempt{0};
  };

  auto dispatch(const DAGRunId& dag_run_id) -> void;
  auto dispatch_pending() -> void;
  auto dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task;
  auto dispatch_after_delay(DAGRunId dag_run_id, std::chrono::seconds delay) -> spawn_task;
  auto run_task(DAGRunId dag_run_id, TaskJob job) -> spawn_task;
  auto on_task_success(const DAGRunId& dag_run_id, NodeIndex idx) -> void;
  auto on_task_failure(const DAGRunId& dag_run_id, NodeIndex idx,
                       const std::string& error, int exit_code) -> bool;
  auto on_task_skipped(const DAGRunId& dag_run_id, NodeIndex idx) -> void;
  auto on_task_fail_immediately(const DAGRunId& dag_run_id, NodeIndex idx,
                                const std::string& error, int exit_code) -> void;
  auto on_run_complete(DAGRun& run, const DAGRunId& dag_run_id) -> void;

  Runtime& runtime_;
  IExecutor& executor_;
  ExecutionCallbacks callbacks_;

  std::flat_map<DAGRunId, std::unique_ptr<DAGRun>> runs_;
  std::flat_map<DAGRunId, std::vector<ExecutorConfig>> run_cfgs_;
  std::flat_map<DAGRunId, std::vector<TaskConfig>> task_cfgs_;

  mutable std::mutex mu_;
  std::atomic<int> coro_count_{0};
  std::condition_variable done_cv_;
  std::atomic<int> running_tasks_{0};
  int max_concurrency_{100};
};

}  // namespace taskmaster
