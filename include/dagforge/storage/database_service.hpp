#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/storage/orm_models.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/xcom/xcom_types.hpp"
#endif

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge {

struct TaskConfig;

struct ClaimedTaskInstance {
  DAGRunId dag_run_id{};
  int64_t run_rowid{0};
  int64_t task_rowid{0};
  int attempt{0};

  [[nodiscard]] auto task_instance_key() const noexcept -> TaskInstanceKey {
    return TaskInstanceKey{
        .run_rowid = run_rowid,
        .task_rowid = task_rowid,
        .attempt = attempt,
    };
  }
};

struct RunInsertBundle {
  DAGRun run;
  std::vector<TaskInstanceInfo> instances;
};

// Abstract async database interface.
// Implementations: MySQLDatabase (now), PostgresDatabase (future).
// All methods are coroutines returning task<Result<T>>.
// Callers co_await directly — no intermediate Actor/Channel layer needed.
class DatabaseService {
public:
  using TimePoint = std::chrono::system_clock::time_point;
  using RunHistoryEntry = orm::RunHistoryEntry;

  virtual ~DatabaseService() = default;

  // Lifecycle
  virtual auto open() -> task<Result<void>> = 0;
  virtual auto close() -> task<Result<void>> = 0;
  [[nodiscard]] virtual auto is_open() const noexcept -> bool = 0;

  // === DAG CRUD ===
  virtual auto save_dag(const DAGInfo &dag) -> task<Result<int64_t>> = 0;
  virtual auto set_dag_active(const DAGId &dag_id, bool active)
      -> task<Result<void>> = 0;
  virtual auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>> = 0;
  virtual auto get_dag_active(const DAGId &dag_id) -> task<Result<bool>> = 0;
  virtual auto delete_dag(const DAGId &dag_id) -> task<Result<void>> = 0;
  virtual auto get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> = 0;
  virtual auto get_dag_by_rowid(int64_t dag_rowid) -> task<Result<DAGInfo>> = 0;
  virtual auto list_dags() -> task<Result<std::vector<DAGInfo>>> = 0;
  virtual auto list_dag_states()
      -> task<Result<std::vector<DagStateRecord>>> = 0;

  // === Task CRUD ===
  virtual auto save_task(const DAGId &dag_id, const TaskConfig &t)
      -> task<Result<int64_t>> = 0;
  virtual auto delete_task(const DAGId &dag_id, const TaskId &task_id)
      -> task<Result<void>> = 0;
  virtual auto get_tasks(const DAGId &dag_id)
      -> task<Result<std::vector<TaskConfig>>> = 0;

  // === Task Dependencies ===
  virtual auto
  save_task_dependencies(const DAGId &dag_id, const TaskId &task_id,
                         const std::vector<TaskId> &dep_task_ids,
                         std::string_view dependency_type = "success")
      -> task<Result<void>> = 0;
  virtual auto get_task_dependencies(const DAGId &dag_id)
      -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> = 0;
  virtual auto clear_task_dependencies(const DAGId &dag_id)
      -> task<Result<void>> = 0;

  // === DAG Run ===
  virtual auto save_dag_run(const DAGRun &run) -> task<Result<int64_t>> = 0;
  virtual auto update_dag_run_state(const DAGRunId &id, DAGRunState state)
      -> task<Result<void>> = 0;
  virtual auto get_dag_run_state(const DAGRunId &id)
      -> task<Result<DAGRunState>> = 0;
  virtual auto get_incomplete_dag_runs()
      -> task<Result<std::vector<DAGRunId>>> = 0;

  // === Task Instance ===
  virtual auto save_task_instance(const DAGRunId &run_id,
                                  const TaskInstanceInfo &info)
      -> task<Result<void>> = 0;
  virtual auto update_task_instance(const DAGRunId &run_id,
                                    const TaskInstanceInfo &info)
      -> task<Result<void>> = 0;
  virtual auto get_task_instances(const DAGRunId &run_id)
      -> task<Result<std::vector<TaskInstanceInfo>>> = 0;
  virtual auto
  save_task_instances_batch(const DAGRunId &run_id,
                            const std::vector<TaskInstanceInfo> &instances)
      -> task<Result<void>> = 0;
  virtual auto claim_task_instances(std::size_t limit,
                                    std::string_view worker_id)
      -> task<Result<std::vector<ClaimedTaskInstance>>> = 0;
  virtual auto touch_task_heartbeat(const TaskInstanceKey &key)
      -> task<Result<void>> = 0;
  virtual auto reap_zombie_task_instances(std::int64_t heartbeat_timeout_ms)
      -> task<Result<std::size_t>> = 0;

  // === Run History ===
  virtual auto list_run_history(std::size_t limit = 50)
      -> task<Result<std::vector<RunHistoryEntry>>> = 0;
  virtual auto list_dag_run_history(const DAGId &dag_id, std::size_t limit = 50)
      -> task<Result<std::vector<RunHistoryEntry>>> = 0;
  virtual auto get_run_history(const DAGRunId &run_id)
      -> task<Result<RunHistoryEntry>> = 0;

  // === XCom ===
  virtual auto save_xcom(const DAGRunId &run_id, const TaskId &task_id,
                         std::string key, std::string value_json)
      -> task<Result<void>> = 0;
  virtual auto get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                        std::string_view key) -> task<Result<XComEntry>> = 0;
  virtual auto get_task_xcoms(const DAGRunId &run_id, const TaskId &task_id)
      -> task<Result<std::vector<XComEntry>>> = 0;
  virtual auto get_run_xcoms(const DAGRunId &run_id)
      -> task<Result<std::vector<XComTaskEntry>>> = 0;
  virtual auto delete_run_xcoms(const DAGRunId &run_id)
      -> task<Result<void>> = 0;

  // === Scheduling ===
  virtual auto get_last_execution_date(const DAGId &dag_id)
      -> task<Result<TimePoint>> = 0;
  virtual auto run_exists(const DAGId &dag_id, TimePoint execution_time)
      -> task<Result<bool>> = 0;
  virtual auto has_dag_run(const DAGId &dag_id, TimePoint execution_date)
      -> task<Result<bool>> = 0;

  // === Watermarks ===
  virtual auto save_watermark(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> = 0;
  virtual auto get_watermark(const DAGId &dag_id)
      -> task<Result<TimePoint>> = 0;
  virtual auto update_watermark_success(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> = 0;
  virtual auto update_watermark_failure(const DAGId &dag_id, TimePoint ts)
      -> task<Result<void>> = 0;

  // === Previous Task State ===
  virtual auto get_previous_task_state(std::int64_t task_rowid,
                                       TimePoint current_execution_date,
                                       const DAGRunId &current_run_id)
      -> task<Result<TaskState>> = 0;

  // === Task Logs ===
  virtual auto append_task_log(const DAGRunId &run_id, const TaskId &task_id,
                               int attempt, std::string stream,
                               std::string content) -> task<Result<void>> = 0;
  virtual auto get_task_logs(const DAGRunId &run_id, const TaskId &task_id,
                             int attempt, std::size_t limit = 5000)
      -> task<Result<std::vector<orm::TaskLogEntry>>> = 0;
  virtual auto get_run_logs(const DAGRunId &run_id, std::size_t limit = 10000)
      -> task<Result<std::vector<orm::TaskLogEntry>>> = 0;

  // === Cleanup ===
  virtual auto clear_all_dag_data() -> task<Result<void>> = 0;

  // === Recovery ===
  virtual auto mark_incomplete_runs_failed() -> task<Result<std::size_t>> = 0;
};

} // namespace dagforge
