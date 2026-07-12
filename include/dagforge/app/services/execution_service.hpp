#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_config.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/xcom/template_resolver.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <flat_map>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge {

class Runtime;
class IExecutor;
struct TaskConfig;
struct XComPushConfig;

struct ExecutionCallbacks {
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               TaskState status)>
      on_task_status;
  std::move_only_function<void(const DAGRunId &dag_run_id, DAGRunState status)>
      on_run_status;
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               int attempt, std::string_view stream,
                               std::string_view msg)>
      on_log;
  // Fire-and-forget: called once when a run finishes; zero copies via
  // shared_ptr.
  std::move_only_function<void(const DAGRunId &dag_run_id, const DAGRun &run)>
      on_run_completed;
  // Fire-and-forget: called once when a run finishes; zero copies via
  // shared_ptr.
  std::move_only_function<void(std::shared_ptr<const DAGRun> run)>
      on_persist_run;
  // Fire-and-forget: per-task status persisted as it changes.
  std::move_only_function<void(const DAGRunId &dag_run_id,
                               const TaskInstanceInfo &info)>
      on_persist_task;
  // Fire-and-forget: XCom values are sent to the persistence actor.
  std::move_only_function<void(const DAGRunId &dag_run_id, const TaskId &task,
                               std::string_view key,
                               std::string_view value_json)>
      on_persist_xcom;
  // Async reads — all go through the persistence Actor (ask pattern).
  std::move_only_function<task<Result<XComEntry>>(
      const DAGRunId &dag_run_id, const TaskId &task, std::string key)>
      get_xcom;
  std::move_only_function<task<Result<std::vector<XComEntry>>>(
      const DAGRunId &dag_run_id, const TaskId &task)>
      get_task_xcoms;
  std::move_only_function<task<Result<std::vector<XComTaskEntry>>>(
      const DAGRunId &dag_run_id)>
      get_run_xcoms;
  std::move_only_function<task<Result<DAGId>>(const DAGRunId &dag_run_id)>
      get_dag_id_by_run;
  std::move_only_function<task<Result<TaskState>>(
      std::int64_t task_rowid,
      std::chrono::system_clock::time_point execution_date,
      const DAGRunId &current_dag_run_id)>
      check_previous_task_state;
  // Synchronous reads from in-memory DAGManager (no DB round-trip needed).
  std::move_only_function<int(const DAGRunId &dag_run_id, NodeIndex idx)>
      get_max_retries;
  std::move_only_function<std::chrono::seconds(const DAGRunId &dag_run_id,
                                               NodeIndex idx)>
      get_retry_interval;
  std::move_only_function<void(const TaskInstanceKey &key)> on_task_heartbeat;
};

class ExecutionService {
public:
  enum class XComMetricSource : std::uint8_t {
    Cache = 0,
    RunBatch,
    TaskBatch,
    Single,
    BranchCache,
    BranchSingle,
    Count,
  };

  ExecutionService(Runtime &runtime, IExecutor &executor);
  ~ExecutionService();

  ExecutionService(const ExecutionService &) = delete;
  auto operator=(const ExecutionService &) -> ExecutionService & = delete;

  auto set_callbacks(ExecutionCallbacks callbacks) -> void;
  auto set_local_task_lease_timeout(std::chrono::milliseconds timeout) -> void;

  struct RunContext {
    std::unique_ptr<DAGRun> run;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig::Compiled>> task_configs;
    std::optional<DAGId> dag_id;
    std::unordered_map<std::string, std::string, StringHash, StringEqual>
        conf_values;
  };

  auto start_run(const DAGRunId &dag_run_id, RunContext ctx) -> void;

  [[nodiscard]] auto get_cached_dag_id(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;

  // Cross-shard safe query that returns a deep snapshot.
  [[nodiscard]] auto get_run_snapshot(const DAGRunId &dag_run_id) const
      -> task<Result<std::shared_ptr<const DAGRun>>>;

  [[nodiscard]] auto has_active_runs() const -> bool;

  [[nodiscard]] auto wait_for_completion_async(int timeout_ms)
      -> task<Result<void>>;

  [[nodiscard]] auto coro_count() const -> int;
  [[nodiscard]] auto dispatch_invocations() const -> std::uint64_t;
  [[nodiscard]] auto dispatch_scan_invocations() const -> std::uint64_t;
  [[nodiscard]] auto dispatch_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_scan_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_ready_snapshot_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_candidate_materialize_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_dependency_check_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_launch_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto dispatch_spawn_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto task_runs_total() const -> std::uint64_t;
  [[nodiscard]] auto task_successes_total() const -> std::uint64_t;
  [[nodiscard]] auto task_failures_total() const -> std::uint64_t;
  [[nodiscard]] auto task_failure_error_type_totals() const
      -> std::array<std::uint64_t, 9>;
  [[nodiscard]] auto task_skipped_total() const -> std::uint64_t;
  [[nodiscard]] auto queue_depth_total() const -> std::size_t;
  [[nodiscard]] auto executor_active_count(ExecutorType type) const
      -> std::uint64_t;
  [[nodiscard]] auto executor_start_total(ExecutorType type) const
      -> std::uint64_t;
  [[nodiscard]] auto task_duration_histogram() const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto xcom_prefetch_requests_total(XComMetricSource source) const
      -> std::uint64_t;
  [[nodiscard]] auto xcom_prefetch_hits_total(XComMetricSource source) const
      -> std::uint64_t;
  [[nodiscard]] auto xcom_push_entries_total() const -> std::uint64_t;
  [[nodiscard]] auto xcom_push_extraction_failures_total() const
      -> std::uint64_t;

  auto set_max_concurrency(int max_concurrency) -> void;

  [[nodiscard]] auto runtime() noexcept -> Runtime & { return runtime_; }
  [[nodiscard]] auto executor() noexcept -> IExecutor & { return executor_; }
  [[nodiscard]] auto template_resolver() noexcept -> TemplateResolver & {
    return template_resolver_;
  }
  auto emit_log_chunk(const DAGRunId &dag_run_id, const TaskId &task_id,
                      int attempt, std::string_view stream,
                      std::string_view msg) -> void;
  auto on_executor_heartbeat(const DAGRunId &dag_run_id,
                             const InstanceId &instance_id) -> void;

  struct TaskJob {
    NodeIndex idx{kInvalidNode};
    InstanceId inst_id;
    // Borrowed from run_state.task_configs; valid while the run is in-flight.
    const TaskConfig::Compiled *task_config{nullptr};
    // Borrowed from run_state.executor_configs; valid while the run is
    // in-flight.
    const ExecutorConfig *executor_config{nullptr};
    TaskInstanceKey task_instance_key{};
    int attempt{0};
    std::chrono::system_clock::time_point execution_date{};
    std::chrono::system_clock::time_point data_interval_start{};
    std::chrono::system_clock::time_point data_interval_end{};

    [[nodiscard]] auto task_id() const noexcept -> const TaskId & {
      return task_config->task_id;
    }
    [[nodiscard]] auto cfg() const noexcept -> const ExecutorConfig & {
      return *executor_config;
    }
    [[nodiscard]] auto xcom_push() const noexcept
        -> const std::vector<XComPushConfig> & {
      return task_config->xcom_push;
    }
    [[nodiscard]] auto xcom_pull() const noexcept
        -> const std::vector<XComPullConfig> & {
      return task_config->xcom_pull;
    }
  };

private:
  enum class DispatchState : std::uint8_t {
    Idle = 0,
    Queued,
    Dispatching,
    DispatchingQueued,
  };

  enum class DispatchScanState : std::uint8_t {
    Idle = 0,
    Active,
    Rearmed,
  };

  struct ActiveRunState {
    using XComValueMap =
        std::unordered_map<std::string, std::string, StringHash, StringEqual>;
    using XComTaskCache =
        std::unordered_map<std::string_view, XComValueMap, StringHash,
                           StringEqual>;

    std::unique_ptr<DAGRun> run;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig::Compiled>> task_configs;
    std::optional<DAGId> dag_id;
    std::unordered_map<std::string, std::string, StringHash, StringEqual>
        conf_values;
    XComTaskCache xcom_cache;
    DispatchState dispatch_state{DispatchState::Idle};
  };

  struct TransitionEffects {
    std::optional<TaskInstanceInfo> persisted_task_info;
    std::vector<TaskInstanceInfo> persisted_infos;
    std::vector<std::pair<TaskId, TaskState>> status_updates;
    std::shared_ptr<const DAGRun> completed_snapshot;
    bool ready_tasks_available{false};
  };

  using RunMap = std::flat_map<DAGRunId, ActiveRunState>;

  // ---- Per-shard state (single-writer, no mutex needed on write path) ----
  struct ShardState {
    RunMap runs;
    std::flat_map<DAGRunId, std::shared_ptr<const DAGRun>> completed_runs;
    std::deque<DAGRunId> completed_run_order;
    std::deque<DAGRunId> ready_run_queue;
    struct ActiveLocalTaskLease {
      DAGRunId dag_run_id;
      TaskId task_id;
      InstanceId instance_id;
      TaskInstanceKey task_instance_key{};
      NodeIndex idx{kInvalidNode};
      ExecutorType executor_type{ExecutorType::Shell};
      int attempt{0};
      std::chrono::steady_clock::time_point task_started_at{};
      io::TimingWheel::Handle handle{};
    };
    std::unordered_map<std::string, ActiveLocalTaskLease> local_task_leases;
    DispatchScanState dispatch_scan_state{DispatchScanState::Idle};
  };

  struct alignas(64) QueueSizeCounter {
    std::atomic<std::uint32_t> value{0};
  };

  auto dispatch(DAGRunId dag_run_id) -> task<Result<void>>;
  auto dispatch_pending() -> void;
  auto dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task;
  auto dispatch_after_delay(DAGRunId dag_run_id, NodeIndex retry_idx,
                            std::chrono::seconds delay) -> spawn_task;
  auto run_task(DAGRunId dag_run_id, TaskJob job) -> spawn_task;
  auto on_task_success(const DAGRunId &dag_run_id, NodeIndex idx)
      -> task<Result<void>>;
  auto on_task_failure(const DAGRunId &dag_run_id, NodeIndex idx,
                       std::string_view error, int exit_code, bool timed_out)
      -> Result<bool>;
  auto on_task_skipped(const DAGRunId &dag_run_id, NodeIndex idx)
      -> Result<void>;
  auto on_task_fail_immediately(const DAGRunId &dag_run_id, NodeIndex idx,
                                std::string_view error, int exit_code)
      -> Result<void>;
  auto record_task_failure_error_type(std::string_view error_type) -> void;
  auto record_task_snapshot(const DAGRun &run, NodeIndex idx,
                            TransitionEffects &effects) const -> void;
  auto collect_transition_delta(const DAGRun &run,
                                const DAGRun::TransitionDelta &delta,
                                TransitionEffects &effects) const -> void;
  auto complete_transition_if_needed(ShardState &state, RunMap::iterator it,
                                     TransitionEffects &effects) -> void;
  auto finalize_task_transition(const DAGRunId &dag_run_id,
                                TransitionEffects effects) -> void;
  auto on_run_complete(std::shared_ptr<const DAGRun> run,
                       const DAGRunId &dag_run_id) -> Result<void>;
  auto maybe_persist_task(const DAGRunId &dag_run_id,
                          const TaskInstanceInfo &info) -> void;
  auto maybe_persist_tasks(const DAGRunId &dag_run_id,
                           std::span<const TaskInstanceInfo> infos) -> void;
  [[nodiscard]] auto local_task_lease_enabled() const noexcept -> bool;
  auto
  arm_local_task_lease(const DAGRunId &dag_run_id, const TaskJob &job,
                       std::chrono::steady_clock::time_point task_started_at)
      -> void;
  auto refresh_local_task_lease(const DAGRunId &dag_run_id,
                                const InstanceId &instance_id)
      -> std::optional<TaskInstanceKey>;
  [[nodiscard]] auto consume_local_task_lease(const DAGRunId &dag_run_id,
                                              const InstanceId &instance_id)
      -> bool;
  auto expire_local_task_lease(const DAGRunId &dag_run_id,
                               const InstanceId &instance_id) -> void;
  [[nodiscard]] auto begin_task_execution(const DAGRunId &dag_run_id,
                                          const TaskJob &job)
      -> std::chrono::steady_clock::time_point;
  auto
  finalize_task_execution(const DAGRunId &dag_run_id, TaskJob &job,
                          ExecutorResult result,
                          std::chrono::steady_clock::time_point task_started_at,
                          shard_id target) -> task<void>;
  auto run_noop_task_inline(const DAGRunId &dag_run_id, TaskJob &job)
      -> task<void>;

  [[nodiscard]] auto owner_shard(const DAGRunId &dag_run_id) const noexcept
      -> shard_id;
  auto post_to_owner(const DAGRunId &dag_run_id,
                     std::move_only_function<void()> fn) -> void;
  [[nodiscard]] auto copy_run_snapshot(const ShardState &state,
                                       const DAGRunId &dag_run_id) const
      -> Result<std::shared_ptr<const DAGRun>>;
  auto remember_completed_run(const DAGRunId &dag_run_id,
                              std::shared_ptr<const DAGRun> run)
      -> void;
  [[nodiscard]] auto shard_state(const DAGRunId &dag_run_id) -> ShardState & {
    return shard_states_[owner_shard(dag_run_id)];
  }
  auto dispatch_pending_on_shard(shard_id sid) -> void;
  auto notify_capacity_available() -> void;
  auto schedule_dispatch_scan(shard_id sid) -> void;
  auto schedule_dispatch(const DAGRunId &dag_run_id) -> void;
  auto enqueue_ready_run(ShardState &state, ActiveRunState &run_state,
                         const DAGRunId &dag_run_id) -> bool;
  auto schedule_dispatch_on_owner(ActiveRunState &run_state,
                                  const DAGRunId &dag_run_id) -> void;
  auto finish_dispatch_cycle(shard_id sid, const DAGRunId &dag_run_id) -> void;

  Runtime &runtime_;
  IExecutor &executor_;
  ExecutionCallbacks callbacks_;
  TemplateResolver template_resolver_;

  std::vector<ShardState> shard_states_;
  std::vector<QueueSizeCounter> ready_run_queue_sizes_;
  std::atomic<std::uint32_t> notify_rr_{0};
  std::shared_ptr<int> lifetime_token_{std::make_shared<int>(0)};
  std::atomic<int> active_run_count_{0};
  std::atomic<int> coro_count_{0};
  std::atomic<int> running_tasks_{0};
  std::atomic<std::uint64_t> dispatch_invocations_{0};
  std::atomic<std::uint64_t> dispatch_scan_invocations_{0};
  metrics::Histogram dispatch_duration_histogram_{};
  metrics::Histogram dispatch_scan_duration_histogram_{};
  metrics::Histogram dispatch_ready_snapshot_duration_histogram_{};
  metrics::Histogram dispatch_candidate_materialize_duration_histogram_{};
  metrics::Histogram dispatch_dependency_check_duration_histogram_{};
  metrics::Histogram dispatch_launch_duration_histogram_{};
  metrics::Histogram dispatch_spawn_duration_histogram_{};
  std::atomic<std::uint64_t> task_runs_total_{0};
  std::atomic<std::uint64_t> task_successes_total_{0};
  std::atomic<std::uint64_t> task_failures_total_{0};
  std::array<std::atomic<std::uint64_t>, 9> task_failure_error_type_totals_{};
  std::atomic<std::uint64_t> task_skipped_total_{0};
  std::array<std::atomic<std::uint64_t>, 5> executor_active_counts_{};
  std::array<std::atomic<std::uint64_t>, 5> executor_start_totals_{};
  std::array<std::atomic<std::uint64_t>,
             static_cast<std::size_t>(XComMetricSource::Count)>
      xcom_prefetch_requests_{};
  std::array<std::atomic<std::uint64_t>,
             static_cast<std::size_t>(XComMetricSource::Count)>
      xcom_prefetch_hits_{};
  std::atomic<std::uint64_t> xcom_push_entries_total_{0};
  std::atomic<std::uint64_t> xcom_push_extraction_failures_total_{0};
  metrics::Histogram task_duration_histogram_{
      {1'000'000ULL, 5'000'000ULL, 10'000'000ULL, 25'000'000ULL, 50'000'000ULL,
       100'000'000ULL, 250'000'000ULL, 500'000'000ULL, 1'000'000'000ULL,
       2'500'000'000ULL, 5'000'000'000ULL, 10'000'000'000ULL}};
  std::chrono::milliseconds local_task_lease_timeout_{0};
  int max_concurrency_{100};

  friend struct ExecutionVisitor;
};

} // namespace dagforge
