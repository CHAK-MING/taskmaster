#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/run_value_store.hpp"
#include "dagforge/workflow/artifact_store.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"
#include "dagforge/workflow/evidence_ledger.hpp"
#include "dagforge/workflow/evidence_types.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

struct WorkflowCallbacks {
  std::move_only_function<void(const RunSnapshot &)> on_run_state;
  std::move_only_function<void(const WorkflowRunId &, const TaskSnapshot &)>
      on_task_state;
  std::move_only_function<void(const WorkflowRunId &,
                               std::shared_ptr<const RunSnapshot>)>
      on_complete;
};

class WorkflowRuntime {
public:
  WorkflowRuntime(
      Runtime &runtime, ExecutorRegistry &executors,
      std::shared_ptr<IArtifactStore> artifact_store =
          std::make_shared<InMemoryArtifactStore>(),
      std::shared_ptr<EvidenceLedger> evidence_ledger =
          std::make_shared<EvidenceLedger>(),
      std::shared_ptr<CheckpointStore> checkpoint_store =
          std::make_shared<CheckpointStore>(),
      std::size_t max_completed_runs = 10'000);
  ~WorkflowRuntime();

  WorkflowRuntime(const WorkflowRuntime &) = delete;
  auto operator=(const WorkflowRuntime &) -> WorkflowRuntime & = delete;

  [[nodiscard]] auto start(std::shared_ptr<const ExecutionPlan> plan,
                           TriggerEnvelope trigger,
                           WorkflowCallbacks callbacks = {})
      -> Result<WorkflowRunId>;
  auto restore(std::shared_ptr<const ExecutionPlan> plan,
               WorkflowCheckpoint checkpoint) -> Result<void>;

  [[nodiscard]] auto snapshot(const WorkflowRunId &run_id) const
      -> task<Result<std::shared_ptr<const RunSnapshot>>>;
  [[nodiscard]] auto output(const WorkflowRunId &run_id,
                            const OutputRef &output_ref) const
      -> task<Result<std::shared_ptr<const WorkflowValue>>>;
  [[nodiscard]] auto pause(const WorkflowRunId &run_id)
      -> task<Result<void>>;
  [[nodiscard]] auto resume(const WorkflowRunId &run_id)
      -> task<Result<void>>;
  [[nodiscard]] auto cancel(const WorkflowRunId &run_id)
      -> task<Result<void>>;

  [[nodiscard]] auto evidence(const WorkflowRunId &run_id) const
      -> std::vector<EvidenceRecord>;
  [[nodiscard]] auto artifact_store() noexcept -> IArtifactStore & {
    return *artifact_store_;
  }
  [[nodiscard]] auto checkpoint_store() noexcept -> CheckpointStore & {
    return *checkpoint_store_;
  }
  [[nodiscard]] auto active_run_count() const noexcept -> std::uint64_t {
    return active_run_count_.load(std::memory_order_acquire);
  }

private:
  struct TaskRuntimeState {
    TaskSnapshot snapshot;
    std::optional<InstanceId> instance_id;
    io::TimingWheel::Handle retry_handle;
  };

  struct ActiveRun {
    std::shared_ptr<const ExecutionPlan> plan;
    TriggerEnvelope trigger;
    RunSnapshot snapshot;
    std::vector<TaskRuntimeState> tasks;
    std::unique_ptr<RunValueStore> values;
    std::deque<std::size_t> ready;
    std::size_t active_attempts{0};
    bool dispatching{false};
    io::TimingWheel::Handle deadline_handle;
    WorkflowCallbacks callbacks;
  };

  struct ShardState {
    std::unordered_map<std::string, ActiveRun> active_runs;
    std::unordered_map<std::string, std::shared_ptr<const RunSnapshot>>
        completed_runs;
    std::unordered_map<std::string,
                       std::vector<std::pair<OutputRef, WorkflowValue>>>
        completed_values;
    std::deque<std::string> completed_order;
  };

  [[nodiscard]] auto owner_shard(const WorkflowRunId &run_id) const noexcept
      -> shard_id;
  auto initialize_run(WorkflowRunId run_id,
                      std::shared_ptr<const ExecutionPlan> plan,
                      TriggerEnvelope trigger, WorkflowCallbacks callbacks)
      -> void;
  auto dispatch(const WorkflowRunId &run_id) -> void;
  auto start_task(const WorkflowRunId &run_id, std::size_t task_index) -> void;
  auto start_async_task(WorkflowRunId run_id, std::size_t task_index,
                        AttemptId attempt_id)
      -> spawn_task;
  auto complete_task(const WorkflowRunId &run_id, std::size_t task_index,
                     const AttemptId &attempt_id,
                     Result<ExecutorOutputs> result)
      -> void;
  auto finalize_run_if_ready(const WorkflowRunId &run_id) -> bool;
  auto update_dependents(const WorkflowRunId &run_id,
                         std::size_t completed_index) -> void;
  auto schedule_retry(const WorkflowRunId &run_id, std::size_t task_index)
      -> void;
  auto request_stop(const WorkflowRunId &run_id, StopIntent intent,
                    std::string reason) -> Result<void>;
  auto settle_control_state(const WorkflowRunId &run_id) -> void;
  [[nodiscard]] auto begin_attempt(ActiveRun &run, std::size_t task_index)
      -> AttemptId;
  auto mark_attempt_running(ActiveRun &run, std::size_t task_index,
                            const AttemptId &attempt_id) -> void;
  [[nodiscard]] auto active_attempt(TaskRuntimeState &task,
                                    const AttemptId &attempt_id)
      -> AttemptSnapshot *;
  auto transition_run(ActiveRun &run, RunState state) -> Result<void>;
  auto transition_task(ActiveRun &run, std::size_t task_index,
                       TaskState state) -> Result<void>;
  auto transition_attempt(AttemptSnapshot &attempt, AttemptState state)
      -> Result<void>;
  [[nodiscard]] auto invariants_hold(const ActiveRun &run) const noexcept
      -> bool;
  [[nodiscard]] auto conditions_pass(const ActiveRun &run,
                                     std::size_t node_index) const
      -> Result<bool>;
  [[nodiscard]] auto input_values(const ActiveRun &run,
                                  std::size_t node_index) const
      -> Result<ExecutorInputs>;
  [[nodiscard]] auto make_snapshot(const ActiveRun &run) const
      -> std::shared_ptr<const RunSnapshot>;
  auto emit_run_state(ActiveRun &run) -> void;
  auto emit_task_state(ActiveRun &run, std::size_t task_index) -> void;
  auto append_evidence(const ActiveRun &run, std::size_t node_index,
                       EvidenceType type, JsonValue metadata = {}) -> void;
  auto checkpoint(ActiveRun &run) -> void;

  [[nodiscard]] auto execute_task(WorkflowRunId run_id,
                                  std::size_t task_index,
                                  AttemptId attempt_id, NodePlan node,
                                  ExecutorInputs inputs)
      -> task<Result<ExecutorOutputs>>;

  Runtime &runtime_;
  ExecutorRegistry &executors_;
  std::shared_ptr<IArtifactStore> artifact_store_;
  std::shared_ptr<EvidenceLedger> evidence_ledger_;
  std::shared_ptr<CheckpointStore> checkpoint_store_;
  std::size_t max_completed_runs_{10'000};
  std::vector<ShardState> shard_states_;
  std::shared_ptr<int> lifetime_token_{std::make_shared<int>(0)};
  mutable std::mutex idempotency_mutex_;
  std::unordered_map<std::string, WorkflowRunId> idempotency_runs_;
  std::atomic<std::uint64_t> active_run_count_{0};
};

} // namespace dagforge::workflow
