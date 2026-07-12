#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/workflow/node_configs.hpp"
#include "dagforge/workflow/run_value_store.hpp"
#include "dagforge/workflow/workflow_storage.hpp"
#include "dagforge/workflow/workflow_types.hpp"

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

using NodeOutputs = std::vector<std::pair<WorkflowPortId, WorkflowValue>>;

struct WorkflowAdapters {
  std::move_only_function<task<Result<ModelResponse>>(ModelCall)> invoke_model;
  std::move_only_function<task<Result<ToolResult>>(ToolInvocation)> invoke_tool;
};

struct ApprovalRequest {
  ApprovalId approval_id;
  WorkflowRunId run_id;
  WorkflowNodeId node_id;
  Principal requested_by;
  std::string summary;
  JsonValue context;
  std::chrono::system_clock::time_point expires_at{};
};

struct WorkflowCallbacks {
  std::move_only_function<void(const RunSnapshot &)> on_run_state;
  std::move_only_function<void(const WorkflowRunId &, const NodeSnapshot &)>
      on_node_state;
  std::move_only_function<void(const ApprovalRequest &)> on_approval_requested;
  std::move_only_function<void(const WorkflowRunId &,
                               std::shared_ptr<const RunSnapshot>)>
      on_complete;
};

class WorkflowRuntime {
public:
  WorkflowRuntime(
      Runtime &runtime, IExecutor &executor,
      std::shared_ptr<IArtifactStore> artifact_store =
          std::make_shared<InMemoryArtifactStore>(),
      std::shared_ptr<EvidenceLedger> evidence_ledger =
          std::make_shared<EvidenceLedger>(),
      std::shared_ptr<CheckpointStore> checkpoint_store =
          std::make_shared<CheckpointStore>(),
      WorkflowAdapters adapters = {});
  ~WorkflowRuntime();

  WorkflowRuntime(const WorkflowRuntime &) = delete;
  auto operator=(const WorkflowRuntime &) -> WorkflowRuntime & = delete;

  [[nodiscard]] auto start(std::shared_ptr<const ExecutionPlan> plan,
                           TriggerEnvelope trigger,
                           WorkflowCallbacks callbacks = {})
      -> Result<WorkflowRunId>;

  [[nodiscard]] auto snapshot(const WorkflowRunId &run_id) const
      -> task<Result<std::shared_ptr<const RunSnapshot>>>;
  [[nodiscard]] auto output(const WorkflowRunId &run_id,
                            const OutputRef &output_ref) const
      -> task<Result<std::shared_ptr<const WorkflowValue>>>;
  [[nodiscard]] auto pending_approvals(const WorkflowRunId &run_id) const
      -> task<Result<std::vector<ApprovalRequest>>>;

  [[nodiscard]] auto approve(const WorkflowRunId &run_id,
                             const ApprovalId &approval_id, bool approved,
                             Principal actor, std::string comment = {})
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
  using InputMap = std::unordered_map<
      std::string, std::shared_ptr<const WorkflowValue>>;

  struct NodeRuntimeState {
    NodeSnapshot snapshot;
    std::optional<ApprovalRequest> approval;
    std::optional<ComputeTaskHandle> compute_handle;
    std::optional<InstanceId> instance_id;
  };

  struct ActiveRun {
    std::shared_ptr<const ExecutionPlan> plan;
    TriggerEnvelope trigger;
    RunSnapshot snapshot;
    std::vector<NodeRuntimeState> nodes;
    std::unique_ptr<RunValueStore> values;
    std::deque<std::size_t> ready;
    std::size_t active_nodes{0};
    std::uint64_t model_tokens_used{0};
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
  };

  [[nodiscard]] auto owner_shard(const WorkflowRunId &run_id) const noexcept
      -> shard_id;
  auto initialize_run(WorkflowRunId run_id,
                      std::shared_ptr<const ExecutionPlan> plan,
                      TriggerEnvelope trigger, WorkflowCallbacks callbacks)
      -> void;
  auto dispatch(const WorkflowRunId &run_id) -> void;
  auto start_node(const WorkflowRunId &run_id, std::size_t node_index) -> void;
  auto start_async_node(const WorkflowRunId &run_id, std::size_t node_index)
      -> spawn_task;
  auto start_compute_node(const WorkflowRunId &run_id, std::size_t node_index)
      -> void;
  auto start_approval_node(const WorkflowRunId &run_id, std::size_t node_index)
      -> void;
  auto complete_node(const WorkflowRunId &run_id, std::size_t node_index,
                     Result<NodeOutputs> result) -> void;
  auto complete_run_if_terminal(const WorkflowRunId &run_id) -> bool;
  auto update_dependents(const WorkflowRunId &run_id,
                         std::size_t completed_index) -> void;
  [[nodiscard]] auto conditions_pass(const ActiveRun &run,
                                     std::size_t node_index) const
      -> Result<bool>;
  [[nodiscard]] auto input_values(const ActiveRun &run,
                                  std::size_t node_index) const
      -> Result<InputMap>;
  [[nodiscard]] auto make_snapshot(const ActiveRun &run) const
      -> std::shared_ptr<const RunSnapshot>;
  auto emit_run_state(ActiveRun &run) -> void;
  auto emit_node_state(ActiveRun &run, std::size_t node_index) -> void;
  auto append_evidence(const ActiveRun &run, std::size_t node_index,
                       EvidenceType type, JsonValue metadata = {}) -> void;
  auto checkpoint(ActiveRun &run) -> void;
  auto fail_run(ActiveRun &run, std::string error) -> void;

  [[nodiscard]] auto execute_process_node(WorkflowRunId run_id,
                                          NodePlan node, InputMap inputs)
      -> task<Result<NodeOutputs>>;
  [[nodiscard]] auto execute_http_node(WorkflowRunId run_id, NodePlan node,
                                       InputMap inputs)
      -> task<Result<NodeOutputs>>;
  [[nodiscard]] auto execute_model_node(WorkflowRunId run_id, NodePlan node,
                                        InputMap inputs, TriggerEnvelope trigger)
      -> task<Result<NodeOutputs>>;
  [[nodiscard]] auto execute_tool_node(WorkflowRunId run_id, NodePlan node,
                                       InputMap inputs)
      -> task<Result<NodeOutputs>>;
  [[nodiscard]] auto execute_inline_node(const NodePlan &node,
                                         const InputMap &inputs) const
      -> Result<NodeOutputs>;
  [[nodiscard]] static auto execute_compute_work(NodePlan node,
                                                 InputMap inputs,
                                                 std::stop_token stop_token)
      -> Result<NodeOutputs>;

  Runtime &runtime_;
  IExecutor &executor_;
  std::shared_ptr<IArtifactStore> artifact_store_;
  std::shared_ptr<EvidenceLedger> evidence_ledger_;
  std::shared_ptr<CheckpointStore> checkpoint_store_;
  WorkflowAdapters adapters_;
  std::vector<ShardState> shard_states_;
  std::shared_ptr<int> lifetime_token_{std::make_shared<int>(0)};
  mutable std::mutex idempotency_mutex_;
  std::unordered_map<std::string, WorkflowRunId> idempotency_runs_;
  std::atomic<std::uint64_t> active_run_count_{0};
};

} // namespace dagforge::workflow
