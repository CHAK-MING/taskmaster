#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>
#endif

namespace dagforge::workflow {

enum class RunState : std::uint8_t {
  Running,
  Pausing,
  Paused,
  Stopping,
  Succeeded,
  Failed,
  Cancelled,
};

enum class StopIntent : std::uint8_t {
  Succeed,
  Fail,
  Cancel,
};

enum class TaskState : std::uint8_t {
  Pending,
  Ready,
  Running,
  RetryWaiting,
  Succeeded,
  Failed,
  Skipped,
  Cancelled,
};

enum class AttemptState : std::uint8_t {
  Starting,
  Running,
  Terminating,
  Succeeded,
  Failed,
  TimedOut,
  Cancelled,
};

enum class FailureClass : std::uint8_t {
  Retryable,
  Permanent,
  Cancelled,
  Timeout,
  Infrastructure,
};

enum class FailurePolicy : std::uint8_t {
  ContinueIndependent,
  FailFast,
};

enum class SkipReason : std::uint8_t {
  ConditionFalse,
  UpstreamFailed,
  UpstreamCancelled,
  BranchNotSelected,
};

enum class TerminationReason : std::uint8_t {
  RunCancelled,
  RunFailed,
  AttemptTimeout,
};

enum class ConditionKind : std::uint8_t {
  Always,
  BoolEquals,
  StringEquals,
};

enum class EvidenceType : std::uint8_t {
  TriggerReceived,
  PlanCompiled,
  PolicyAccepted,
  PolicyRejected,
  TaskStarted,
  TaskCompleted,
  TaskFailed,
  RunPauseRequested,
  RunPaused,
  RunResumed,
  RunStopRequested,
  AttemptStarted,
  AttemptCompleted,
  Checkpoint,
  RunCompleted,
  RunFailed,
  RunCancelled,
};

struct Principal {
  std::string subject;
  std::vector<std::string> roles;
};

struct TraceContext {
  std::string trace_id;
  std::string parent_span_id;
};

struct KeyValue {
  std::string key;
  std::string value;
};

struct InputEnvironmentBinding {
  std::string input;
  std::string environment;
};

struct CommandNodeConfig {
  std::string program;
  std::vector<std::string> arguments;
  std::vector<KeyValue> env;
  std::vector<InputEnvironmentBinding> input_env;
};

struct ArtifactRef {
  ArtifactId artifact_id;
  std::string media_type{"application/octet-stream"};
  std::uint64_t size_bytes{0};
  std::string digest;
};

using WorkflowValue =
    std::variant<std::monostate, bool, std::int64_t, double, std::string,
                 JsonValue, ArtifactRef>;

struct OutputRef {
  WorkflowNodeId node_id;
  WorkflowPortId port;

  auto operator==(const OutputRef &) const -> bool = default;
};

struct InputBinding {
  WorkflowPortId input;
  OutputRef source;
};

struct ConditionExpr {
  ConditionKind kind{ConditionKind::Always};
  bool expected_bool{true};
  std::string expected_string;
};

struct ConditionalEdge {
  OutputRef source;
  WorkflowNodeId target;
  ConditionExpr condition;
};

struct ResourceBudget {
  std::size_t max_nodes{256};
  std::size_t max_parallel_nodes{32};
  std::uint64_t max_total_output_bytes{64ULL * 1024ULL * 1024ULL};
  std::chrono::milliseconds max_run_duration{std::chrono::hours(1)};
};

struct WorkflowPolicy {
  FailurePolicy failure_policy{FailurePolicy::ContinueIndependent};
  ResourceBudget budget;
};

struct NodePlan {
  WorkflowNodeId node_id;
  std::string name;
  CommandNodeConfig command;
  std::vector<InputBinding> inputs;
  std::vector<WorkflowPortId> outputs;
  int max_retries{0};
  std::chrono::milliseconds retry_initial_delay{std::chrono::seconds(1)};
  std::chrono::milliseconds retry_max_delay{std::chrono::seconds(30)};
  std::chrono::seconds timeout{std::chrono::minutes(5)};
  bool checkpoint{false};
};

struct WorkflowPlan {
  WorkflowId workflow_id;
  std::uint32_t schema_version{1};
  std::vector<NodePlan> nodes;
  std::vector<ConditionalEdge> edges;
  std::vector<OutputRef> outputs;
  WorkflowPolicy policy;
};

struct CompiledNode {
  std::size_t index{0};
  NodePlan plan;
  std::vector<std::size_t> dependencies;
  std::vector<std::size_t> dependents;
};

struct ExecutionPlan {
  WorkflowPlanId plan_id;
  WorkflowId workflow_id;
  std::string digest;
  std::vector<CompiledNode> nodes;
  std::vector<ConditionalEdge> edges;
  std::vector<std::size_t> topological_order;
  std::vector<OutputRef> outputs;
  WorkflowPolicy policy;
};

struct TriggerEnvelope {
  WorkflowTriggerId trigger_id;
  WorkflowId workflow_id;
  std::string source;
  std::string event_type;
  WorkflowValue payload;
  std::string idempotency_key;
  Principal principal;
  TraceContext trace;
  std::chrono::system_clock::time_point occurred_at{
      std::chrono::system_clock::now()};
};

struct AttemptSnapshot {
  AttemptId attempt_id;
  std::uint32_t number{0};
  AttemptState state{AttemptState::Starting};
  std::optional<TerminationReason> termination_reason;
  std::optional<FailureClass> failure_class;
  std::optional<int> exit_code;
  std::string error;
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
};

struct TaskSnapshot {
  WorkflowNodeId node_id;
  TaskState state{TaskState::Pending};
  std::uint32_t attempt_count{0};
  std::optional<AttemptId> active_attempt_id;
  std::optional<std::chrono::system_clock::time_point> next_attempt_at;
  std::optional<SkipReason> skip_reason;
  std::string last_error;
  std::vector<AttemptSnapshot> attempts;
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
};

struct RunSnapshot {
  WorkflowRunId run_id;
  WorkflowId workflow_id;
  WorkflowPlanId plan_id;
  RunState state{RunState::Running};
  std::optional<StopIntent> stop_intent;
  std::string stop_reason;
  std::vector<TaskSnapshot> tasks;
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  std::string error;
};

[[nodiscard]] constexpr auto to_string_view(RunState value) noexcept
    -> std::string_view {
  switch (value) {
  case RunState::Running:
    return "running";
  case RunState::Pausing:
    return "pausing";
  case RunState::Paused:
    return "paused";
  case RunState::Stopping:
    return "stopping";
  case RunState::Succeeded:
    return "succeeded";
  case RunState::Failed:
    return "failed";
  case RunState::Cancelled:
    return "cancelled";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(StopIntent value) noexcept
    -> std::string_view {
  switch (value) {
  case StopIntent::Succeed:
    return "succeed";
  case StopIntent::Fail:
    return "fail";
  case StopIntent::Cancel:
    return "cancel";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(TaskState value) noexcept
    -> std::string_view {
  switch (value) {
  case TaskState::Pending:
    return "pending";
  case TaskState::Ready:
    return "ready";
  case TaskState::Running:
    return "running";
  case TaskState::RetryWaiting:
    return "retry_waiting";
  case TaskState::Succeeded:
    return "succeeded";
  case TaskState::Failed:
    return "failed";
  case TaskState::Skipped:
    return "skipped";
  case TaskState::Cancelled:
    return "cancelled";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(AttemptState value) noexcept
    -> std::string_view {
  switch (value) {
  case AttemptState::Starting:
    return "starting";
  case AttemptState::Running:
    return "running";
  case AttemptState::Terminating:
    return "terminating";
  case AttemptState::Succeeded:
    return "succeeded";
  case AttemptState::Failed:
    return "failed";
  case AttemptState::TimedOut:
    return "timed_out";
  case AttemptState::Cancelled:
    return "cancelled";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(SkipReason value) noexcept
    -> std::string_view {
  switch (value) {
  case SkipReason::ConditionFalse:
    return "condition_false";
  case SkipReason::UpstreamFailed:
    return "upstream_failed";
  case SkipReason::UpstreamCancelled:
    return "upstream_cancelled";
  case SkipReason::BranchNotSelected:
    return "branch_not_selected";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(FailurePolicy value) noexcept
    -> std::string_view {
  switch (value) {
  case FailurePolicy::ContinueIndependent:
    return "continue_independent";
  case FailurePolicy::FailFast:
    return "fail_fast";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(FailureClass value) noexcept
    -> std::string_view {
  switch (value) {
  case FailureClass::Retryable:
    return "retryable";
  case FailureClass::Permanent:
    return "permanent";
  case FailureClass::Cancelled:
    return "cancelled";
  case FailureClass::Timeout:
    return "timeout";
  case FailureClass::Infrastructure:
    return "infrastructure";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(TerminationReason value) noexcept
    -> std::string_view {
  switch (value) {
  case TerminationReason::RunCancelled:
    return "run_cancelled";
  case TerminationReason::RunFailed:
    return "run_failed";
  case TerminationReason::AttemptTimeout:
    return "attempt_timeout";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(ConditionKind value) noexcept
    -> std::string_view {
  switch (value) {
  case ConditionKind::Always:
    return "always";
  case ConditionKind::BoolEquals:
    return "bool_equals";
  case ConditionKind::StringEquals:
    return "string_equals";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto is_terminal(RunState state) noexcept -> bool {
  return state == RunState::Succeeded || state == RunState::Failed ||
         state == RunState::Cancelled;
}

[[nodiscard]] constexpr auto is_terminal(TaskState state) noexcept -> bool {
  return state == TaskState::Succeeded || state == TaskState::Failed ||
         state == TaskState::Skipped || state == TaskState::Cancelled;
}

[[nodiscard]] constexpr auto is_terminal(AttemptState state) noexcept -> bool {
  return state == AttemptState::Succeeded || state == AttemptState::Failed ||
         state == AttemptState::TimedOut || state == AttemptState::Cancelled;
}

[[nodiscard]] constexpr auto can_transition(RunState from,
                                            RunState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case RunState::Running:
    return to == RunState::Pausing || to == RunState::Stopping ||
           to == RunState::Succeeded || to == RunState::Failed;
  case RunState::Pausing:
    return to == RunState::Paused || to == RunState::Stopping ||
           to == RunState::Succeeded || to == RunState::Failed;
  case RunState::Paused:
    return to == RunState::Running || to == RunState::Stopping;
  case RunState::Stopping:
    return to == RunState::Succeeded || to == RunState::Failed ||
           to == RunState::Cancelled;
  case RunState::Succeeded:
  case RunState::Failed:
  case RunState::Cancelled:
    return false;
  }
  return false;
}

[[nodiscard]] constexpr auto can_transition(TaskState from,
                                            TaskState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case TaskState::Pending:
    return to == TaskState::Ready || to == TaskState::Skipped ||
           to == TaskState::Cancelled;
  case TaskState::Ready:
    return to == TaskState::Running || to == TaskState::Skipped ||
           to == TaskState::Cancelled;
  case TaskState::Running:
    return to == TaskState::RetryWaiting || to == TaskState::Succeeded ||
           to == TaskState::Failed || to == TaskState::Cancelled;
  case TaskState::RetryWaiting:
    return to == TaskState::Ready || to == TaskState::Cancelled;
  case TaskState::Succeeded:
  case TaskState::Failed:
  case TaskState::Skipped:
  case TaskState::Cancelled:
    return false;
  }
  return false;
}

[[nodiscard]] constexpr auto can_transition(AttemptState from,
                                            AttemptState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case AttemptState::Starting:
    return to == AttemptState::Running || to == AttemptState::Failed ||
           to == AttemptState::Terminating || to == AttemptState::Cancelled;
  case AttemptState::Running:
    return to == AttemptState::Terminating ||
           to == AttemptState::Succeeded || to == AttemptState::Failed ||
           to == AttemptState::TimedOut || to == AttemptState::Cancelled;
  case AttemptState::Terminating:
    return to == AttemptState::TimedOut || to == AttemptState::Cancelled ||
           to == AttemptState::Failed;
  case AttemptState::Succeeded:
  case AttemptState::Failed:
  case AttemptState::TimedOut:
  case AttemptState::Cancelled:
    return false;
  }
  return false;
}

} // namespace dagforge::workflow
