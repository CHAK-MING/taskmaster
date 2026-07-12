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

enum class NodeType : std::uint8_t {
  Shell,
  Docker,
  Lua,
  Http,
  Model,
  Tool,
  Compute,
  Evaluator,
  Approval,
  Noop,
};

enum class RunState : std::uint8_t {
  Queued,
  Running,
  AwaitingApproval,
  Success,
  Failed,
  Cancelled,
};

enum class NodeState : std::uint8_t {
  Pending,
  Ready,
  Running,
  AwaitingApproval,
  Success,
  Failed,
  Skipped,
  Cancelled,
};

enum class ConditionKind : std::uint8_t {
  Always,
  BoolEquals,
  StringEquals,
  EvaluationPassed,
};

enum class EvidenceType : std::uint8_t {
  TriggerReceived,
  PlanCompiled,
  PolicyAccepted,
  PolicyRejected,
  NodeStarted,
  NodeCompleted,
  NodeFailed,
  ModelRequest,
  ModelResponse,
  ToolRequest,
  ToolResponse,
  Evaluation,
  ApprovalRequested,
  ApprovalResolved,
  Checkpoint,
  RunCompleted,
  RunFailed,
};

struct Principal {
  std::string subject;
  std::vector<std::string> roles;
};

struct TraceContext {
  std::string trace_id;
  std::string parent_span_id;
};

struct CredentialRef {
  std::string name;
};

struct ArtifactRef {
  ArtifactId artifact_id;
  std::string media_type{"application/octet-stream"};
  std::uint64_t size_bytes{0};
  std::string digest;
};

struct Message {
  std::string role;
  std::string content;
};

using MessageList = std::vector<Message>;

struct ToolCall {
  std::string name;
  JsonValue arguments;
};

struct ToolResult {
  std::string name;
  bool success{false};
  JsonValue output;
  std::string error;
};

struct ModelUsage {
  std::uint64_t input_tokens{0};
  std::uint64_t output_tokens{0};
};

struct ModelResponse {
  Message message;
  std::vector<ToolCall> tool_calls;
  std::optional<JsonValue> structured_output;
  ModelUsage usage;
  std::string provider_request_id;
};

struct EvaluationResult {
  bool passed{false};
  double score{0.0};
  std::string reason;
  JsonValue evidence;
};

using WorkflowValue =
    std::variant<std::monostate, bool, std::int64_t, double, std::string,
                 JsonValue, MessageList, ToolResult, ArtifactRef, ModelResponse,
                 EvaluationResult>;

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
  std::uint64_t max_model_tokens{1'000'000};
  std::chrono::milliseconds max_run_duration{std::chrono::hours(1)};
};

struct WorkflowPolicy {
  bool allow_shell{false};
  bool allow_docker{true};
  bool allow_lua{true};
  bool allow_network{true};
  bool allow_model_calls{true};
  bool allow_tools{true};
  bool require_approval_for_shell{true};
  std::vector<std::string> allowed_http_hosts;
  std::vector<std::string> allowed_model_providers;
  std::vector<std::string> allowed_tools;
  ResourceBudget budget;
};

struct NodePlan {
  WorkflowNodeId node_id;
  std::string name;
  NodeType type{NodeType::Noop};
  JsonValue config;
  std::vector<InputBinding> inputs;
  std::vector<WorkflowPortId> outputs;
  int max_retries{0};
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

struct NodeSnapshot {
  WorkflowNodeId node_id;
  NodeState state{NodeState::Pending};
  int attempt{0};
  std::string error;
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
};

struct RunSnapshot {
  WorkflowRunId run_id;
  WorkflowId workflow_id;
  WorkflowPlanId plan_id;
  RunState state{RunState::Queued};
  std::vector<NodeSnapshot> nodes;
  std::vector<ApprovalId> pending_approvals;
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  std::string error;
};

[[nodiscard]] constexpr auto to_string_view(NodeType value) noexcept
    -> std::string_view {
  switch (value) {
  case NodeType::Shell:
    return "shell";
  case NodeType::Docker:
    return "docker";
  case NodeType::Lua:
    return "lua";
  case NodeType::Http:
    return "http";
  case NodeType::Model:
    return "model";
  case NodeType::Tool:
    return "tool";
  case NodeType::Compute:
    return "compute";
  case NodeType::Evaluator:
    return "evaluator";
  case NodeType::Approval:
    return "approval";
  case NodeType::Noop:
    return "noop";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(RunState value) noexcept
    -> std::string_view {
  switch (value) {
  case RunState::Queued:
    return "queued";
  case RunState::Running:
    return "running";
  case RunState::AwaitingApproval:
    return "awaiting_approval";
  case RunState::Success:
    return "success";
  case RunState::Failed:
    return "failed";
  case RunState::Cancelled:
    return "cancelled";
  }
  return "unknown";
}

[[nodiscard]] constexpr auto to_string_view(NodeState value) noexcept
    -> std::string_view {
  switch (value) {
  case NodeState::Pending:
    return "pending";
  case NodeState::Ready:
    return "ready";
  case NodeState::Running:
    return "running";
  case NodeState::AwaitingApproval:
    return "awaiting_approval";
  case NodeState::Success:
    return "success";
  case NodeState::Failed:
    return "failed";
  case NodeState::Skipped:
    return "skipped";
  case NodeState::Cancelled:
    return "cancelled";
  }
  return "unknown";
}

} // namespace dagforge::workflow
