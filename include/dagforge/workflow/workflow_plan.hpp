#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/workflow/workflow_value.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#endif

namespace dagforge::workflow {

enum class FailurePolicy : std::uint8_t {
  ContinueIndependent,
  FailFast,
};

enum class ConditionKind : std::uint8_t {
  Always,
  BoolEquals,
  StringEquals,
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
  std::string executor;
  JsonValue config{JsonValue::object_t{}};
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

} // namespace dagforge::workflow
