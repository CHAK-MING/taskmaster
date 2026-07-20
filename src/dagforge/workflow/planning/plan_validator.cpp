#include "dagforge/workflow/plan_validator.hpp"

#include <algorithm>
#include <chrono>
#include <format>
#include <ranges>
#include <string>
#include <string_view>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto contains(const std::vector<std::string> &values,
                            std::string_view value) -> bool {
  return std::ranges::find(values, value) != values.end();
}

[[nodiscard]] auto reject_plan(
    Error kind, std::string code, std::string description, std::string path,
    std::optional<WorkflowNodeId> node_id = std::nullopt,
    std::optional<std::string> executor = std::nullopt) -> PlanResult<void> {
  return plan_fail(make_plan_diagnostic(
      kind, std::move(code), std::move(description), std::move(path),
      std::move(node_id), std::move(executor)));
}

[[nodiscard]] auto node_path(std::size_t index, std::string_view suffix)
    -> std::string {
  return std::format("/nodes/{}{}", index, suffix);
}

[[nodiscard]] auto edge_path(std::size_t index, std::string_view suffix)
    -> std::string {
  return std::format("/edges/{}{}", index, suffix);
}

} // namespace

auto PlanValidator::validate_model(const WorkflowPlan &plan) const
    -> PlanResult<void> {
  if (plan.workflow_id.empty()) {
    return reject_plan(Error::InvalidArgument, "plan_workflow_id_required",
                       "Workflow Plan requires a workflow_id", "/workflow_id");
  }
  if (plan.policy.budget.max_run_duration <= std::chrono::seconds::zero()) {
    return reject_plan(
        Error::InvalidArgument, "plan_run_duration_invalid",
        "Workflow Plan max_run_duration_sec must be greater than zero",
        "/policy/budget/max_run_duration_sec");
  }

  switch (plan.policy.failure_policy) {
  case FailurePolicy::ContinueIndependent:
  case FailurePolicy::FailFast:
    break;
  default:
    return reject_plan(Error::InvalidArgument, "plan_failure_policy_invalid",
                       "Workflow Plan failure_policy is not supported",
                       "/policy/failure_policy");
  }

  for (std::size_t node_index = 0; node_index < plan.nodes.size();
       ++node_index) {
    const auto &node = plan.nodes[node_index];
    const auto node_id = node.node_id.empty()
                             ? std::optional<WorkflowNodeId>{}
                             : std::optional<WorkflowNodeId>{node.node_id};
    const auto executor = node.executor.empty()
                              ? std::optional<std::string>{}
                              : std::optional<std::string>{node.executor};
    if (node.node_id.empty()) {
      return reject_plan(Error::InvalidArgument, "plan_node_id_required",
                         "Workflow Plan node requires an id",
                         node_path(node_index, "/id"));
    }
    if (node.executor.empty()) {
      return reject_plan(Error::InvalidArgument, "plan_node_executor_required",
                         "Workflow Plan node requires an executor",
                         node_path(node_index, "/executor"), node_id);
    }
    if (!node.config.is_object()) {
      return reject_plan(Error::InvalidArgument, "plan_node_config_not_object",
                         "Workflow Plan node config must be a JSON object",
                         node_path(node_index, "/config"), node_id, executor);
    }
    if (node.timeout <= std::chrono::seconds::zero()) {
      return reject_plan(
          Error::InvalidArgument, "plan_node_timeout_invalid",
          "Workflow Plan node timeout_sec must be greater than zero",
          node_path(node_index, "/timeout_sec"), node_id, executor);
    }
    if (node.max_retries < 0) {
      return reject_plan(
          Error::InvalidArgument, "plan_node_max_retries_invalid",
          "Workflow Plan node max_retries cannot be negative",
          node_path(node_index, "/max_retries"), node_id, executor);
    }
    if (node.retry_initial_delay < std::chrono::milliseconds::zero()) {
      return reject_plan(
          Error::InvalidArgument, "plan_node_retry_initial_delay_invalid",
          "Workflow Plan node retry_initial_delay_ms cannot be negative",
          node_path(node_index, "/retry_initial_delay_ms"), node_id, executor);
    }
    if (node.retry_max_delay < node.retry_initial_delay) {
      return reject_plan(
          Error::InvalidArgument, "plan_node_retry_range_invalid",
          "Workflow Plan node retry_max_delay_ms cannot be less than "
          "retry_initial_delay_ms",
          node_path(node_index, "/retry_max_delay_ms"), node_id, executor);
    }

    for (std::size_t output_index = 0; output_index < node.outputs.size();
         ++output_index) {
      if (node.outputs[output_index].empty()) {
        return reject_plan(
            Error::InvalidArgument, "plan_node_output_required",
            "Workflow Plan node output port cannot be empty",
            node_path(node_index, std::format("/outputs/{}", output_index)),
            node_id, executor);
      }
    }
    for (std::size_t input_index = 0; input_index < node.inputs.size();
         ++input_index) {
      const auto &binding = node.inputs[input_index];
      const auto base = std::format("/inputs/{}", input_index);
      if (binding.input.empty()) {
        return reject_plan(
            Error::InvalidArgument, "plan_node_input_name_required",
            "Workflow Plan input binding requires an input name",
            node_path(node_index, base + "/input"), node_id, executor);
      }
      if (binding.source.node_id.empty()) {
        return reject_plan(
            Error::InvalidArgument, "plan_node_input_source_required",
            "Workflow Plan input binding requires a source node",
            node_path(node_index, base + "/node"), node_id, executor);
      }
      if (binding.source.port.empty()) {
        return reject_plan(
            Error::InvalidArgument, "plan_node_input_port_required",
            "Workflow Plan input binding requires a source port",
            node_path(node_index, base + "/port"), node_id, executor);
      }
    }
  }

  for (std::size_t edge_index = 0; edge_index < plan.edges.size();
       ++edge_index) {
    const auto &edge = plan.edges[edge_index];
    if (edge.source.node_id.empty()) {
      return reject_plan(Error::InvalidArgument,
                         "plan_edge_source_node_required",
                         "Workflow Plan edge requires a source node",
                         edge_path(edge_index, "/source_node"));
    }
    if (edge.source.port.empty()) {
      return reject_plan(Error::InvalidArgument,
                         "plan_edge_source_port_required",
                         "Workflow Plan edge requires a source port",
                         edge_path(edge_index, "/source_port"));
    }
    if (edge.target.empty()) {
      return reject_plan(Error::InvalidArgument, "plan_edge_target_required",
                         "Workflow Plan edge requires a target node",
                         edge_path(edge_index, "/target"));
    }
    switch (edge.condition.kind) {
    case ConditionKind::Always:
    case ConditionKind::BoolEquals:
    case ConditionKind::StringEquals:
      break;
    default:
      return reject_plan(Error::InvalidArgument, "plan_edge_condition_invalid",
                         "Workflow Plan edge condition kind is not supported",
                         edge_path(edge_index, "/condition/kind"));
    }
  }

  for (std::size_t output_index = 0; output_index < plan.outputs.size();
       ++output_index) {
    if (plan.outputs[output_index].node_id.empty()) {
      return reject_plan(Error::InvalidArgument,
                         "plan_published_output_node_required",
                         "Published output requires a node",
                         std::format("/outputs/{}/node", output_index));
    }
    if (plan.outputs[output_index].port.empty()) {
      return reject_plan(Error::InvalidArgument,
                         "plan_published_output_port_required",
                         "Published output requires a port",
                         std::format("/outputs/{}/port", output_index));
    }
  }

  return plan_ok();
}

auto PlanValidator::validate(const WorkflowPlan &plan) const
    -> PlanResult<void> {
  auto model = validate_model(plan);
  if (!model) {
    return model;
  }
  if (plan.schema_version != 1) {
    return reject_plan(
        Error::InvalidArgument, "plan_schema_version_unsupported",
        "Workflow Plan schema_version is not supported", "/schema_version");
  }
  if (plan.nodes.empty()) {
    return reject_plan(Error::InvalidArgument, "plan_nodes_required",
                       "Workflow Plan requires at least one node", "/nodes");
  }

  const auto &budget = plan.policy.budget;
  if (budget.max_nodes == 0) {
    return reject_plan(Error::ResourceExhausted,
                       "plan_budget_max_nodes_invalid",
                       "Workflow Plan max_nodes must be greater than zero",
                       "/policy/budget/max_nodes");
  }
  if (budget.max_parallel_nodes == 0) {
    return reject_plan(
        Error::ResourceExhausted, "plan_budget_parallelism_invalid",
        "Workflow Plan max_parallel_nodes must be greater than zero",
        "/policy/budget/max_parallel_nodes");
  }
  if (budget.max_total_output_bytes == 0) {
    return reject_plan(
        Error::ResourceExhausted, "plan_budget_output_bytes_invalid",
        "Workflow Plan max_total_output_bytes must be greater than zero",
        "/policy/budget/max_total_output_bytes");
  }
  if (plan.nodes.size() > budget.max_nodes) {
    return reject_plan(
        Error::ResourceExhausted, "plan_node_count_exceeds_budget",
        "Workflow Plan node count exceeds its max_nodes budget", "/nodes");
  }

  if (!admission_) {
    return plan_ok();
  }
  const auto &admission = *admission_;
  if (plan.nodes.size() > admission.max_nodes) {
    return reject_plan(
        Error::ResourceExhausted, "admission_node_count_exceeded",
        "Workflow Plan node count exceeds server admission policy", "/nodes");
  }
  if (budget.max_nodes > admission.max_nodes) {
    return reject_plan(
        Error::ResourceExhausted, "admission_max_nodes_exceeded",
        "Workflow Plan max_nodes exceeds server admission policy",
        "/policy/budget/max_nodes");
  }
  if (budget.max_parallel_nodes > admission.max_parallel_nodes) {
    return reject_plan(
        Error::ResourceExhausted, "admission_parallelism_exceeded",
        "Workflow Plan max_parallel_nodes exceeds server admission policy",
        "/policy/budget/max_parallel_nodes");
  }
  if (budget.max_total_output_bytes > admission.max_total_output_bytes) {
    return reject_plan(
        Error::ResourceExhausted, "admission_output_bytes_exceeded",
        "Workflow Plan max_total_output_bytes exceeds server admission policy",
        "/policy/budget/max_total_output_bytes");
  }
  if (budget.max_run_duration >
      std::chrono::seconds(admission.max_run_duration_sec)) {
    return reject_plan(
        Error::ResourceExhausted, "admission_run_duration_exceeded",
        "Workflow Plan max_run_duration_sec exceeds server admission policy",
        "/policy/budget/max_run_duration_sec");
  }
  for (std::size_t node_index = 0; node_index < plan.nodes.size();
       ++node_index) {
    const auto &node = plan.nodes[node_index];
    if (!admission.allow_unlisted_executors &&
        !contains(admission.allowed_executors, node.executor)) {
      return reject_plan(
          Error::Unauthorized, "admission_executor_not_allowed",
          "Workflow Plan executor is not allowed by server admission policy",
          node_path(node_index, "/executor"), node.node_id, node.executor);
    }
  }
  return plan_ok();
}

} // namespace dagforge::workflow
