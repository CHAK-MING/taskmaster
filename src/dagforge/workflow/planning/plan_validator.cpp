#include "dagforge/workflow/plan_validator.hpp"

#include <algorithm>
#include <chrono>
#include <ranges>
#include <string_view>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto contains(const std::vector<std::string> &values,
                            std::string_view value) -> bool {
  return std::ranges::find(values, value) != values.end();
}

} // namespace

auto PlanValidator::validate_model(const WorkflowPlan &plan) const
    -> Result<void> {
  if (plan.workflow_id.empty() ||
      plan.policy.budget.max_run_duration <= std::chrono::seconds::zero()) {
    return fail(Error::InvalidArgument);
  }

  switch (plan.policy.failure_policy) {
  case FailurePolicy::ContinueIndependent:
  case FailurePolicy::FailFast:
    break;
  default:
    return fail(Error::InvalidArgument);
  }

  for (const auto &node : plan.nodes) {
    if (node.node_id.empty() || node.executor.empty() ||
        !node.config.is_object() ||
        node.timeout <= std::chrono::seconds::zero() || node.max_retries < 0 ||
        node.retry_initial_delay < std::chrono::milliseconds::zero() ||
        node.retry_max_delay < node.retry_initial_delay) {
      return fail(Error::InvalidArgument);
    }
    if (std::ranges::any_of(node.outputs,
                            [](const auto &port) { return port.empty(); }) ||
        std::ranges::any_of(node.inputs, [](const auto &binding) {
          return binding.input.empty() || binding.source.node_id.empty() ||
                 binding.source.port.empty();
        })) {
      return fail(Error::InvalidArgument);
    }
  }

  for (const auto &edge : plan.edges) {
    if (edge.source.node_id.empty() || edge.source.port.empty() ||
        edge.target.empty()) {
      return fail(Error::InvalidArgument);
    }
    switch (edge.condition.kind) {
    case ConditionKind::Always:
    case ConditionKind::BoolEquals:
    case ConditionKind::StringEquals:
      break;
    default:
      return fail(Error::InvalidArgument);
    }
  }

  if (std::ranges::any_of(plan.outputs, [](const auto &output) {
        return output.node_id.empty() || output.port.empty();
      })) {
    return fail(Error::InvalidArgument);
  }

  return ok();
}

auto PlanValidator::validate(const WorkflowPlan &plan) const -> Result<void> {
  auto model = validate_model(plan);
  if (!model) {
    return model;
  }
  if (plan.schema_version != 1 || plan.nodes.empty()) {
    return fail(Error::InvalidArgument);
  }

  const auto &budget = plan.policy.budget;
  if (budget.max_nodes == 0 || budget.max_parallel_nodes == 0 ||
      budget.max_total_output_bytes == 0 ||
      plan.nodes.size() > budget.max_nodes) {
    return fail(Error::ResourceExhausted);
  }

  if (!admission_) {
    return ok();
  }
  const auto &admission = *admission_;
  if (plan.nodes.size() > admission.max_nodes ||
      budget.max_nodes > admission.max_nodes ||
      budget.max_parallel_nodes > admission.max_parallel_nodes ||
      budget.max_total_output_bytes > admission.max_total_output_bytes ||
      budget.max_run_duration >
          std::chrono::seconds(admission.max_run_duration_sec)) {
    return fail(Error::ResourceExhausted);
  }
  for (const auto &node : plan.nodes) {
    if (!admission.allow_unlisted_executors &&
        !contains(admission.allowed_executors, node.executor)) {
      return fail(Error::Unauthorized);
    }
  }
  return ok();
}

} // namespace dagforge::workflow
