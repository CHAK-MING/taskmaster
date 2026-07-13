#include "dagforge/workflow/admission_policy.hpp"

#include <algorithm>
#include <chrono>
#include <string_view>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto contains(const std::vector<std::string> &values,
                            std::string_view value) -> bool {
  return std::ranges::find(values, value) != values.end();
}

} // namespace

auto AdmissionPolicy::validate(const WorkflowPlan &plan) const -> Result<void> {
  const auto &budget = plan.policy.budget;
  if (plan.nodes.size() > config_.max_nodes ||
      budget.max_nodes > config_.max_nodes ||
      budget.max_parallel_nodes > config_.max_parallel_nodes ||
      budget.max_total_output_bytes > config_.max_total_output_bytes ||
      budget.max_run_duration >
          std::chrono::seconds(config_.max_run_duration_sec)) {
    return fail(Error::ResourceExhausted);
  }

  for (const auto &node : plan.nodes) {
    if (!config_.allow_unlisted_programs &&
        !contains(config_.allowed_programs, node.command.program)) {
      return fail(Error::Unauthorized);
    }
    for (const auto &entry : node.command.env) {
      if (!config_.allow_unlisted_environment &&
          !contains(config_.allowed_environment, entry.key)) {
        return fail(Error::Unauthorized);
      }
    }
    for (const auto &binding : node.command.input_env) {
      if (!config_.allow_unlisted_environment &&
          !contains(config_.allowed_environment, binding.environment)) {
        return fail(Error::Unauthorized);
      }
    }
  }
  return ok();
}

} // namespace dagforge::workflow
