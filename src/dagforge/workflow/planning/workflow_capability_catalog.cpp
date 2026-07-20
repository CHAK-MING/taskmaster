#include "../detail/capability_catalog.hpp"

#include "dagforge/workflow/workflow_plan.hpp"

#include <ranges>
#include <string>
#include <utility>
#include <vector>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto contains(const std::vector<std::string> &values,
                            std::string_view value) -> bool {
  return std::ranges::find(values, value) != values.end();
}

} // namespace

auto build_workflow_capabilities(
    const ExecutorRegistry &executors,
    const std::optional<config::AdmissionConfig> &configured_admission)
    -> Result<WorkflowCapabilities> {
  auto plan_schema = json_schema_payload<WorkflowPlan>();
  if (!plan_schema) {
    return fail(plan_schema.error());
  }
  auto descriptions = executors.descriptions();
  if (!descriptions) {
    return fail(descriptions.error());
  }

  auto admission = configured_admission.value_or(config::AdmissionConfig{});
  if (!configured_admission) {
    admission.allow_unlisted_executors = true;
    admission.allowed_executors.clear();
  }

  std::vector<std::string> enabled_executors;
  std::vector<std::string> allowed_executors;
  enabled_executors.reserve(descriptions->size());
  allowed_executors.reserve(descriptions->size());
  for (const auto &description : *descriptions) {
    enabled_executors.push_back(description.type);
    if (admission.allow_unlisted_executors ||
        contains(admission.allowed_executors, description.type)) {
      allowed_executors.push_back(description.type);
    }
  }

  return ok(WorkflowCapabilities{
      .capability_schema_version = kCapabilitySchemaVersion,
      .workflow_schema_version = 1,
      .workflow_plan_schema = std::move(*plan_schema),
      .admission = std::move(admission),
      .enabled_executors = std::move(enabled_executors),
      .allowed_executors = std::move(allowed_executors),
      .executors = std::move(*descriptions),
  });
}

} // namespace dagforge::workflow::detail
