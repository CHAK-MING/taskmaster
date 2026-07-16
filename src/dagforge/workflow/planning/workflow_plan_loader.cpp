#include "dagforge/workflow/workflow_plan_loader.hpp"

#include "dagforge/util/json.hpp"

namespace dagforge::workflow {

auto WorkflowPlanLoader::from_json(std::string_view text)
    -> Result<WorkflowPlan> {
  return parse_json_as<WorkflowPlan>(text);
}

auto WorkflowPlanLoader::to_json(const WorkflowPlan &plan)
    -> Result<std::string> {
  return serialize_json(plan);
}

} // namespace dagforge::workflow
