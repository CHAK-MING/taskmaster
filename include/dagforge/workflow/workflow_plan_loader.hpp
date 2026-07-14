#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <string>
#include <string_view>
#endif

namespace dagforge::workflow {

class WorkflowPlanLoader {
public:
  [[nodiscard]] static auto from_json(std::string_view text)
      -> Result<WorkflowPlan>;
  [[nodiscard]] static auto to_json(const WorkflowPlan &plan)
      -> Result<std::string>;
};

} // namespace dagforge::workflow
