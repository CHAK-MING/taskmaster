#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_validator.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <memory>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

class PlanCompiler {
public:
  explicit PlanCompiler(const ExecutorRegistry &executors,
                        PlanValidator validator = {});

  [[nodiscard]] auto compile(WorkflowPlan plan) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto compile(WorkflowPlan plan,
                             const WorkflowPlanId &plan_id) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;

  [[nodiscard]] static auto digest(const WorkflowPlan &plan)
      -> Result<std::string>;

private:
  const ExecutorRegistry *executors_;
  PlanValidator validator_;
};

} // namespace dagforge::workflow
