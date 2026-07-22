#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_diagnostic.hpp"
#include "dagforge/workflow/plan_validator.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace dagforge::workflow {

struct PlanValidation {
  WorkflowId workflow_id;
  std::string digest;
  std::size_t nodes{0};
};

class PlanCompiler {
public:
  explicit PlanCompiler(const ExecutorRegistry &executors,
                        PlanValidator validator = {});

  [[nodiscard]] auto compile(WorkflowPlan plan) const
      -> PlanResult<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto validate(WorkflowPlan plan) const
      -> PlanResult<PlanValidation>;
  [[nodiscard]] auto compile(WorkflowPlan plan,
                             const WorkflowPlanId &plan_id) const
      -> PlanResult<std::shared_ptr<const ExecutionPlan>>;

  [[nodiscard]] static auto digest(const WorkflowPlan &plan)
      -> Result<std::string>;

private:
  [[nodiscard]] auto compile_with_id(WorkflowPlan plan,
                                     WorkflowPlanId plan_id) const
      -> PlanResult<std::shared_ptr<const ExecutionPlan>>;

  const ExecutorRegistry *executors_;
  PlanValidator validator_;
};

} // namespace dagforge::workflow
