#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <memory>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

class PolicyEngine {
public:
  [[nodiscard]] auto validate(const WorkflowPlan &plan) const
      -> Result<void>;
};

class PlanCompiler {
public:
  explicit PlanCompiler(PolicyEngine policy_engine = {});

  [[nodiscard]] auto compile(WorkflowPlan plan) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;

  [[nodiscard]] static auto digest(const WorkflowPlan &plan)
      -> Result<std::string>;

private:
  PolicyEngine policy_engine_;
};

} // namespace dagforge::workflow
