#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/admission_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/plan_diagnostic.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <optional>
#include <utility>
#endif

namespace dagforge::workflow {

class PlanValidator {
public:
  PlanValidator() = default;
  explicit PlanValidator(config::AdmissionConfig admission)
      : admission_(std::move(admission)) {}

  // Validates invariants intrinsic to the model, independent of whether the
  // plan is currently admissible or executable by a particular registry.
  [[nodiscard]] auto validate_model(const WorkflowPlan &plan) const
      -> PlanResult<void>;
  [[nodiscard]] auto validate(const WorkflowPlan &plan) const
      -> PlanResult<void>;

  [[nodiscard]] auto admission() const noexcept
      -> const std::optional<config::AdmissionConfig> & {
    return admission_;
  }

private:
  std::optional<config::AdmissionConfig> admission_;
};

} // namespace dagforge::workflow
