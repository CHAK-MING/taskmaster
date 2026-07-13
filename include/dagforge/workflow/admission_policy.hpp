#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <utility>
#endif

namespace dagforge::workflow {

class AdmissionPolicy {
public:
  AdmissionPolicy() = default;
  explicit AdmissionPolicy(AdmissionConfig config)
      : config_(std::move(config)) {}

  [[nodiscard]] auto validate(const WorkflowPlan &plan) const -> Result<void>;

private:
  AdmissionConfig config_;
};

} // namespace dagforge::workflow
