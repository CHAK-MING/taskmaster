#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/admission_config.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/executor_description.hpp"

#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

inline constexpr std::uint32_t kCapabilitySchemaVersion = 1;

struct WorkflowCapabilities {
  std::uint32_t capability_schema_version{kCapabilitySchemaVersion};
  std::uint32_t workflow_schema_version{1};
  JsonPayload workflow_plan_schema;
  config::AdmissionConfig admission;
  std::vector<std::string> enabled_executors;
  std::vector<std::string> allowed_executors;
  std::vector<ExecutorDescription> executors;
};

} // namespace dagforge::workflow
