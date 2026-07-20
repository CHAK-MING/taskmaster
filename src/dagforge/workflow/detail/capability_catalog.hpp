#pragma once

#include "dagforge/config/admission_config.hpp"
#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/workflow_capabilities.hpp"

#include <optional>

namespace dagforge::workflow::detail {

[[nodiscard]] auto build_workflow_capabilities(
    const ExecutorRegistry &executors,
    const std::optional<config::AdmissionConfig> &admission)
    -> Result<WorkflowCapabilities>;

} // namespace dagforge::workflow::detail
