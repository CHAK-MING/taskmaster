#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"
#endif

namespace dagforge::workflow {

[[nodiscard]] auto make_default_workflow_adapters(WorkflowConfig config)
    -> WorkflowAdapters;

} // namespace dagforge::workflow
