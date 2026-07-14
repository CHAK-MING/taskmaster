#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/workflow/executor_registry.hpp"

#include <memory>
#endif

namespace dagforge {
class ICommandExecutor;
}

namespace dagforge::workflow {

[[nodiscard]] auto create_command_executor_adapter(
    ICommandExecutor &command_executor, SandboxConfig sandbox)
    -> std::shared_ptr<ITaskExecutor>;

} // namespace dagforge::workflow
