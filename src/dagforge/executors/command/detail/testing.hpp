#pragma once

#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/sandbox/command_runner.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include <memory>

namespace dagforge::executors::command::detail {

[[nodiscard]] auto
create_task_executor(std::unique_ptr<sandbox::ICommandRunner> runner,
                     const config::CommandPolicyConfig &policy)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>>;

} // namespace dagforge::executors::command::detail
