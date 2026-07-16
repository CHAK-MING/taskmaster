#pragma once

#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/sandbox/command_runner.hpp"

namespace dagforge::sandbox::detail {

[[nodiscard]] auto create_minijail_command_runner(
    Runtime &runtime, config::MinijailConfig sandbox,
    config::CommandPolicyConfig policy)
    -> Result<std::unique_ptr<ICommandRunner>>;

} // namespace dagforge::sandbox::detail
