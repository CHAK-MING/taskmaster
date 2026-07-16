#pragma once

#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/sandbox/command_runner.hpp"

#include <memory>

namespace dagforge::sandbox::detail {

[[nodiscard]] auto create_policy_command_runner(
    std::unique_ptr<ICommandRunner> inner,
    config::CommandPolicyConfig policy_config)
    -> Result<std::unique_ptr<ICommandRunner>>;

} // namespace dagforge::sandbox::detail
