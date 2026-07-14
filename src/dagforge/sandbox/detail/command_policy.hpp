#pragma once

#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/sandbox/command_spec.hpp"

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace dagforge::sandbox::detail {

class CommandPolicy {
public:
  [[nodiscard]] static auto create(config::CommandPolicyConfig config)
      -> Result<std::shared_ptr<const CommandPolicy>>;

  [[nodiscard]] auto canonical_program(std::string_view program) const
      -> Result<std::string>;
  [[nodiscard]] auto validate_environment_key(std::string_view key) const
      -> Result<void>;
  [[nodiscard]] auto validate_environment(std::string_view key,
                                          std::string_view value) const
      -> Result<void>;
  [[nodiscard]] auto validate(CommandSpec &command) const -> Result<void>;

  [[nodiscard]] auto inherited_environment() const noexcept
      -> const std::unordered_map<std::string, std::string> & {
    return inherited_environment_;
  }

  [[nodiscard]] auto config() const noexcept
      -> const config::CommandPolicyConfig & {
    return config_;
  }

private:
  CommandPolicy(
      config::CommandPolicyConfig config,
      std::unordered_map<std::string, std::string> programs,
      std::unordered_set<std::string> authorized_programs,
      std::unordered_set<std::string> allowed_environment,
      std::unordered_map<std::string, std::string> inherited_environment);

  config::CommandPolicyConfig config_;
  std::unordered_map<std::string, std::string> programs_;
  std::unordered_set<std::string> authorized_programs_;
  std::unordered_set<std::string> allowed_environment_;
  std::unordered_map<std::string, std::string> inherited_environment_;
};

} // namespace dagforge::sandbox::detail
