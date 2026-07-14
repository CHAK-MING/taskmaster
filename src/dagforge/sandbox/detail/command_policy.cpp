#include "command_policy.hpp"

#include "command_validation.hpp"

#include <cstdlib>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace dagforge::sandbox::detail {

CommandPolicy::CommandPolicy(
    config::CommandPolicyConfig config,
    std::unordered_map<std::string, std::string> programs,
    std::unordered_set<std::string> authorized_programs,
    std::unordered_set<std::string> allowed_environment,
    std::unordered_map<std::string, std::string> inherited_environment)
    : config_(std::move(config)), programs_(std::move(programs)),
      authorized_programs_(std::move(authorized_programs)),
      allowed_environment_(std::move(allowed_environment)),
      inherited_environment_(std::move(inherited_environment)) {}

auto CommandPolicy::create(config::CommandPolicyConfig config)
    -> Result<std::shared_ptr<const CommandPolicy>> {
  std::unordered_map<std::string, std::string> programs;
  std::unordered_set<std::string> authorized_programs;
  for (const auto &configured : config.programs) {
    if (!detail::is_valid_program_name(configured.name)) {
      return fail(Error::InvalidArgument);
    }
    auto canonical = detail::canonical_program(configured.path,
                                               config.require_trusted_programs);
    if (!canonical || !programs.emplace(configured.name, *canonical).second) {
      return fail(canonical ? Error::InvalidArgument : canonical.error());
    }
    authorized_programs.emplace(std::move(*canonical));
  }

  for (const auto &configured : config.allowed_programs) {
    auto canonical =
        detail::canonical_program(configured, config.require_trusted_programs);
    if (!canonical) {
      return fail(canonical.error());
    }
    authorized_programs.emplace(std::move(*canonical));
  }

  std::unordered_set<std::string> allowed_environment;
  for (const auto &configured : config.allowed_environment) {
    if (!detail::is_valid_environment_key(configured) ||
        !allowed_environment.emplace(configured).second) {
      return fail(Error::InvalidArgument);
    }
  }

  std::unordered_map<std::string, std::string> inherited_environment;
  for (const auto &key : config.inherited_environment) {
    if (!detail::is_valid_environment_key(key) ||
        detail::is_reserved_environment_key(key) ||
        detail::is_sensitive_environment_key(key) ||
        inherited_environment.contains(key)) {
      return fail(Error::InvalidArgument);
    }
    if (const auto *value = std::getenv(key.c_str()); value != nullptr) {
      inherited_environment.emplace(key, value);
    }
  }

  return ok(std::shared_ptr<const CommandPolicy>{new CommandPolicy(
      std::move(config), std::move(programs), std::move(authorized_programs),
      std::move(allowed_environment), std::move(inherited_environment))});
}

auto CommandPolicy::canonical_program(std::string_view program) const
    -> Result<std::string> {
  if (program.empty() || program.contains('\0')) {
    return fail(Error::InvalidArgument);
  }
  if (!program.contains('/')) {
    const auto registered = programs_.find(std::string{program});
    if (registered == programs_.end()) {
      return fail(Error::Unauthorized);
    }
    return ok(registered->second);
  }
  auto canonical =
      detail::canonical_program(program, config_.require_trusted_programs);
  if (!canonical) {
    return fail(canonical.error());
  }
  if (!config_.allow_unlisted_programs &&
      !authorized_programs_.contains(*canonical)) {
    return fail(Error::Unauthorized);
  }
  return canonical;
}

auto CommandPolicy::validate_environment_key(std::string_view key) const
    -> Result<void> {
  if (!detail::is_valid_environment_key(key) ||
      detail::is_reserved_environment_key(key)) {
    return fail(Error::InvalidArgument);
  }
  if (!config_.allow_unlisted_environment &&
      !allowed_environment_.contains(std::string{key})) {
    return fail(Error::Unauthorized);
  }
  return ok();
}

auto CommandPolicy::validate_environment(std::string_view key,
                                         std::string_view value) const
    -> Result<void> {
  auto valid_key = validate_environment_key(key);
  if (!valid_key) {
    return valid_key;
  }
  return value.contains('\0') ? fail(Error::InvalidArgument) : ok();
}

auto CommandPolicy::validate(CommandSpec &command) const -> Result<void> {
  auto canonical = canonical_program(command.program);
  if (!canonical) {
    return fail(canonical.error());
  }
  command.program = std::move(*canonical);
  if (std::ranges::any_of(command.arguments, [](const auto &argument) {
        return argument.contains('\0');
      })) {
    return fail(Error::InvalidArgument);
  }
  for (const auto &[key, value] : command.environment) {
    auto valid = validate_environment(key, value);
    if (!valid) {
      return valid;
    }
  }
  return ok();
}

} // namespace dagforge::sandbox::detail
