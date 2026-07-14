#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cerrno>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

#include <sys/stat.h>
#include <unistd.h>
#endif

namespace dagforge::sandbox::detail {

inline constexpr std::array<std::string_view, 3> kReservedEnvironment{
    "HOME", "PATH", "TMPDIR"};

[[nodiscard]] inline auto is_valid_environment_key(std::string_view key)
    -> bool {
  if (key.empty())
    return false;
  if (!std::isalpha(static_cast<unsigned char>(key[0])) && key[0] != '_')
    return false;
  return std::ranges::all_of(key, [](char c) {
    const auto uc = static_cast<unsigned char>(c);
    return std::isalnum(uc) != 0 || c == '_';
  });
}

[[nodiscard]] inline auto is_valid_program_name(std::string_view name) -> bool {
  if (name.empty() || name.contains('/') || name.contains('\\') ||
      name.contains('\0')) {
    return false;
  }
  return std::ranges::all_of(name, [](char c) {
    const auto value = static_cast<unsigned char>(c);
    return std::isalnum(value) != 0 || c == '_' || c == '-' || c == '.' ||
           c == '+';
  });
}

[[nodiscard]] inline auto is_reserved_environment_key(std::string_view key)
    -> bool {
  return std::ranges::find(kReservedEnvironment, key) !=
         kReservedEnvironment.end();
}

[[nodiscard]] inline auto is_sensitive_environment_key(std::string_view key)
    -> bool {
  std::string upper;
  upper.reserve(key.size());
  std::ranges::transform(key, std::back_inserter(upper), [](char value) {
    return static_cast<char>(std::toupper(static_cast<unsigned char>(value)));
  });

  static constexpr std::array<std::string_view, 5> kSensitiveWords{
      "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "PRIVATE_KEY"};
  if (std::ranges::any_of(kSensitiveWords, [&](std::string_view word) {
        return upper.contains(word);
      })) {
    return true;
  }
  return upper == "KEY" || upper.starts_with("KEY_") ||
         upper.ends_with("_KEY") || upper.contains("_KEY_") ||
         upper == "SSH_AUTH_SOCK";
}

[[nodiscard]] inline auto
trusted_regular_file(const std::filesystem::path &path, bool require_executable,
                     bool require_trusted_permissions)
    -> Result<std::filesystem::path> {
  std::error_code error;
  auto canonical = std::filesystem::canonical(path, error);
  if (error || !std::filesystem::is_regular_file(canonical, error)) {
    return fail(Error::NotFound);
  }
  struct stat metadata{};
  if (::stat(canonical.c_str(), &metadata) != 0) {
    return fail(std::error_code(errno, std::generic_category()));
  }
  if (require_executable && ::access(canonical.c_str(), X_OK) != 0) {
    return fail(Error::Unauthorized);
  }
  if (require_trusted_permissions) {
    if ((metadata.st_mode & (S_IWGRP | S_IWOTH)) != 0 ||
        (metadata.st_uid != 0 && metadata.st_uid != ::geteuid())) {
      return fail(Error::Unauthorized);
    }
  }
  return ok(std::move(canonical));
}

[[nodiscard]] inline auto canonical_program(std::string_view configured,
                                            bool require_trusted_permissions)
    -> Result<std::string> {
  if (configured.empty() || configured.contains('\0') ||
      !std::filesystem::path(configured).is_absolute()) {
    return fail(Error::InvalidArgument);
  }
  auto resolved = trusted_regular_file(std::filesystem::path(configured), true,
                                       require_trusted_permissions);
  if (!resolved) {
    return fail(resolved.error());
  }
  return ok(resolved->string());
}

} // namespace dagforge::sandbox::detail
