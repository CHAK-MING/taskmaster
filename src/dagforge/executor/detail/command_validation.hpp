#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

#include <sys/stat.h>
#include <unistd.h>
#endif

namespace dagforge::executor_detail {

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

[[nodiscard]] inline auto trusted_regular_file(
    const std::filesystem::path &path, bool require_executable,
    bool require_trusted_permissions) -> Result<std::filesystem::path> {
  std::error_code error;
  auto canonical = std::filesystem::canonical(path, error);
  if (error || !std::filesystem::is_regular_file(canonical, error)) {
    return fail(Error::NotFound);
  }
  struct stat metadata {};
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

[[nodiscard]] inline auto canonical_program(
    std::string_view configured, bool require_trusted_permissions)
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

} // namespace dagforge::executor_detail
