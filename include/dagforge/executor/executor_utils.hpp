#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#endif

namespace dagforge {

/// Truncate a command string for log preview (max 80 chars).
[[nodiscard]] inline auto cmd_preview(std::string_view cmd) -> std::string {
  if (cmd.size() <= 80)
    return std::string(cmd);
  return std::string(cmd.substr(0, 80)) + "...";
}

/// Validate an environment variable key (POSIX: [A-Za-z_][A-Za-z0-9_]*).
[[nodiscard]] inline auto is_valid_env_key(std::string_view key) -> bool {
  if (key.empty())
    return false;
  if (!std::isalpha(static_cast<unsigned char>(key[0])) && key[0] != '_')
    return false;
  return std::ranges::all_of(key, [](char c) {
    const auto uc = static_cast<unsigned char>(c);
    return std::isalnum(uc) != 0 || c == '_';
  });
}

} // namespace dagforge
