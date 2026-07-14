#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <algorithm>
#include <cctype>
#include <string_view>
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

} // namespace dagforge::executor_detail
