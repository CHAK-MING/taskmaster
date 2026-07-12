#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <ankerl/unordered_dense.h>

#include <cstddef>
#include <string_view>
#endif

namespace dagforge::util {

[[nodiscard]] inline auto hash_value(std::string_view value) noexcept
    -> std::size_t {
  return static_cast<std::size_t>(
      ankerl::unordered_dense::hash<std::string_view>{}(value));
}

template <typename T>
[[nodiscard]] inline auto hash_value(const T &value) noexcept -> std::size_t {
  return static_cast<std::size_t>(ankerl::unordered_dense::hash<T>{}(value));
}

template <typename T>
[[nodiscard]] inline auto shard_of(const T &value,
                                   unsigned shard_count) noexcept -> unsigned {
  return static_cast<unsigned>(hash_value(value) % shard_count);
}

} // namespace dagforge::util
