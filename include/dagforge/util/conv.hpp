#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <charconv>
#include <concepts>
#include <string_view>
#endif

namespace dagforge::util {

template <std::integral T>
[[nodiscard]] inline auto parse_int(std::string_view s, int base = 10)
    -> Result<T> {
  T value{};
  auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), value, base);
  if (ec == std::errc{} && ptr == s.data() + s.size()) {
    return ok(value);
  }
  return fail(Error::ParseError);
}

} // namespace dagforge::util
