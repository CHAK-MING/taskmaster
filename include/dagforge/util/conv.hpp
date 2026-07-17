#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/parse.hpp"

#include <concepts>
#include <string_view>
#endif

namespace dagforge::util {

template <std::integral T>
[[nodiscard]] inline auto parse_int(std::string_view s, int base = 10)
    -> Result<T> {
  auto value = parse_integer<T>(s, base);
  if (value) {
    return ok(*value);
  }
  return fail(Error::ParseError);
}

} // namespace dagforge::util
