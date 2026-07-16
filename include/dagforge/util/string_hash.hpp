#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/hash.hpp"

#include <functional>
#include <string>
#include <string_view>
#endif

namespace dagforge {

struct StringHash {
  using is_transparent = void;

  [[nodiscard]] auto operator()(std::string_view value) const noexcept
      -> std::size_t {
    return util::hash_value(value);
  }

  [[nodiscard]] auto operator()(const std::string &value) const noexcept
      -> std::size_t {
    return (*this)(std::string_view{value});
  }

  [[nodiscard]] auto operator()(const char *value) const noexcept
      -> std::size_t {
    return (*this)(std::string_view{value});
  }
};

using StringEqual = std::equal_to<>;

} // namespace dagforge
