#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <glaze/json.hpp>

#include <array>
#include <cstddef>
#endif

namespace glz {

template <> struct meta<dagforge::Error> {
  static constexpr auto keys = dagforge::kErrorNames;
  static constexpr auto value = [] {
    std::array<dagforge::Error, dagforge::kErrorNames.size()> values{};
    for (std::size_t index = 0; index < values.size(); ++index) {
      values[index] = static_cast<dagforge::Error>(index);
    }
    return values;
  }();
};

} // namespace glz
