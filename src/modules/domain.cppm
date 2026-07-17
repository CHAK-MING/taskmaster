module;

#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

export module dagforge.domain;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
// id.hpp declares domain aliases over the generic typed-string primitive.
// clang-format off
#include "dagforge/util/typed_id.hpp"
#include "dagforge/util/id.hpp"
// clang-format on
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
