module;

#include <cerrno>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

export module dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/core/error.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
