module;

#include <array>
#include <cstdint>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

export module dagforge.io;

export import dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/io/result.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
