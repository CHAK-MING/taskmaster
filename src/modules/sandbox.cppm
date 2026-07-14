module;

#include <glaze/core/reflect.hpp>

#include <cstdint>
#include <flat_map>
#include <string>
#include <vector>

export module dagforge.sandbox;

export import dagforge.util;

export {
#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
#include "dagforge/sandbox/command_spec.hpp"
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
}
