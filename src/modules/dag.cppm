module;

#include <glaze/core/reflect.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

export module dagforge.dag;

export import dagforge.base;
export import dagforge.domain;
export import dagforge.scheduler;
export import dagforge.util;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/dag/graph_types.hpp"
#include "dagforge/dag/run_types.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
