module;

#include <glaze/core/reflect.hpp>

#include <chrono>
#include <cstdint>
#include <flat_map>
#include <functional>
#include <memory>
#include <new>
#include <string>
#include <vector>

export module dagforge.executor;

export import dagforge.util;

export {
#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
#include "dagforge/executor/executor_types.hpp"
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
}
