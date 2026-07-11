module;

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

export module dagforge.metrics;

export import dagforge.core;
export import dagforge.dag;
export import dagforge.http;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/app/metrics_registry.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
