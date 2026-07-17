module;

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <limits>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

export module dagforge.core;

export import dagforge.base;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/core/memory.hpp"
#include "dagforge/core/metrics.hpp"
#include "dagforge/core/constants.hpp"
#include "dagforge/core/arena.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
