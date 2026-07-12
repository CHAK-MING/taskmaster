module;

#include <glaze/core/reflect.hpp>

#include <array>
#include <bitset>
#include <cctype>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <generator>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

export module dagforge.scheduler;

export import dagforge.base;
export import dagforge.util;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/scheduler/task_state.hpp"
#include "dagforge/scheduler/cron.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
