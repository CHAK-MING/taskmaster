module;

#include <ankerl/unordered_dense.h>
#include <glaze/core/reflect.hpp>

#include <memory>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

export module dagforge.task_types;

export import dagforge.base;
export import dagforge.domain;
export import dagforge.util;

export {
#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
#include "dagforge/config/task_types.hpp"
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
}
