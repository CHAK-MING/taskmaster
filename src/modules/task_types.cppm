module;

#include <boost/describe/enum.hpp>

#include <memory>
#include <ranges>
#include <regex>
#include <string>
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
