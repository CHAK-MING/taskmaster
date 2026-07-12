module;

#include <glaze/core/reflect.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

export module dagforge.config;

export import dagforge.base;
export import dagforge.domain;
export import dagforge.util;

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/config/system_config.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
