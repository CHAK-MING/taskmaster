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

#define DAGFORGE_BUILDING_MODULE_INTERFACE 1
export {
#include "dagforge/config/admission_config.hpp"
#include "dagforge/config/api_config.hpp"
#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/config/http_executor_config.hpp"
#include "dagforge/config/runtime_config.hpp"
#include "dagforge/config/storage_config.hpp"
#include "dagforge/config/workflow_config.hpp"
#include "dagforge/config/system_config.hpp"
}
#undef DAGFORGE_BUILDING_MODULE_INTERFACE
