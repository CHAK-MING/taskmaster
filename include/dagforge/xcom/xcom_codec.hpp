#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_types.hpp"
#include "dagforge/core/error.hpp"

#include <string>
#include <string_view>
#include <vector>
#endif

namespace dagforge::xcom {

[[nodiscard]] auto serialize_push_configs(
    const std::vector<XComPushConfig> &pushes) -> std::string;

[[nodiscard]] auto parse_push_configs(std::string_view input)
    -> Result<std::vector<XComPushConfig>>;

[[nodiscard]] auto serialize_pull_configs(
    const std::vector<XComPullConfig> &pulls) -> std::string;

[[nodiscard]] auto parse_pull_configs(std::string_view input)
    -> Result<std::vector<XComPullConfig>>;

} // namespace dagforge::xcom
