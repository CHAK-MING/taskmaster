#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <string>
#include <string_view>
#endif

namespace dagforge::xcom {

[[nodiscard]] auto render_serialized_json(std::string_view json_text)
    -> Result<std::string>;

} // namespace dagforge::xcom
