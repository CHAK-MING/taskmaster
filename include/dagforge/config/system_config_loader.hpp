#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"

#include <string_view>
#endif

namespace dagforge::config {

class SystemConfigLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<SystemConfig>;
  [[nodiscard]] static auto load_from_string(std::string_view json)
      -> Result<SystemConfig>;
};

} // namespace dagforge::config
