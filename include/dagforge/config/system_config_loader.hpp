#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/config/system_config.hpp"
#endif


namespace dagforge::config {

class SystemConfigLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<SystemConfig>;
  [[nodiscard]] static auto load_from_string(std::string_view toml_str)
      -> Result<SystemConfig>;
};

} // namespace dagforge::config
