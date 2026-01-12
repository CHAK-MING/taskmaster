#pragma once

#include "taskmaster/config/system_config.hpp"
#include "taskmaster/core/error.hpp"

namespace taskmaster {

using Config = SystemConfig;

class ConfigLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<SystemConfig>;
  [[nodiscard]] static auto load_from_string(std::string_view yaml_str)
      -> Result<SystemConfig>;
};

}  // namespace taskmaster
