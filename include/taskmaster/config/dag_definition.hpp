#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"

#include <string>
#include <vector>

namespace taskmaster {

struct DAGDefinition {
  std::string name;
  std::string description;
  std::string cron;
  std::vector<TaskConfig> tasks;
  std::string source_file;
};

class DAGDefinitionLoader {
public:
  [[nodiscard]] static auto load_from_file(std::string_view path)
      -> Result<DAGDefinition>;
  [[nodiscard]] static auto load_from_string(std::string_view yaml_str)
      -> Result<DAGDefinition>;
  [[nodiscard]] static auto to_string(const DAGDefinition& dag) -> std::string;
};

}  // namespace taskmaster
