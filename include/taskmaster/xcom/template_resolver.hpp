#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace taskmaster {

class TemplateResolver {
 public:
  explicit TemplateResolver(Persistence& persistence);

  [[nodiscard]] auto resolve_env_vars(DAGRunId dag_run_id,
                                      const std::vector<XComPullConfig>& pulls)
      -> Result<std::unordered_map<std::string, std::string>>;

  [[nodiscard]] auto resolve_template(std::string_view tmpl,
                                      DAGRunId dag_run_id,
                                      const std::vector<XComPullConfig>& pulls)
      -> Result<std::string>;

 private:
  Persistence& persistence_;

  [[nodiscard]] auto stringify(const nlohmann::json& value) -> std::string;
};

}  // namespace taskmaster
