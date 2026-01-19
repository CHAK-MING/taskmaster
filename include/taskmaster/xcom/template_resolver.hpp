#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <chrono>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace taskmaster {

struct TemplateContext {
  DAGRunId dag_run_id;
  std::chrono::system_clock::time_point execution_date{};
  std::chrono::system_clock::time_point data_interval_start{};
  std::chrono::system_clock::time_point data_interval_end{};
};

class TemplateResolver {
 public:
  explicit TemplateResolver(Persistence& persistence);

  [[nodiscard]] auto resolve_env_vars(const TemplateContext& ctx,
                                      const std::vector<XComPullConfig>& pulls)
      -> Result<std::unordered_map<std::string, std::string, StringHash, StringEqual>>;

  [[nodiscard]] auto resolve_template(std::string_view tmpl,
                                      const TemplateContext& ctx,
                                      const std::vector<XComPullConfig>& pulls)
      -> Result<std::string>;

 private:
  Persistence& persistence_;

  [[nodiscard]] auto stringify(const nlohmann::json& value) -> std::string;
  [[nodiscard]] auto resolve_date_variables(std::string_view tmpl,
                                            const TemplateContext& ctx)
      -> std::string;
};

}  // namespace taskmaster
