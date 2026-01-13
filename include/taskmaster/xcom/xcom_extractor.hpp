#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/executor/executor.hpp"

#include <nlohmann/json.hpp>
#include <regex>
#include <string>
#include <string_view>
#include <vector>

namespace taskmaster {

struct ExtractedXCom {
  std::string key;
  nlohmann::json value;
};

class XComExtractor {
 public:
  [[nodiscard]] auto extract(const ExecutorResult& result,
                             const std::vector<XComPushConfig>& configs)
      -> Result<std::vector<ExtractedXCom>>;

 private:
  [[nodiscard]] auto extract_one(const ExecutorResult& result,
                                 const XComPushConfig& config)
      -> Result<ExtractedXCom>;

  [[nodiscard]] auto get_source_text(const ExecutorResult& result,
                                     XComSource source) -> std::string;

  [[nodiscard]] auto apply_regex(std::string_view text,
                                 const std::string& pattern,
                                 int group) -> Result<std::string>;

  [[nodiscard]] auto apply_json_path(const nlohmann::json& json,
                                     std::string_view path)
      -> Result<nlohmann::json>;
};

}  // namespace taskmaster
