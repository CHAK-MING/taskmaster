#include "taskmaster/xcom/template_resolver.hpp"

#include <format>
#include <regex>

namespace taskmaster {

TemplateResolver::TemplateResolver(Persistence& persistence)
    : persistence_(persistence) {}

auto TemplateResolver::resolve_env_vars(
    DAGRunId dag_run_id,
    const std::vector<XComPullConfig>& pulls)
    -> Result<std::unordered_map<std::string, std::string>> {
  std::unordered_map<std::string, std::string> env_vars;

  for (const auto& pull : pulls) {
    auto xcom_result = persistence_.get_xcom(dag_run_id, pull.source_task,
                                             pull.key);
    if (!xcom_result) {
      return std::unexpected(xcom_result.error());
    }

    env_vars[pull.env_var] = json_to_string(*xcom_result);
  }

  return env_vars;
}

auto TemplateResolver::resolve_template(
    std::string_view tmpl,
    DAGRunId dag_run_id,
    [[maybe_unused]] const std::vector<XComPullConfig>& pulls)
    -> Result<std::string> {
  std::string result(tmpl);

  static const std::regex xcom_pattern(R"(\{\{\s*xcom\.([^.]+)\.([^}\s]+)\s*\}\})");

  std::string::const_iterator search_start = result.cbegin();
  std::smatch match;
  std::string output;
  size_t last_pos = 0;

  while (std::regex_search(search_start, result.cend(), match, xcom_pattern)) {
    size_t match_pos = static_cast<size_t>(match.position()) +
                       static_cast<size_t>(search_start - result.cbegin());

    output.append(result, last_pos, match_pos - last_pos);

    TaskId task_id{match[1].str()};
    std::string key = match[2].str();

    auto xcom_result = persistence_.get_xcom(dag_run_id, task_id, key);
    if (!xcom_result) {
      return std::unexpected(xcom_result.error());
    }

    output.append(json_to_string(*xcom_result));

    last_pos = match_pos + match.length();
    search_start = result.cbegin() + static_cast<std::ptrdiff_t>(last_pos);
  }

  output.append(result, last_pos, result.size() - last_pos);

  return output;
}

auto TemplateResolver::json_to_string(const nlohmann::json& value)
    -> std::string {
  if (value.is_string()) {
    return value.get<std::string>();
  }
  if (value.is_number_integer()) {
    return std::to_string(value.get<int64_t>());
  }
  if (value.is_number_float()) {
    return std::to_string(value.get<double>());
  }
  if (value.is_boolean()) {
    return value.get<bool>() ? "true" : "false";
  }
  return value.dump();
}

}  // namespace taskmaster
