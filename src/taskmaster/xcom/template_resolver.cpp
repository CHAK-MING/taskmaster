#include "taskmaster/xcom/template_resolver.hpp"

#include <algorithm>
#include <format>
#include <ranges>
#include <regex>

namespace taskmaster {

TemplateResolver::TemplateResolver(Persistence& persistence)
    : persistence_(persistence) {}

auto TemplateResolver::resolve_env_vars(
    const TemplateContext& ctx,
    const std::vector<XComPullConfig>& pulls)
    -> Result<std::unordered_map<std::string, std::string>> {
  std::unordered_map<std::string, std::string> env_vars;

  for (const auto& pull : pulls) {
    auto xcom_result = persistence_.get_xcom(ctx.dag_run_id, pull.source_task,
                                             pull.key);
    if (!xcom_result) {
      return std::unexpected(xcom_result.error());
    }

    env_vars[pull.env_var] = stringify(*xcom_result);
  }

  return env_vars;
}

auto TemplateResolver::resolve_template(
    std::string_view tmpl,
    const TemplateContext& ctx,
    [[maybe_unused]] const std::vector<XComPullConfig>& pulls)
    -> Result<std::string> {
  
  auto result = resolve_date_variables(tmpl, ctx);

  static const std::regex xcom_pattern(R"(\{\{\s*xcom\.([^.]+)\.([^\}\s]+)\s*\}\})");

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

    auto xcom_result = persistence_.get_xcom(ctx.dag_run_id, task_id, key);
    if (!xcom_result) {
      return std::unexpected(xcom_result.error());
    }

    output.append(stringify(*xcom_result));

    last_pos = match_pos + match.length();
    search_start = result.cbegin() + static_cast<std::ptrdiff_t>(last_pos);
  }

  output.append(result, last_pos, result.size() - last_pos);

  return output;
}

auto TemplateResolver::resolve_date_variables(std::string_view tmpl,
                                              const TemplateContext& ctx)
    -> std::string {
  std::string result(tmpl);
  
  if (ctx.execution_date == std::chrono::system_clock::time_point{}) {
    return result;
  }

  auto const time_t_val = std::chrono::system_clock::to_time_t(ctx.execution_date);
  std::tm tm_val{};
  gmtime_r(&time_t_val, &tm_val);

  auto const ds = std::format("{:04d}-{:02d}-{:02d}", 
      tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
  auto const ds_nodash = std::format("{:04d}{:02d}{:02d}",
      tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday);
  auto const ts = std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}",
      tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday,
      tm_val.tm_hour, tm_val.tm_min, tm_val.tm_sec);
  auto const ts_nodash = std::format("{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}",
      tm_val.tm_year + 1900, tm_val.tm_mon + 1, tm_val.tm_mday,
      tm_val.tm_hour, tm_val.tm_min, tm_val.tm_sec);

  static const std::array<std::pair<std::string_view, std::string_view>, 4> 
      patterns_order{{
        {"{{ts_nodash}}", {}},
        {"{{ds_nodash}}", {}},
        {"{{ts}}", {}},
        {"{{ds}}", {}},
      }};

  auto replace_all = [](std::string& str, std::string_view from, 
                        std::string_view to) {
    size_t pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos) {
      str.replace(pos, from.length(), to);
      pos += to.length();
    }
  };

  replace_all(result, "{{ts_nodash}}", ts_nodash);
  replace_all(result, "{{ds_nodash}}", ds_nodash);
  replace_all(result, "{{ts}}", ts);
  replace_all(result, "{{ds}}", ds);

  return result;
}

auto TemplateResolver::stringify(const nlohmann::json& value)
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
