#include "taskmaster/xcom/template_resolver.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <format>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace taskmaster {

TemplateResolver::TemplateResolver(Persistence& persistence)
    : persistence_(persistence) {}

auto TemplateResolver::resolve_env_vars(
    const TemplateContext& ctx,
    const std::vector<XComPullConfig>& pulls)
    -> Result<std::unordered_map<std::string, std::string, StringHash, StringEqual>> {
  std::unordered_map<std::string, std::string, StringHash, StringEqual> env_vars;

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

namespace {

// Pre-computed date format strings for template resolution
struct DateFormats {
  std::string ds;                    // YYYY-MM-DD
  std::string ds_nodash;             // YYYYMMDD
  std::string ts;                    // YYYY-MM-DDTHH:MM:SS
  std::string ts_nodash;             // YYYYMMDDTHHMMSS
  std::string data_interval_start;   // ISO 8601
  std::string data_interval_end;     // ISO 8601
  bool valid{false};
};

auto compute_date_formats(std::chrono::system_clock::time_point execution_date)
    -> DateFormats {
  if (execution_date == std::chrono::system_clock::time_point{}) {
    return {};
  }

  auto const time_t_val = std::chrono::system_clock::to_time_t(execution_date);
  std::tm tm_val{};
  gmtime_r(&time_t_val, &tm_val);

  return DateFormats{
      .ds = std::format("{:04d}-{:02d}-{:02d}", tm_val.tm_year + 1900,
                        tm_val.tm_mon + 1, tm_val.tm_mday),
      .ds_nodash = std::format("{:04d}{:02d}{:02d}", tm_val.tm_year + 1900,
                               tm_val.tm_mon + 1, tm_val.tm_mday),
      .ts = std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}",
                        tm_val.tm_year + 1900, tm_val.tm_mon + 1,
                        tm_val.tm_mday, tm_val.tm_hour, tm_val.tm_min,
                        tm_val.tm_sec),
      .ts_nodash = std::format("{:04d}{:02d}{:02d}T{:02d}{:02d}{:02d}",
                               tm_val.tm_year + 1900, tm_val.tm_mon + 1,
                               tm_val.tm_mday, tm_val.tm_hour, tm_val.tm_min,
                               tm_val.tm_sec),
      .data_interval_start = {},
      .data_interval_end = {},
      .valid = true,
  };
}

auto format_time_point(std::chrono::system_clock::time_point tp) -> std::string {
  if (tp == std::chrono::system_clock::time_point{}) {
    return {};
  }
  auto const time_t_val = std::chrono::system_clock::to_time_t(tp);
  std::tm tm_val{};
  gmtime_r(&time_t_val, &tm_val);
  return std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}Z",
                     tm_val.tm_year + 1900, tm_val.tm_mon + 1,
                     tm_val.tm_mday, tm_val.tm_hour, tm_val.tm_min,
                     tm_val.tm_sec);
}

// Trim leading/trailing whitespace from a string_view
auto trim(std::string_view sv) -> std::string_view {
  while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.front()))) {
    sv.remove_prefix(1);
  }
  while (!sv.empty() && std::isspace(static_cast<unsigned char>(sv.back()))) {
    sv.remove_suffix(1);
  }
  return sv;
}

// Parse XCom token: "xcom.task_id.key" -> (task_id, key)
// Returns nullopt if not a valid xcom token
auto parse_xcom_token(std::string_view token)
    -> std::optional<std::pair<std::string_view, std::string_view>> {
  constexpr std::string_view prefix = "xcom.";
  if (!token.starts_with(prefix)) {
    return std::nullopt;
  }
  
  auto rest = token.substr(prefix.size());
  auto dot_pos = rest.find('.');
  if (dot_pos == std::string_view::npos || dot_pos == 0 ||
      dot_pos == rest.size() - 1) {
    return std::nullopt;
  }
  
  return std::pair{rest.substr(0, dot_pos), rest.substr(dot_pos + 1)};
}

// Find closing }} using char-by-char scan (O(n) guaranteed)
// Returns npos if not found
auto find_closing_braces(std::string_view tmpl, size_t start) -> size_t {
  for (size_t i = start; i + 1 < tmpl.size(); ++i) {
    if (tmpl[i] == '}' && tmpl[i + 1] == '}') {
      return i;
    }
  }
  return std::string_view::npos;
}

}  // namespace

auto TemplateResolver::resolve_template(
    std::string_view tmpl,
    const TemplateContext& ctx,
    [[maybe_unused]] const std::vector<XComPullConfig>& pulls)
    -> Result<std::string> {
  
  if (tmpl.empty()) {
    return std::string{};
  }

  // Pre-compute date formats once
  auto date_fmts = compute_date_formats(ctx.execution_date);
  date_fmts.data_interval_start = format_time_point(ctx.data_interval_start);
  date_fmts.data_interval_end = format_time_point(ctx.data_interval_end);
  
  // XCom cache: "task_id\0key" -> value (avoids repeated DB queries)
  std::unordered_map<std::string, std::string, StringHash, StringEqual> xcom_cache;
  
  std::string output;
  output.reserve(tmpl.size() * 2);  // Conservative estimate
  
  size_t pos = 0;
  while (pos < tmpl.size()) {
    // Check for escape sequence: \{{ -> {{
    if (pos + 2 < tmpl.size() && tmpl[pos] == '\\' && 
        tmpl[pos + 1] == '{' && tmpl[pos + 2] == '{') {
      output.append("{{");
      pos += 3;
      continue;
    }
    
    // Check for template start: {{
    if (pos + 1 < tmpl.size() && tmpl[pos] == '{' && tmpl[pos + 1] == '{') {
      // Find closing }} using O(n) char-by-char scan
      auto close_pos = find_closing_braces(tmpl, pos + 2);
      if (close_pos == std::string_view::npos) {
        // Unclosed {{ - leave as literal
        output.append("{{");
        pos += 2;
        continue;
      }
      
      // Extract and trim the token
      auto token = trim(tmpl.substr(pos + 2, close_pos - pos - 2));
      bool replaced = false;
      
      // Try date variables first (most common)
      if (date_fmts.valid) {
        if (token == "ds") {
          output.append(date_fmts.ds);
          replaced = true;
        } else if (token == "ds_nodash") {
          output.append(date_fmts.ds_nodash);
          replaced = true;
        } else if (token == "ts") {
          output.append(date_fmts.ts);
          replaced = true;
        } else if (token == "ts_nodash") {
          output.append(date_fmts.ts_nodash);
          replaced = true;
        }
      }
      
      // Try data interval variables
      if (!replaced) {
        if (token == "data_interval_start" && !date_fmts.data_interval_start.empty()) {
          output.append(date_fmts.data_interval_start);
          replaced = true;
        } else if (token == "data_interval_end" && !date_fmts.data_interval_end.empty()) {
          output.append(date_fmts.data_interval_end);
          replaced = true;
        }
      }
      
      // Try XCom lookup
      if (!replaced) {
        if (auto xcom_parts = parse_xcom_token(token)) {
          auto const& [task_str, key_str] = *xcom_parts;
          
          // Build cache key: "task_id\0key"
          std::string cache_key;
          cache_key.reserve(task_str.size() + 1 + key_str.size());
          cache_key.append(task_str);
          cache_key.push_back('\0');
          cache_key.append(key_str);
          
          // Check cache first
          auto cache_it = xcom_cache.find(cache_key);
          if (cache_it != xcom_cache.end()) {
            output.append(cache_it->second);
            replaced = true;
          } else {
            // Query DB and cache result
            TaskId task_id{std::string(task_str)};
            std::string key{key_str};
            
            auto xcom_result = persistence_.get_xcom(ctx.dag_run_id, task_id, key);
            if (!xcom_result) {
              return std::unexpected(xcom_result.error());
            }
            
            auto value = stringify(*xcom_result);
            output.append(value);
            xcom_cache[std::move(cache_key)] = std::move(value);
            replaced = true;
          }
        }
      }
      
      if (replaced) {
        pos = close_pos + 2;
      } else {
        // Unknown token - leave as literal
        output.append(tmpl.substr(pos, close_pos + 2 - pos));
        pos = close_pos + 2;
      }
      continue;
    }
    
    // Regular character - copy to output
    output.push_back(tmpl[pos]);
    ++pos;
  }
  
  return output;
}

auto TemplateResolver::resolve_date_variables(std::string_view tmpl,
                                              const TemplateContext& ctx)
    -> std::string {
  // This method is now only used internally or for backwards compatibility
  // The main resolve_template now handles date variables in a single pass
  
  if (ctx.execution_date == std::chrono::system_clock::time_point{}) {
    return std::string(tmpl);
  }

  auto const date_fmts = compute_date_formats(ctx.execution_date);
  
  std::string result(tmpl);

  auto replace_all = [](std::string& str, std::string_view from,
                        std::string_view to) {
    size_t pos = 0;
    while ((pos = str.find(from, pos)) != std::string::npos) {
      str.replace(pos, from.length(), to);
      pos += to.length();
    }
  };

  replace_all(result, "{{ts_nodash}}", date_fmts.ts_nodash);
  replace_all(result, "{{ds_nodash}}", date_fmts.ds_nodash);
  replace_all(result, "{{ts}}", date_fmts.ts);
  replace_all(result, "{{ds}}", date_fmts.ds);

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
