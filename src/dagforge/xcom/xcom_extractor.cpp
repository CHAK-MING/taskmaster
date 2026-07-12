#include "dagforge/xcom/xcom_extractor.hpp"
#include "dagforge/util/json.hpp"

#include <boost/algorithm/string/trim.hpp>
#include <regex>
#include <utility>


namespace dagforge::xcom {

namespace {

[[nodiscard]] auto extract_last_non_empty_line(std::string_view text)
    -> std::string {
  while (!text.empty() &&
         (text.back() == '\n' || text.back() == '\r')) {
    text.remove_suffix(1);
  }

  while (!text.empty()) {
    const auto line_start = text.rfind('\n');
    std::string_view line =
        line_start == std::string_view::npos ? text : text.substr(line_start + 1);
    line = boost::trim_copy(line);
    if (!line.empty()) {
      return std::string(line);
    }
    if (line_start == std::string_view::npos) {
      break;
    }
    text = text.substr(0, line_start);
    if (!text.empty() && text.back() == '\r') {
      text.remove_suffix(1);
    }
  }

  return {};
}

[[nodiscard]] auto get_source_text(const ExecutorResult &result,
                                   XComSource source) -> std::string {
  switch (source) {
  case XComSource::Stdout:
    return std::string(result.stdout_output);
  case XComSource::Stderr:
    return std::string(result.stderr_output);
  case XComSource::ExitCode:
    return std::to_string(result.exit_code);
  case XComSource::Json:
    return std::string(result.stdout_output);
  }
  std::unreachable();
}

[[nodiscard]] auto extract_json_from_output(std::string_view output)
    -> std::string {
  if (output.empty())
    return "";

  if (is_valid_json(output)) {
    return std::string(output);
  }

  auto last_start = output.find_last_of("{[");
  if (last_start != std::string_view::npos) {
    auto potential_json = boost::trim_copy(output.substr(last_start));
    if (is_valid_json(potential_json)) {
      return std::string{potential_json};
    }
  }

  return std::string(output);
}

[[nodiscard]] auto is_valid_json_pointer(std::string_view pointer) noexcept
    -> bool {
  if (pointer.empty()) {
    return true;
  }
  if (pointer.front() != '/') {
    return false;
  }
  for (std::size_t i = 0; i < pointer.size(); ++i) {
    if (pointer[i] != '~') {
      continue;
    }
    if (++i >= pointer.size() ||
        (pointer[i] != '0' && pointer[i] != '1')) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] auto apply_json_pointer(const JsonValue &json,
                                      std::string_view pointer)
    -> Result<JsonValue> {
  if (!is_valid_json_pointer(pointer)) {
    return fail(Error::InvalidArgument);
  }
  const auto *value = glz::navigate_to(&json, pointer);
  if (value == nullptr) {
    return fail(Error::NotFound);
  }
  return ok(*value);
}

[[nodiscard]] auto extract_one(const ExecutorResult &result,
                               const XComPushConfig &config)
    -> Result<ExtractedXCom> {
  std::string source_text = get_source_text(result, config.source);
  if (config.source != XComSource::Json &&
      config.source != XComSource::ExitCode &&
      config.regex_pattern.empty() && config.json_pointer.empty()) {
    source_text = extract_last_non_empty_line(source_text);
  }
  auto text_res = ok(std::move(source_text));
  if (!config.regex_pattern.empty()) {
    try {
      std::match_results<std::string::const_iterator> match;
      const auto &text = *text_res;
      const std::regex *re = config.compiled_regex.get();
      std::optional<std::regex> local_regex;
      if (re == nullptr) {
        local_regex.emplace(config.regex_pattern);
        re = &*local_regex;
      }

      if (!std::regex_search(text.begin(), text.end(), match, *re)) {
        return fail(Error::NotFound);
      }

      if (config.regex_group < 0 ||
          static_cast<size_t>(config.regex_group) >= match.size()) {
        return fail(Error::InvalidArgument);
      }

      text_res = ok(std::string(match[config.regex_group].first,
                                match[config.regex_group].second));
    } catch (const std::regex_error &) {
      return fail(Error::InvalidArgument);
    }
  }

  return std::move(text_res).and_then(
      [&](std::string &&text) -> Result<ExtractedXCom> {
        if (config.source == XComSource::Json ||
            !config.json_pointer.empty()) {
          if (config.source == XComSource::Json) {
            text = extract_json_from_output(text);
          }
          auto parsed = parse_json(text);
          if (!parsed) {
            return fail(Error::InvalidArgument);
          }

          if (!config.json_pointer.empty()) {
            return apply_json_pointer(*parsed, config.json_pointer)
                .transform([&](auto &&val) {
                  return ExtractedXCom{
                      .key = config.key,
                      .value = dump_json(val),
                  };
                });
          }
          return ok(
              ExtractedXCom{.key = config.key,
                            .value = dump_json(*parsed)});
        }

        JsonValue value = (config.source == XComSource::ExitCode)
                              ? JsonValue(result.exit_code)
                              : JsonValue(std::move(text));

        return ok(
            ExtractedXCom{.key = config.key, .value = dump_json(value)});
      });
}

} // namespace

auto extract(const ExecutorResult &result,
             const std::vector<XComPushConfig> &configs)
    -> Result<std::vector<ExtractedXCom>> {
  std::vector<ExtractedXCom> extracted;
  extracted.reserve(configs.size());

  for (const auto &config : configs) {
    auto xcom_result = extract_one(result, config);
    if (!xcom_result) {
      return fail(xcom_result.error());
    }
    extracted.emplace_back(std::move(*xcom_result));
  }

  return ok(extracted);
}

} // namespace dagforge::xcom
