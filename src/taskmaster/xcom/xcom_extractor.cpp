#include "taskmaster/xcom/xcom_extractor.hpp"

#include <format>

namespace taskmaster {

auto XComExtractor::extract(const ExecutorResult& result,
                            const std::vector<XComPushConfig>& configs)
    -> Result<std::vector<ExtractedXCom>> {
  std::vector<ExtractedXCom> extracted;
  extracted.reserve(configs.size());

  for (const auto& config : configs) {
    auto xcom_result = extract_one(result, config);
    if (!xcom_result) {
      return std::unexpected(xcom_result.error());
    }
    extracted.push_back(std::move(*xcom_result));
  }

  return extracted;
}

auto XComExtractor::extract_one(const ExecutorResult& result,
                                const XComPushConfig& config)
    -> Result<ExtractedXCom> {
  std::string source_text = get_source_text(result, config.source);

  if (config.regex_pattern) {
    auto regex_result = apply_regex(source_text, *config.regex_pattern,
                                    config.regex_group);
    if (!regex_result) {
      return std::unexpected(regex_result.error());
    }
    source_text = std::move(*regex_result);
  }

  nlohmann::json value;

  if (config.source == XComSource::Json || config.json_path) {
    auto parsed = nlohmann::json::parse(source_text, nullptr, false);
    if (parsed.is_discarded()) {
      return fail(Error::InvalidArgument);
    }

    if (config.json_path) {
      auto path_result = apply_json_path(parsed, *config.json_path);
      if (!path_result) {
        return std::unexpected(path_result.error());
      }
      value = std::move(*path_result);
    } else {
      value = std::move(parsed);
    }
  } else if (config.source == XComSource::ExitCode) {
    value = result.exit_code;
  } else {
    value = source_text;
  }

  return ExtractedXCom{.key = config.key, .value = std::move(value)};
}

auto XComExtractor::get_source_text(const ExecutorResult& result,
                                    XComSource source) -> std::string {
  switch (source) {
    case XComSource::Stdout:
      return result.stdout_output;
    case XComSource::Stderr:
      return result.stderr_output;
    case XComSource::ExitCode:
      return std::to_string(result.exit_code);
    case XComSource::Json:
      return result.stdout_output;
  }
  return {};
}

auto XComExtractor::apply_regex(std::string_view text,
                                const std::string& pattern,
                                int group) -> Result<std::string> {
  try {
    std::regex re(pattern);
    std::match_results<std::string_view::const_iterator> match;

    if (!std::regex_search(text.begin(), text.end(), match, re)) {
      return fail(Error::NotFound);
    }

    if (group < 0 || static_cast<size_t>(group) >= match.size()) {
      return fail(Error::InvalidArgument);
    }

    return std::string(match[group].first, match[group].second);
  } catch (const std::regex_error&) {
    return fail(Error::InvalidArgument);
  }
}

auto XComExtractor::apply_json_path(const nlohmann::json& json,
                                    std::string_view path)
    -> Result<nlohmann::json> {
  const nlohmann::json* current = &json;
  std::string segment;
  size_t pos = 0;

  if (!path.empty() && path[0] == '.') {
    pos = 1;
  }

  while (pos < path.size()) {
    size_t next_dot = path.find('.', pos);
    size_t bracket = path.find('[', pos);

    if (bracket < next_dot) {
      segment = std::string(path.substr(pos, bracket - pos));
      if (!segment.empty()) {
        if (!current->contains(segment)) {
          return fail(Error::NotFound);
        }
        current = &(*current)[segment];
      }

      size_t end_bracket = path.find(']', bracket);
      if (end_bracket == std::string_view::npos) {
        return fail(Error::InvalidArgument);
      }

      auto index_str = path.substr(bracket + 1, end_bracket - bracket - 1);
      int index = std::stoi(std::string(index_str));

      if (!current->is_array() ||
          index < 0 ||
          static_cast<size_t>(index) >= current->size()) {
        return fail(Error::NotFound);
      }
      current = &(*current)[index];
      pos = end_bracket + 1;
      if (pos < path.size() && path[pos] == '.') {
        pos++;
      }
    } else {
      size_t end = (next_dot == std::string_view::npos) ? path.size() : next_dot;
      segment = std::string(path.substr(pos, end - pos));

      if (!segment.empty()) {
        if (!current->contains(segment)) {
          return fail(Error::NotFound);
        }
        current = &(*current)[segment];
      }
      pos = (next_dot == std::string_view::npos) ? path.size() : next_dot + 1;
    }
  }

  return *current;
}

}  // namespace taskmaster
