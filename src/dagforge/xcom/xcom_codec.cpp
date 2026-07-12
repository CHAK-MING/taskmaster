#include "dagforge/xcom/xcom_codec.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/xcom/xcom_util.hpp"

#include <glaze/json.hpp>

namespace dagforge::xcom_dto {

struct XComPushConfigJson {
  std::string key;
  std::string source;
  std::string json_pointer;
  std::string regex;
  int regex_group{0};
};

struct XComPullConfigJson {
  std::string from;
  std::string key;
  std::string env;
  bool required{false};
  JsonValue default_value;
  bool has_default_value{false};
};

} // namespace dagforge::xcom_dto

namespace glz {

template <> struct meta<dagforge::xcom_dto::XComPushConfigJson> {
  using T = dagforge::xcom_dto::XComPushConfigJson;
  static constexpr auto value = object(
      "key", &T::key, "source", &T::source, "json_pointer",
      &T::json_pointer, "regex", &T::regex, "regex_group", &T::regex_group);
};

template <> struct meta<dagforge::xcom_dto::XComPullConfigJson> {
  using T = dagforge::xcom_dto::XComPullConfigJson;
  static constexpr auto value = object("from", &T::from, "key", &T::key,
                                       "env", &T::env, "required", &T::required,
                                       "default_value", &T::default_value,
                                       "has_default_value", &T::has_default_value);
};

} // namespace glz

namespace dagforge::xcom {

auto serialize_push_configs(const std::vector<XComPushConfig> &pushes)
    -> std::string {
  std::vector<xcom_dto::XComPushConfigJson> out;
  out.reserve(pushes.size());
  for (const auto &push : pushes) {
    out.push_back(xcom_dto::XComPushConfigJson{
        .key = push.key,
        .source = enum_to_string(push.source),
        .json_pointer = push.json_pointer,
        .regex = push.regex_pattern,
        .regex_group = push.regex_group,
    });
  }
  if (auto serialized = serialize_json(out); serialized) {
    return std::move(*serialized);
  }
  return "[]";
}

auto parse_push_configs(std::string_view input)
    -> Result<std::vector<XComPushConfig>> {
  if (input.empty() || input == "[]") {
    return ok(std::vector<XComPushConfig>{});
  }

  auto parsed =
      parse_json_as<std::vector<xcom_dto::XComPushConfigJson>>(input);
  if (!parsed) {
    return fail(parsed.error());
  }

  std::vector<XComPushConfig> out;
  out.reserve(parsed->size());
  for (auto &item : *parsed) {
    XComPushConfig cfg;
    cfg.key = std::move(item.key);
    if (!item.source.empty()) {
      cfg.source = parse<XComSource>(item.source);
    }
    cfg.json_pointer = std::move(item.json_pointer);
    cfg.regex_pattern = std::move(item.regex);
    cfg.regex_group = item.regex_group;
    if (auto prepared = cfg.prepare(); !prepared) {
      return fail(prepared.error());
    }
    out.push_back(std::move(cfg));
  }
  return ok(std::move(out));
}

auto serialize_pull_configs(const std::vector<XComPullConfig> &pulls)
    -> std::string {
  std::vector<xcom_dto::XComPullConfigJson> out;
  out.reserve(pulls.size());
  for (const auto &pull : pulls) {
    JsonValue default_value{};
    bool has_default_value = false;
    if (pull.has_default_value) {
      if (auto parsed = parse_json(pull.default_value_json); parsed) {
        default_value = std::move(*parsed);
        has_default_value = true;
      }
    }
    out.push_back(xcom_dto::XComPullConfigJson{
        .from = pull.ref.task_id.str(),
        .key = pull.ref.key,
        .env = pull.env_var,
        .required = pull.required,
        .default_value = std::move(default_value),
        .has_default_value = has_default_value,
    });
  }
  if (auto serialized = serialize_json(out); serialized) {
    return std::move(*serialized);
  }
  return "[]";
}

auto parse_pull_configs(std::string_view input)
    -> Result<std::vector<XComPullConfig>> {
  if (input.empty() || input == "[]") {
    return ok(std::vector<XComPullConfig>{});
  }

  auto parsed =
      parse_json_as<std::vector<xcom_dto::XComPullConfigJson>>(input);
  if (!parsed) {
    return fail(parsed.error());
  }

  std::vector<XComPullConfig> out;
  out.reserve(parsed->size());
  for (auto &item : *parsed) {
    XComPullConfig cfg;
    cfg.ref.task_id = TaskId{std::string(item.from)};
    cfg.ref.key = std::move(item.key);
    cfg.env_var = std::move(item.env);
    cfg.required = item.required;
    cfg.has_default_value = item.has_default_value;
    if (cfg.has_default_value) {
      cfg.default_value_json = dump_json(item.default_value);
      auto rendered = render_serialized_json(cfg.default_value_json);
      if (!rendered) {
        return fail(rendered.error());
      }
      cfg.default_value_rendered = std::move(*rendered);
    }
    out.push_back(std::move(cfg));
  }
  return ok(std::move(out));
}

} // namespace dagforge::xcom
