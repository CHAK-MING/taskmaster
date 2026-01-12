#pragma once

#include "taskmaster/util/id.hpp"

#include <yaml-cpp/yaml.h>

#include <string>
#include <string_view>

namespace YAML {

template <typename Tag>
struct convert<taskmaster::TypedId<Tag>> {
  static auto encode(const taskmaster::TypedId<Tag>& id) -> Node {
    return Node(std::string(id.value()));
  }
  static auto decode(const Node& node, taskmaster::TypedId<Tag>& id) -> bool {
    if (!node.IsScalar()) return false;
    id = taskmaster::TypedId<Tag>{node.as<std::string>()};
    return true;
  }
};

}  // namespace YAML

namespace taskmaster {

template <typename T>
concept YamlParsable = requires(const YAML::Node& n) { { n.as<T>() }; };

template <YamlParsable T>
[[nodiscard]] auto yaml_get_or(const YAML::Node& node, std::string_view key,
                               T default_val) -> T {
  auto field = node[std::string(key)];
  if (!field || (!field.IsScalar() && !field.IsSequence() && !field.IsMap())) {
    return default_val;
  }
  return field.as<T>();
}

inline void yaml_emit(YAML::Emitter& out, std::string_view key,
                      const auto& value) {
  out << YAML::Key << std::string(key) << YAML::Value << value;
}

inline void yaml_emit_if_not_empty(YAML::Emitter& out, std::string_view key,
                                   std::string_view value) {
  if (!value.empty()) {
    out << YAML::Key << std::string(key) << YAML::Value << std::string(value);
  }
}

}  // namespace taskmaster
