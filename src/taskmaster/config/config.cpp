#include "taskmaster/config/config.hpp"

#include "taskmaster/config/yaml_utils.hpp"
#include "taskmaster/util/log.hpp"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <sstream>

namespace YAML {

template <>
struct convert<taskmaster::StorageConfig> {
  static bool decode(const Node& node, taskmaster::StorageConfig& s) {
    if (!node.IsMap()) {
      return false;
    }
    s.db_file = taskmaster::yaml_get_or<std::string>(node, "db_file", "taskmaster.db");
    return true;
  }
};

template <>
struct convert<taskmaster::SchedulerConfig> {
  static bool decode(const Node& node, taskmaster::SchedulerConfig& s) {
    if (!node.IsMap()) {
      return false;
    }
    s.log_level = taskmaster::yaml_get_or<std::string>(node, "log_level", "info");
    s.log_file = taskmaster::yaml_get_or<std::string>(node, "log_file", "");
    s.pid_file = taskmaster::yaml_get_or<std::string>(node, "pid_file", "");
    s.tick_interval_ms = taskmaster::yaml_get_or(node, "tick_interval_ms", 1000);
    s.max_concurrency = taskmaster::yaml_get_or(node, "max_concurrency", 10);
    return true;
  }
};

template <>
struct convert<taskmaster::ApiConfig> {
  static bool decode(const Node& node, taskmaster::ApiConfig& a) {
    if (!node.IsMap()) {
      return false;
    }
    a.enabled = taskmaster::yaml_get_or(node, "enabled", false);
    a.port = taskmaster::yaml_get_or<uint16_t>(node, "port", 8080);
    a.host = taskmaster::yaml_get_or<std::string>(node, "host", "127.0.0.1");
    return true;
  }
};

template <>
struct convert<taskmaster::DAGSourceConfig> {
  static bool decode(const Node& node, taskmaster::DAGSourceConfig& d) {
    if (!node.IsMap()) {
      return false;
    }
    auto mode_str = taskmaster::yaml_get_or<std::string>(node, "mode", "file");
    d.mode = taskmaster::parse<taskmaster::DAGSourceMode>(mode_str);
    d.directory = taskmaster::yaml_get_or<std::string>(node, "directory", "./dags");
    d.scan_interval_sec = taskmaster::yaml_get_or(node, "scan_interval_sec", 30);
    return true;
  }
};

template <>
struct convert<taskmaster::SystemConfig> {
  static bool decode(const Node& node, taskmaster::SystemConfig& c) {
    if (!node.IsMap()) {
      return false;
    }
    if (auto storage = node["storage"]) {
      c.storage = storage.as<taskmaster::StorageConfig>();
    }
    if (auto scheduler = node["scheduler"]) {
      c.scheduler = scheduler.as<taskmaster::SchedulerConfig>();
    }
    if (auto api = node["api"]) {
      c.api = api.as<taskmaster::ApiConfig>();
    }
    if (auto dag_source = node["dag_source"]) {
      c.dag_source = dag_source.as<taskmaster::DAGSourceConfig>();
    }
    return true;
  }
};

}  // namespace YAML

namespace taskmaster {

namespace {

void to_yaml(YAML::Emitter& out, const StorageConfig& s) {
  out << YAML::BeginMap;
  if (s.db_file != "taskmaster.db") {
    yaml_emit(out, "db_file", s.db_file);
  }
  out << YAML::EndMap;
}

void to_yaml(YAML::Emitter& out, const SchedulerConfig& s) {
  out << YAML::BeginMap;
  yaml_emit(out, "log_level", s.log_level);
  yaml_emit_if_not_empty(out, "log_file", s.log_file);
  yaml_emit_if_not_empty(out, "pid_file", s.pid_file);
  if (s.tick_interval_ms != 1000) {
    yaml_emit(out, "tick_interval_ms", s.tick_interval_ms);
  }
  if (s.max_concurrency != 10) {
    yaml_emit(out, "max_concurrency", s.max_concurrency);
  }
  out << YAML::EndMap;
}

void to_yaml(YAML::Emitter& out, const ApiConfig& a) {
  out << YAML::BeginMap;
  if (a.enabled) {
    yaml_emit(out, "enabled", a.enabled);
  }
  if (a.port != 8080) {
    yaml_emit(out, "port", a.port);
  }
  if (a.host != "127.0.0.1") {
    yaml_emit(out, "host", a.host);
  }
  out << YAML::EndMap;
}

void to_yaml(YAML::Emitter& out, const DAGSourceConfig& d) {
  out << YAML::BeginMap;
  if (d.mode != DAGSourceMode::File) {
    yaml_emit(out, "mode", std::string(to_string_view(d.mode)));
  }
  if (d.directory != "./dags") {
    yaml_emit(out, "directory", d.directory);
  }
  if (d.scan_interval_sec != 30) {
    yaml_emit(out, "scan_interval_sec", d.scan_interval_sec);
  }
  out << YAML::EndMap;
}

}  // namespace

auto ConfigLoader::load_from_file(std::string_view path)
    -> Result<SystemConfig> {
  std::string path_str{path};
  std::ifstream file(path_str);
  if (!file.is_open()) {
    log::error("Failed to open config file: {}", path);
    return fail(Error::FileNotFound);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  return load_from_string(buffer.str());
}

auto ConfigLoader::load_from_string(std::string_view yaml_str)
    -> Result<SystemConfig> {
  try {
    YAML::Node root = YAML::Load(std::string(yaml_str));
    if (!root.IsDefined() || root.IsNull()) {
      log::error("Failed to parse YAML: empty or invalid content");
      return fail(Error::ParseError);
    }
    SystemConfig config = root.as<SystemConfig>();
    return ok(std::move(config));
  } catch (const YAML::Exception& e) {
    log::error("YAML parse error: {}", e.what());
    return fail(Error::ParseError);
  }
}


}  // namespace taskmaster
