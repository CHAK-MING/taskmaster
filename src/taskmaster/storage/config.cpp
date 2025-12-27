#include "taskmaster/storage/config.hpp"

#include "taskmaster/util/log.hpp"

#include <yaml-cpp/yaml.h>

#include <fstream>

namespace taskmaster {

namespace {

template <typename T>
auto read_field(const YAML::Node& node, std::string_view key, T default_val)
    -> T {
  if (auto field = node[std::string(key)]) {
    return field.template as<T>();
  }
  return default_val;
}

template <typename T>
auto write_field(YAML::Emitter& out, std::string_view key, const T& value,
                 const T& default_val) -> void {
  if (value != default_val) {
    out << YAML::Key << std::string(key) << YAML::Value << value;
  }
}

// Specialization for string (check empty instead of default)
auto write_field_if_not_empty(YAML::Emitter& out, std::string_view key,
                              const std::string& value) -> void {
  if (!value.empty()) {
    out << YAML::Key << std::string(key) << YAML::Value << value;
  }
}

void from_yaml(const YAML::Node& node, TaskConfig& t) {
  t.id = node["id"].as<std::string>();
  t.name = read_field<std::string>(node, "name", t.id);
  t.command = node["command"].as<std::string>();
  t.working_dir = read_field<std::string>(node, "working_dir", "");
  t.executor = string_to_executor_type(
      read_field<std::string>(node, "executor", "shell"));

  if (auto deps = node["deps"]) {
    t.deps = deps.as<std::vector<std::string>>();
  }

  t.timeout = std::chrono::seconds(read_field(node, "timeout", 300));
  t.retry_interval =
      std::chrono::seconds(read_field(node, "retry_interval", 60));
  t.max_retries = read_field(node, "max_retries", 3);
  t.enabled = read_field(node, "enabled", true);
}

void from_yaml(const YAML::Node& node, SchedulerConfig& s) {
  s.log_level = read_field<std::string>(node, "log_level", "info");
  s.log_file = read_field<std::string>(node, "log_file", "");
  s.pid_file = read_field<std::string>(node, "pid_file", "");
  s.tick_interval_ms = read_field(node, "tick_interval_ms", 1000);
}

void from_yaml(const YAML::Node& node, ApiConfig& a) {
  a.enabled = read_field(node, "enabled", false);
  a.port = read_field<uint16_t>(node, "port", 8080);
  a.host = read_field<std::string>(node, "host", "127.0.0.1");
}

void from_yaml(const YAML::Node& node, Config& c) {
  if (auto scheduler = node["scheduler"])
    from_yaml(scheduler, c.scheduler);
  if (auto api = node["api"])
    from_yaml(api, c.api);
  if (auto tasks = node["tasks"]) {
    c.tasks.reserve(tasks.size());
    for (const auto& task_node : tasks) {
      TaskConfig task;
      from_yaml(task_node, task);
      c.tasks.push_back(std::move(task));
    }
  }
}

void to_yaml(YAML::Emitter& out, const TaskConfig& t) {
  out << YAML::BeginMap;
  out << YAML::Key << "id" << YAML::Value << t.id;
  write_field(out, "name", t.name, t.id);
  out << YAML::Key << "command" << YAML::Value << t.command;
  if (t.executor != ExecutorType::Shell) {
    out << YAML::Key << "executor" << YAML::Value
        << std::string(executor_type_to_string(t.executor));
  }
  write_field_if_not_empty(out, "working_dir", t.working_dir);
  if (!t.deps.empty()) {
    out << YAML::Key << "deps" << YAML::Value << YAML::Flow << t.deps;
  }
  write_field(out, "timeout", static_cast<int>(t.timeout.count()), 300);
  write_field(out, "retry_interval", static_cast<int>(t.retry_interval.count()),
              60);
  write_field(out, "max_retries", t.max_retries, 3);
  write_field(out, "enabled", t.enabled, true);
  out << YAML::EndMap;
}

void to_yaml(YAML::Emitter& out, const SchedulerConfig& s) {
  out << YAML::BeginMap;
  write_field<std::string>(out, "log_level", s.log_level, "info");
  write_field_if_not_empty(out, "log_file", s.log_file);
  write_field_if_not_empty(out, "pid_file", s.pid_file);
  write_field(out, "tick_interval_ms", s.tick_interval_ms, 1000);
  out << YAML::EndMap;
}

void to_yaml(YAML::Emitter& out, const ApiConfig& a) {
  out << YAML::BeginMap;
  write_field(out, "enabled", a.enabled, false);
  write_field<uint16_t>(out, "port", a.port, 8080);
  write_field<std::string>(out, "host", a.host, "127.0.0.1");
  out << YAML::EndMap;
}

}  // namespace

auto ConfigLoader::load_from_file(std::string_view path) -> Result<Config> {
  try {
    YAML::Node node = YAML::LoadFile(std::string(path));
    Config config;
    from_yaml(node, config);
    config.rebuild_index();
    return ok(std::move(config));
  } catch (const YAML::Exception& e) {
    log::error("Failed to load config file {}: {}", path, e.what());
    return fail(Error::ParseError);
  }
}

auto ConfigLoader::load_from_string(std::string_view yaml_str)
    -> Result<Config> {
  try {
    YAML::Node node = YAML::Load(std::string(yaml_str));
    Config config;
    from_yaml(node, config);
    config.rebuild_index();
    return ok(std::move(config));
  } catch (const YAML::Exception& e) {
    log::error("Failed to parse YAML config: {}", e.what());
    return fail(Error::ParseError);
  }
}

auto ConfigLoader::save_to_file(const Config& config, std::string_view path)
    -> Result<void> {
  try {
    std::string path_str{path};
    std::ofstream file(path_str);
    if (!file.is_open()) {
      log::error("Failed to open file for writing: {}", path);
      return fail(Error::FileOpenFailed);
    }
    file << to_string(config);
    return ok();
  } catch (const std::exception& e) {
    log::error("Failed to save config: {}", e.what());
    return fail(Error::Unknown);
  }
}

auto ConfigLoader::to_string(const Config& config) -> std::string {
  YAML::Emitter out;
  out << YAML::BeginMap;

  out << YAML::Key << "scheduler" << YAML::Value;
  to_yaml(out, config.scheduler);

  if (config.api.enabled || config.api.port != 8080 ||
      config.api.host != "127.0.0.1") {
    out << YAML::Key << "api" << YAML::Value;
    to_yaml(out, config.api);
  }

  if (!config.tasks.empty()) {
    out << YAML::Key << "tasks" << YAML::Value << YAML::BeginSeq;
    for (const auto& task : config.tasks) {
      to_yaml(out, task);
    }
    out << YAML::EndSeq;
  }

  out << YAML::EndMap;
  return out.c_str();
}

}  // namespace taskmaster
