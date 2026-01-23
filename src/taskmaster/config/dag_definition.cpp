#include "taskmaster/config/dag_definition.hpp"

#include "taskmaster/config/yaml_utils.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"

#include <yaml-cpp/yaml.h>

#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>
#include <format>

namespace YAML {

template<>
struct convert<taskmaster::XComSource> {
  static bool decode(const Node& node, taskmaster::XComSource& s) {
    if (!node.IsScalar()) return false;
    auto str = node.as<std::string>();
    if (str == "stdout") s = taskmaster::XComSource::Stdout;
    else if (str == "stderr") s = taskmaster::XComSource::Stderr;
    else if (str == "exit_code") s = taskmaster::XComSource::ExitCode;
    else if (str == "json") s = taskmaster::XComSource::Json;
    else return false;
    return true;
  }
};

template<>
struct convert<taskmaster::XComPushConfig> {
  static bool decode(const Node& node, taskmaster::XComPushConfig& p) {
    if (!node.IsMap()) return false;
    p.key = taskmaster::yaml_get_or<std::string>(node, "key", "");
    p.source = taskmaster::yaml_get_or(node, "source", taskmaster::XComSource::Stdout);
    if (auto jp = node["json_path"]) p.json_path = jp.as<std::string>();
    if (auto rp = node["regex"]) p.regex_pattern = rp.as<std::string>();
    p.regex_group = taskmaster::yaml_get_or(node, "regex_group", 0);
    return true;
  }
};

template<>
struct convert<taskmaster::XComPullConfig> {
  static bool decode(const Node& node, taskmaster::XComPullConfig& p) {
    if (!node.IsMap()) return false;
    p.key = taskmaster::yaml_get_or<std::string>(node, "key", "");
    p.source_task = taskmaster::TaskId{taskmaster::yaml_get_or<std::string>(node, "from", "")};
    p.env_var = taskmaster::yaml_get_or<std::string>(node, "env", "");
    return true;
  }
};

template<>
struct convert<taskmaster::TaskDefaults> {
  static bool decode(const Node& node, taskmaster::TaskDefaults& d) {
    if (!node.IsMap()) return false;
    
    if (auto v = node["timeout"]) {
      d.execution_timeout = std::chrono::seconds(v.as<int>());
    }
    if (auto v = node["retry_interval"]) {
      d.retry_interval = std::chrono::seconds(v.as<int>());
    }
    if (auto v = node["max_retries"]) {
      d.max_retries = v.as<int>();
    }
    if (auto v = node["trigger_rule"]) {
      d.trigger_rule = taskmaster::parse<taskmaster::TriggerRule>(v.as<std::string>());
    }
    if (auto v = node["executor"]) {
      d.executor = taskmaster::parse<taskmaster::ExecutorType>(v.as<std::string>());
    }
    if (auto v = node["working_dir"]) {
      d.working_dir = v.as<std::string>();
    }
    if (auto v = node["depends_on_past"]) {
      d.depends_on_past = v.as<bool>();
    }
    return true;
  }
};

template<>
struct convert<taskmaster::TaskDependency> {
  static bool decode(const Node& node, taskmaster::TaskDependency& dep) {
    if (node.IsScalar()) {
      dep.task_id = taskmaster::TaskId{node.as<std::string>()};
      dep.label.clear();
      return true;
    }
    if (node.IsMap()) {
      dep.task_id = taskmaster::TaskId{taskmaster::yaml_get_or<std::string>(node, "task", "")};
      dep.label = taskmaster::yaml_get_or<std::string>(node, "label", "");
      return true;
    }
    return false;
  }
};

template<>
struct convert<taskmaster::TaskConfig> {
  static bool decode(const Node& node, taskmaster::TaskConfig& t) {
    if (!node.IsMap()) {
      return false;
    }
    
    t.task_id = taskmaster::TaskId{taskmaster::yaml_get_or<std::string>(node, "id", "")};
    t.name = taskmaster::yaml_get_or<std::string>(node, "name", "");
    t.command = taskmaster::yaml_get_or<std::string>(node, "command", "");
    t.working_dir = taskmaster::yaml_get_or<std::string>(node, "working_dir", "");
    t.executor = taskmaster::parse<taskmaster::ExecutorType>(
        taskmaster::yaml_get_or<std::string>(node, "executor", "shell"));
    
    if (t.executor == taskmaster::ExecutorType::Docker) {
      taskmaster::DockerTaskConfig docker_cfg;
      docker_cfg.image = taskmaster::yaml_get_or<std::string>(node, "docker_image", "");
      docker_cfg.socket = taskmaster::yaml_get_or<std::string>(node, "docker_socket", "/var/run/docker.sock");
      docker_cfg.pull_policy = taskmaster::parse<taskmaster::ImagePullPolicy>(
          taskmaster::yaml_get_or<std::string>(node, "pull_policy", "never"));
      t.executor_config = docker_cfg;
    } else if (t.executor == taskmaster::ExecutorType::Sensor) {
      taskmaster::SensorTaskConfig sensor_cfg;

      auto sensor_type = taskmaster::yaml_get_or<std::string>(node, "sensor_type", "file");
      if (sensor_type == "http" || sensor_type == "Http" || sensor_type == "HTTP") {
        sensor_cfg.type = taskmaster::SensorType::Http;
      } else if (sensor_type == "command" || sensor_type == "Command") {
        sensor_cfg.type = taskmaster::SensorType::Command;
      } else {
        sensor_cfg.type = taskmaster::SensorType::File;
      }

      // Target
      sensor_cfg.target = taskmaster::yaml_get_or<std::string>(node, "sensor_target", "");
      if (sensor_cfg.target.empty()) {
        if (sensor_cfg.type == taskmaster::SensorType::File) {
          sensor_cfg.target = taskmaster::yaml_get_or<std::string>(node, "sensor_path", "");
        } else if (sensor_cfg.type == taskmaster::SensorType::Http) {
          sensor_cfg.target = taskmaster::yaml_get_or<std::string>(node, "sensor_url", "");
        } else {
          sensor_cfg.target = taskmaster::yaml_get_or<std::string>(node, "sensor_command", "");
          if (sensor_cfg.target.empty()) {
            // Backward-compatible: reuse `command` as the command sensor target.
            sensor_cfg.target = t.command;
          }
        }
      }

      // Timings
      sensor_cfg.poke_interval = std::chrono::seconds(
          taskmaster::yaml_get_or(node, "sensor_interval", static_cast<int>(sensor_cfg.poke_interval.count())));

      if (auto v = node["sensor_timeout"]) {
        sensor_cfg.sensor_timeout = std::chrono::seconds(v.as<int>());
      } else if (auto v = node["timeout"]) {
        // For sensor tasks, many examples use `timeout` as the sensor timeout.
        sensor_cfg.sensor_timeout = std::chrono::seconds(v.as<int>());
      }

      sensor_cfg.soft_fail = taskmaster::yaml_get_or(node, "soft_fail", sensor_cfg.soft_fail);
      sensor_cfg.expected_status = taskmaster::yaml_get_or(node, "sensor_expected_status", sensor_cfg.expected_status);
      sensor_cfg.http_method = taskmaster::yaml_get_or<std::string>(node, "sensor_http_method", sensor_cfg.http_method);

      t.executor_config = sensor_cfg;
    } else {
      t.executor_config = taskmaster::ShellTaskConfig{};
    }

    if (auto deps = node["dependencies"]) {
      t.dependencies = deps.as<std::vector<taskmaster::TaskDependency>>();
    }

    t.execution_timeout = std::chrono::seconds(taskmaster::yaml_get_or(node, "timeout", 300));
    t.retry_interval = std::chrono::seconds(taskmaster::yaml_get_or(node, "retry_interval", 60));
    t.max_retries = taskmaster::yaml_get_or(node, "max_retries", 3);
    t.trigger_rule = taskmaster::parse<taskmaster::TriggerRule>(
        taskmaster::yaml_get_or<std::string>(node, "trigger_rule", "all_success"));
    t.is_branch = taskmaster::yaml_get_or(node, "is_branch", false);
    t.branch_xcom_key = taskmaster::yaml_get_or<std::string>(node, "branch_xcom_key", "branch");
    t.depends_on_past = taskmaster::yaml_get_or(node, "depends_on_past", false);

    if (auto push = node["xcom_push"]) {
      t.xcom_push = push.as<std::vector<taskmaster::XComPushConfig>>();
    }
    if (auto pull = node["xcom_pull"]) {
      t.xcom_pull = pull.as<std::vector<taskmaster::XComPullConfig>>();
    }

    return true;
  }
};

template<>
struct convert<taskmaster::DAGDefinition> {
  static bool decode(const Node& node, taskmaster::DAGDefinition& d) {
    if (!node.IsMap()) {
      return false;
    }

    d.name = taskmaster::yaml_get_or<std::string>(node, "name", "");
    d.description = taskmaster::yaml_get_or<std::string>(node, "description", "");
    d.cron = taskmaster::yaml_get_or<std::string>(node, "cron", "");
    d.catchup = taskmaster::yaml_get_or(node, "catchup", false);

    if (auto start_date_node = node["start_date"]) {
      auto date_str = start_date_node.as<std::string>();
      std::tm tm = {};
      std::istringstream ss(date_str);
      ss >> std::get_time(&tm, "%Y-%m-%d");
      if (!ss.fail()) {
        d.start_date = std::chrono::system_clock::from_time_t(timegm(&tm));
      }
    }

    if (auto end_date_node = node["end_date"]) {
      auto date_str = end_date_node.as<std::string>();
      std::tm tm = {};
      std::istringstream ss(date_str);
      ss >> std::get_time(&tm, "%Y-%m-%d");
      if (!ss.fail()) {
        d.end_date = std::chrono::system_clock::from_time_t(timegm(&tm));
      }
    }

    if (auto defaults = node["default_args"]) {
      d.default_args = defaults.as<taskmaster::TaskDefaults>();
    }

    if (auto tasks = node["tasks"]) {
      d.tasks = tasks.as<std::vector<taskmaster::TaskConfig>>();
      
      for (auto& t : d.tasks) {
        if (d.default_args.execution_timeout && t.execution_timeout == std::chrono::seconds(300)) {
          t.execution_timeout = *d.default_args.execution_timeout;
        }
        if (d.default_args.retry_interval && t.retry_interval == std::chrono::seconds(60)) {
          t.retry_interval = *d.default_args.retry_interval;
        }
        if (d.default_args.max_retries && t.max_retries == 3) {
          t.max_retries = *d.default_args.max_retries;
        }
        if (d.default_args.trigger_rule && t.trigger_rule == taskmaster::TriggerRule::AllSuccess) {
          t.trigger_rule = *d.default_args.trigger_rule;
        }
        if (d.default_args.executor && t.executor == taskmaster::ExecutorType::Shell) {
          t.executor = *d.default_args.executor;
        }
        if (d.default_args.working_dir && t.working_dir.empty()) {
          t.working_dir = *d.default_args.working_dir;
        }
        if (d.default_args.depends_on_past && !t.depends_on_past) {
          t.depends_on_past = *d.default_args.depends_on_past;
        }
      }
    }

    return true;
  }
};

}

namespace taskmaster {

namespace {

void to_yaml(YAML::Emitter& out, std::string_view key, const auto& value) {
  if constexpr (requires { value.value(); }) {
    out << YAML::Key << std::string(key) << YAML::Value << std::string(value.value());
  } else {
    out << YAML::Key << std::string(key) << YAML::Value << value;
  }
}

void to_yaml_if_not_empty(YAML::Emitter& out, std::string_view key,
                          std::string_view value) {
  if (!value.empty()) {
    out << YAML::Key << std::string(key) << YAML::Value << std::string(value);
  }
}

void to_yaml(YAML::Emitter& out, const TaskConfig& t) {
  out << YAML::BeginMap;
  to_yaml(out, "task_id", t.task_id);
  if (!t.name.empty()) {
    to_yaml(out, "name", t.name);
  }
  to_yaml(out, "command", t.command);
  if (t.executor != ExecutorType::Shell) {
    to_yaml(out, "executor", std::string(to_string_view(t.executor)));
  }
  if (auto* docker_cfg = std::get_if<DockerTaskConfig>(&t.executor_config)) {
    if (!docker_cfg->image.empty()) {
      to_yaml(out, "docker_image", docker_cfg->image);
    }
    if (docker_cfg->socket != "/var/run/docker.sock") {
      to_yaml(out, "docker_socket", docker_cfg->socket);
    }
    if (docker_cfg->pull_policy != ImagePullPolicy::Never) {
      to_yaml(out, "pull_policy", std::string(to_string_view(docker_cfg->pull_policy)));
    }
  }
  to_yaml_if_not_empty(out, "working_dir", t.working_dir);
  if (!t.dependencies.empty()) {
    out << YAML::Key << "dependencies" << YAML::Value << YAML::BeginSeq;
    for (const auto& dep : t.dependencies) {
      if (dep.label.empty()) {
        out << std::string(dep.task_id.value());
      } else {
        out << YAML::Flow << YAML::BeginMap;
        out << YAML::Key << "task" << YAML::Value << std::string(dep.task_id.value());
        out << YAML::Key << "label" << YAML::Value << dep.label;
        out << YAML::EndMap;
      }
    }
    out << YAML::EndSeq;
  }
  if (t.execution_timeout != std::chrono::seconds(300)) {
    to_yaml(out, "timeout", static_cast<int>(t.execution_timeout.count()));
  }
  if (t.retry_interval != std::chrono::seconds(60)) {
    to_yaml(out, "retry_interval", static_cast<int>(t.retry_interval.count()));
  }
  if (t.max_retries != 3) {
    to_yaml(out, "max_retries", t.max_retries);
  }
  if (!t.xcom_push.empty()) {
    out << YAML::Key << "xcom_push" << YAML::Value << YAML::BeginSeq;
    for (const auto& p : t.xcom_push) {
      out << YAML::BeginMap;
      out << YAML::Key << "key" << YAML::Value << p.key;
      if (p.source != XComSource::Stdout) {
        const char* src = p.source == XComSource::Stderr ? "stderr" :
                          p.source == XComSource::ExitCode ? "exit_code" : "json";
        out << YAML::Key << "source" << YAML::Value << src;
      }
      if (p.json_path) out << YAML::Key << "json_path" << YAML::Value << *p.json_path;
      if (p.regex_pattern) out << YAML::Key << "regex" << YAML::Value << *p.regex_pattern;
      if (p.regex_group != 0) out << YAML::Key << "regex_group" << YAML::Value << p.regex_group;
      out << YAML::EndMap;
    }
    out << YAML::EndSeq;
  }
  if (!t.xcom_pull.empty()) {
    out << YAML::Key << "xcom_pull" << YAML::Value << YAML::BeginSeq;
    for (const auto& p : t.xcom_pull) {
      out << YAML::BeginMap;
      out << YAML::Key << "key" << YAML::Value << p.key;
      out << YAML::Key << "from" << YAML::Value << std::string(p.source_task.value());
      out << YAML::Key << "env" << YAML::Value << p.env_var;
      out << YAML::EndMap;
    }
    out << YAML::EndSeq;
  }
  out << YAML::EndMap;
}

auto validate_definition(const DAGDefinition& def) -> std::vector<std::string> {
  std::vector<std::string> errors;

  if (def.name.empty()) {
    errors.push_back("DAG name cannot be empty");
  }

  if (def.tasks.empty()) {
    errors.push_back("DAG must have at least one task");
    return errors;
  }

  std::unordered_set<TaskId> task_ids;
  for (const auto& task : def.tasks) {
    if (task.task_id.empty()) {
      errors.push_back("Task ID cannot be empty");
      continue;
    }

    if (!task_ids.insert(task.task_id).second) {
      errors.push_back(std::format("Duplicate task ID: '{}'", task.task_id));
    }

    if (task.command.empty()) {
      errors.push_back(std::format("Task '{}': command cannot be empty", task.task_id));
    }
  }

  for (const auto& task : def.tasks) {
    if (task.task_id.empty())
      continue;

    for (const auto& dep : task.dependencies) {
      if (dep.task_id == task.task_id) {
        errors.push_back(
            std::format("Task '{}': self-dependency not allowed", task.task_id));
      } else if (!task_ids.contains(dep.task_id)) {
        errors.push_back(
            std::format("Task '{}': dependency '{}' not found", task.task_id, dep.task_id));
      }
    }
  }

  return errors;
}

}  // namespace

auto DAGDefinitionLoader::load_from_file(std::string_view path)
    -> Result<DAGDefinition> {
  std::string path_str{path};
  std::ifstream file(path_str);
  if (!file.is_open()) {
    log::error("Failed to open DAG definition file: {}", path);
    return fail(Error::FileNotFound);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  auto result = load_from_string(buffer.str());
  if (result) {
    result->source_file = path_str;
  }
  return result;
}

auto DAGDefinitionLoader::load_from_string(std::string_view yaml_str)
    -> Result<DAGDefinition> {
  try {
    YAML::Node root = YAML::Load(std::string(yaml_str));
    
    DAGDefinition def = root.as<DAGDefinition>();
    
    auto errors = validate_definition(def);
    if (!errors.empty()) {
      for (const auto& err : errors) {
        log::error("DAG validation error: {}", err);
      }
      return fail(Error::InvalidArgument);
    }

    return ok(std::move(def));
  } catch (const YAML::Exception& e) {
    log::error("YAML parse error: {}", e.what());
    return fail(Error::ParseError);
  }
}

auto DAGDefinitionLoader::to_string(const DAGDefinition& dag) -> std::string {
  YAML::Emitter out;
  out << YAML::BeginMap;

  to_yaml_if_not_empty(out, "name", dag.name);
  to_yaml_if_not_empty(out, "description", dag.description);
  to_yaml_if_not_empty(out, "cron", dag.cron);

  if (!dag.tasks.empty()) {
    out << YAML::Key << "tasks" << YAML::Value << YAML::BeginSeq;
    for (const auto& task : dag.tasks) {
      to_yaml(out, task);
    }
    out << YAML::EndSeq;
  }

  out << YAML::EndMap;
  return out.c_str();
}

} 
