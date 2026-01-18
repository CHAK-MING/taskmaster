#include "taskmaster/config/dag_definition.hpp"

#include "taskmaster/config/yaml_utils.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"

#include <yaml-cpp/yaml.h>

#include <fstream>
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
    } else {
      t.executor_config = taskmaster::ShellTaskConfig{};
    }

    if (auto deps = node["dependencies"]) {
      t.dependencies = deps.as<std::vector<taskmaster::TaskId>>();
    }

    t.timeout = std::chrono::seconds(taskmaster::yaml_get_or(node, "timeout", 300));
    t.retry_interval = std::chrono::seconds(taskmaster::yaml_get_or(node, "retry_interval", 60));
    t.max_retries = taskmaster::yaml_get_or(node, "max_retries", 3);
    t.trigger_rule = taskmaster::parse<taskmaster::TriggerRule>(
        taskmaster::yaml_get_or<std::string>(node, "trigger_rule", "all_success"));

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

    if (auto tasks = node["tasks"]) {
      d.tasks = tasks.as<std::vector<taskmaster::TaskConfig>>();
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
    out << YAML::Key << "dependencies" << YAML::Value << YAML::Flow << YAML::BeginSeq;
    for (const auto& dep : t.dependencies) {
      out << std::string(dep.value());
    }
    out << YAML::EndSeq;
  }
  if (t.timeout != std::chrono::seconds(300)) {
    to_yaml(out, "timeout", static_cast<int>(t.timeout.count()));
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
      if (dep == task.task_id) {
        errors.push_back(
            std::format("Task '{}': self-dependency not allowed", task.task_id));
      } else if (!task_ids.contains(dep)) {
        errors.push_back(
            std::format("Task '{}': dependency '{}' not found", task.task_id, dep));
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
