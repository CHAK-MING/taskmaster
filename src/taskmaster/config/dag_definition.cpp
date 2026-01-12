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
struct convert<taskmaster::TaskConfig> {
  static bool decode(const Node& node, taskmaster::TaskConfig& t) {
    if (!node.IsMap()) {
      return false;
    }
    
    t.task_id = taskmaster::TaskId{taskmaster::yaml_get_or<std::string>(node, "id", "")};
    t.name = taskmaster::yaml_get_or<std::string>(node, "name", "");
    t.command = taskmaster::yaml_get_or<std::string>(node, "command", "");
    t.working_dir = taskmaster::yaml_get_or<std::string>(node, "working_dir", "");
    t.executor = taskmaster::string_to_executor_type(
        taskmaster::yaml_get_or<std::string>(node, "executor", "shell"));
    
    if (auto deps = node["dependencies"]) {
      t.dependencies = deps.as<std::vector<taskmaster::TaskId>>();
    }

    t.timeout = std::chrono::seconds(taskmaster::yaml_get_or(node, "timeout", 300));
    t.retry_interval = std::chrono::seconds(taskmaster::yaml_get_or(node, "retry_interval", 60));
    t.max_retries = taskmaster::yaml_get_or(node, "max_retries", 3);

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
    to_yaml(out, "executor", std::string(executor_type_to_string(t.executor)));
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
