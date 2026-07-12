#include "dagforge/config/dag_info_loader.hpp"

#include "dagforge/config/toml_util.hpp"
#include "dagforge/dag/dag_validator.hpp"
#include "dagforge/scheduler/cron.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/xcom/xcom_util.hpp"

#include <glaze/toml.hpp>

#include <algorithm>
#include <chrono>
#include <format>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <variant>

using std::string_view_literals::operator""sv;

namespace dagforge {
namespace detail {
struct TaskFieldPresence {
  bool working_dir{false};
  bool executor{false};
  bool timeout{false};
  bool retry_interval{false};
  bool max_retries{false};
  bool trigger_rule{false};
  bool depends_on_past{false};
};

struct ParsedTaskConfig {
  TaskConfig task{};
  TaskFieldPresence presence{};
  std::vector<std::variant<std::string, TaskDependency>> dependencies;
  std::vector<XComPushConfig> xcom_push;
  std::vector<XComPullConfig> xcom_pull;
};

struct ParsedDagToml {
  DAGInfo dag{};
  std::optional<std::chrono::year_month_day> start_date;
  std::optional<std::chrono::year_month_day> end_date;
  TaskDefaults default_args{};
  std::vector<ParsedTaskConfig> tasks;

  ParsedDagToml() { dag.max_concurrent_runs = 16; }
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::TaskDependency> {
  using T = dagforge::TaskDependency;
  static constexpr auto read_task = [](T &value, const std::string &input) {
    value.task_id = dagforge::TaskId{input};
  };
  static constexpr auto value =
      object("task", custom<read_task, nullptr>, "label", &T::label);
};

template <> struct meta<dagforge::XComPushConfig> {
  using T = dagforge::XComPushConfig;
  static constexpr auto read_source = [](T &value, const std::string &input) {
    value.source = dagforge::parse<dagforge::XComSource>(input);
  };
  static constexpr auto value =
      object("key", &T::key, "source", custom<read_source, nullptr>,
             "json_pointer", &T::json_pointer, "regex", &T::regex_pattern,
             "regex_group", &T::regex_group);
};

template <> struct meta<dagforge::XComPullConfig> {
  using T = dagforge::XComPullConfig;
  static constexpr auto read_key = [](T &value, const std::string &input) {
    value.ref.key = input;
  };
  static constexpr auto read_from = [](T &value, const std::string &input) {
    value.ref.task_id = dagforge::TaskId{input};
  };
  static constexpr auto read_default_value = [](T &value,
                                                const dagforge::JsonValue &input) {
    value.default_value_json = dagforge::dump_json(input);
    value.has_default_value = true;
  };
  static constexpr auto value = object("key", custom<read_key, nullptr>, "from",
                                       custom<read_from, nullptr>, "env",
                                       &T::env_var, "required", &T::required,
                                       "default_value",
                                       custom<read_default_value, nullptr>);
};

template <> struct meta<dagforge::TaskDefaults> {
  using T = dagforge::TaskDefaults;
  static constexpr auto read_timeout = [](T &value, const int input) {
    value.execution_timeout = std::chrono::seconds(input);
  };
  static constexpr auto read_retry_interval = [](T &value, const int input) {
    value.retry_interval = std::chrono::seconds(input);
  };
  static constexpr auto read_trigger_rule = [](T &value,
                                               const std::string &input) {
    value.trigger_rule = dagforge::parse<dagforge::TriggerRule>(input);
  };
  static constexpr auto read_executor = [](T &value, const std::string &input) {
    value.executor = dagforge::parse<dagforge::ExecutorType>(input);
  };
  static constexpr auto value = object(
      "timeout", custom<read_timeout, nullptr>, "retry_interval",
      custom<read_retry_interval, nullptr>, "max_retries", &T::max_retries,
      "trigger_rule", custom<read_trigger_rule, nullptr>, "executor",
      custom<read_executor, nullptr>, "working_dir", &T::working_dir,
      "working_directory", &T::working_dir, "depends_on_past",
      &T::depends_on_past);
};

template <> struct meta<dagforge::detail::ParsedTaskConfig> {
  using T = dagforge::detail::ParsedTaskConfig;
  static constexpr auto read_id = [](T &value, const std::string &input) {
    value.task.task_id = dagforge::TaskId{input};
  };
  static constexpr auto read_name = [](T &value, const std::string &input) {
    value.task.name = input;
  };
  static constexpr auto read_command = [](T &value, const std::string &input) {
    value.task.command = input;
  };
  static constexpr auto read_working_dir = [](T &value,
                                              const std::string &input) {
    value.task.working_dir = input;
    value.presence.working_dir = true;
  };
  static constexpr auto read_executor = [](T &value, const std::string &input) {
    value.task.executor = dagforge::parse<dagforge::ExecutorType>(input);
    value.presence.executor = true;
  };
  static constexpr auto read_timeout = [](T &value, const int input) {
    value.task.execution_timeout = std::chrono::seconds(input);
    value.presence.timeout = true;
  };
  static constexpr auto read_retry_interval = [](T &value, const int input) {
    value.task.retry_interval = std::chrono::seconds(input);
    value.presence.retry_interval = true;
  };
  static constexpr auto read_max_retries = [](T &value, const int input) {
    value.task.max_retries = input;
    value.presence.max_retries = true;
  };
  static constexpr auto read_trigger_rule = [](T &value,
                                               const std::string &input) {
    value.task.trigger_rule = dagforge::parse<dagforge::TriggerRule>(input);
    value.presence.trigger_rule = true;
  };
  static constexpr auto read_is_branch = [](T &value, const bool input) {
    value.task.is_branch = input;
  };
  static constexpr auto read_branch_xcom_key = [](T &value,
                                                  const std::string &input) {
    value.task.branch_xcom_key = input;
  };
  static constexpr auto read_depends_on_past = [](T &value, const bool input) {
    value.task.depends_on_past = input;
    value.presence.depends_on_past = true;
  };
  static constexpr auto read_docker_image = [](T &value,
                                               const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::DockerExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    }
    cfg->image = input;
  };
  static constexpr auto read_docker_socket = [](T &value,
                                                const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::DockerExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    }
    cfg->docker_socket = input;
  };
  static constexpr auto read_pull_policy = [](T &value,
                                              const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::DockerExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::DockerExecutorConfig>();
    }
    cfg->pull_policy = dagforge::parse<dagforge::ImagePullPolicy>(input);
  };
  static constexpr auto read_sensor_type = [](T &value,
                                              const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->type = dagforge::parse<dagforge::SensorType>(input);
  };
  static constexpr auto read_sensor_interval = [](T &value, const int input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->poke_interval = std::chrono::seconds(input);
  };
  static constexpr auto read_soft_fail = [](T &value, const bool input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->soft_fail = input;
  };
  static constexpr auto read_sensor_expected_status = [](T &value,
                                                         const int input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->expected_status = input;
  };
  static constexpr auto read_sensor_http_method = [](T &value,
                                                     const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->http_method = input;
  };
  static constexpr auto read_target = [](T &value, const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::SensorExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::SensorExecutorConfig>();
    }
    cfg->target = input;
  };
  static constexpr auto read_lua_script = [](T &value, const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::LuaExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    }
    cfg->script = input;
  };
  static constexpr auto read_lua_script_file = [](T &value,
                                                  const std::string &input) {
    auto cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::LuaExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    }
    cfg->script_file = input;
  };
  static constexpr auto read_lua_max_instructions = [](T &value,
                                                       const std::int64_t input) {
    auto cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::LuaExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    }
    cfg->max_instructions = static_cast<std::uint64_t>(std::max<std::int64_t>(0, input));
  };
  static constexpr auto read_lua_max_memory_mb = [](T &value,
                                                    const std::int64_t input) {
    auto cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    if (cfg == nullptr) {
      value.task.executor_config = dagforge::LuaExecutorConfig{};
      cfg = value.task.executor_config.as<dagforge::LuaExecutorConfig>();
    }
    const auto megabytes =
        static_cast<std::uint64_t>(std::max<std::int64_t>(0, input));
    cfg->max_memory_bytes = megabytes * 1024ULL * 1024ULL;
  };
  static constexpr auto value = object(
      "id", custom<read_id, nullptr>, "name", custom<read_name, nullptr>,
      "command", custom<read_command, nullptr>, "working_dir",
      custom<read_working_dir, nullptr>, "working_directory",
      custom<read_working_dir, nullptr>, "executor",
      custom<read_executor, nullptr>, "timeout", custom<read_timeout, nullptr>,
      "retry_interval", custom<read_retry_interval, nullptr>, "max_retries",
      custom<read_max_retries, nullptr>, "trigger_rule",
      custom<read_trigger_rule, nullptr>, "is_branch",
      custom<read_is_branch, nullptr>, "branch_xcom_key",
      custom<read_branch_xcom_key, nullptr>, "depends_on_past",
      custom<read_depends_on_past, nullptr>, "dependencies", &T::dependencies,
      "xcom_push", &T::xcom_push, "xcom_pull", &T::xcom_pull, "docker_image",
      custom<read_docker_image, nullptr>, "docker_socket",
      custom<read_docker_socket, nullptr>, "pull_policy",
      custom<read_pull_policy, nullptr>, "sensor_type",
      custom<read_sensor_type, nullptr>, "sensor_interval",
      custom<read_sensor_interval, nullptr>, "soft_fail",
      custom<read_soft_fail, nullptr>, "sensor_expected_status",
      custom<read_sensor_expected_status, nullptr>, "sensor_http_method",
      custom<read_sensor_http_method, nullptr>, "target",
      custom<read_target, nullptr>, "script", custom<read_lua_script, nullptr>,
      "script_file", custom<read_lua_script_file, nullptr>,
      "max_instructions", custom<read_lua_max_instructions, nullptr>,
      "max_memory_mb", custom<read_lua_max_memory_mb, nullptr>);
};

template <> struct meta<dagforge::detail::ParsedDagToml> {
  using T = dagforge::detail::ParsedDagToml;
  static constexpr auto read_id = [](T &value, const std::string &input) {
    value.dag.dag_id = dagforge::DAGId{input};
  };
  static constexpr auto read_name = [](T &value, const std::string &input) {
    value.dag.name = input;
  };
  static constexpr auto read_description =
      [](T &value, const std::string &input) { value.dag.description = input; };
  static constexpr auto read_cron = [](T &value, const std::string &input) {
    value.dag.cron = input;
  };
  static constexpr auto read_max_active_runs = [](T &value, const int input) {
    value.dag.max_concurrent_runs = input;
  };
  static constexpr auto read_catchup = [](T &value, const bool input) {
    value.dag.catchup = input;
  };
  static constexpr auto value =
      object("id", custom<read_id, nullptr>, "name", custom<read_name, nullptr>,
             "description", custom<read_description, nullptr>, "cron",
             custom<read_cron, nullptr>, "start_date", &T::start_date,
             "end_date", &T::end_date, "max_active_runs",
             custom<read_max_active_runs, nullptr>, "catchup",
             custom<read_catchup, nullptr>, "default_args", &T::default_args,
             "tasks", &T::tasks);
};
} // namespace glz

namespace dagforge {
namespace {

auto apply_task_defaults(TaskConfig &task, const TaskDefaults &defaults,
                         const detail::TaskFieldPresence &presence) -> void {
  if (!presence.timeout) {
    task.execution_timeout = defaults.execution_timeout;
  }
  if (!presence.retry_interval) {
    task.retry_interval = defaults.retry_interval;
  }
  if (!presence.max_retries) {
    task.max_retries = defaults.max_retries;
  }
  if (!presence.trigger_rule) {
    task.trigger_rule = defaults.trigger_rule;
  }
  if (!presence.executor) {
    task.executor = defaults.executor;
  }
  if (!presence.depends_on_past) {
    task.depends_on_past = defaults.depends_on_past;
  }
  if (!presence.working_dir) {
    task.working_dir = defaults.working_dir;
  }
}

auto finalize_task(TaskConfig &task) -> void {
  if (task.name.empty()) {
    task.name = task.task_id.str();
  }

  switch (task.executor) {
  case ExecutorType::Docker: {
    auto cfg = task.executor_config.as<DockerExecutorConfig>() != nullptr
                   ? *task.executor_config.as<DockerExecutorConfig>()
                   : DockerExecutorConfig{};
    task.executor_config = std::move(cfg);
    break;
  }
  case ExecutorType::Sensor: {
    auto cfg = task.executor_config.as<SensorExecutorConfig>() != nullptr
                   ? *task.executor_config.as<SensorExecutorConfig>()
                   : SensorExecutorConfig{};
    if (cfg.http_method.empty()) {
      cfg.http_method = "GET";
    }
    task.executor_config = std::move(cfg);
    break;
  }
  case ExecutorType::Noop: {
    auto cfg = task.executor_config.as<NoopExecutorConfig>() != nullptr
                   ? *task.executor_config.as<NoopExecutorConfig>()
                   : NoopExecutorConfig{};
    task.executor_config = std::move(cfg);
    break;
  }
  case ExecutorType::Lua: {
    auto cfg = task.executor_config.as<LuaExecutorConfig>() != nullptr
                   ? *task.executor_config.as<LuaExecutorConfig>()
                   : LuaExecutorConfig{};
    task.executor_config = std::move(cfg);
    break;
  }
  case ExecutorType::Shell:
  default:
    task.executor_config = ShellExecutorConfig{};
    break;
  }
}

auto materialize_task_dependencies(
    const std::vector<std::variant<std::string, TaskDependency>> &input)
    -> std::vector<TaskDependency> {
  std::vector<TaskDependency> result;
  result.reserve(input.size());
  for (const auto &item : input) {
    if (const auto *task_id = std::get_if<std::string>(&item)) {
      if (!task_id->empty()) {
        result.push_back(
            TaskDependency{.task_id = TaskId{*task_id}, .label = ""});
      }
    } else {
      result.push_back(std::get<TaskDependency>(item));
    }
  }
  return result;
}

[[nodiscard]] auto toml_quote(std::string_view value) -> std::string {
  std::ostringstream out;
  out << std::quoted(std::string(value));
  return out.str();
}

auto append_toml_string(std::string &out, std::string_view key,
                        std::string_view value) -> void {
  out += std::format("{} = {}\n", key, toml_quote(value));
}

template <typename T>
auto append_toml_scalar(std::string &out, std::string_view key, const T &value)
    -> void {
  out += std::format("{} = {}\n", key, value);
}

auto append_toml_local_date(std::string &out, std::string_view key,
                            std::chrono::year_month_day value) -> void {
  auto encoded = toml_util::serialize_toml(value);
  if (!encoded) {
    log::error("Failed to serialize TOML local date for key '{}'", key);
    return;
  }
  out += std::format("{} = {}\n", key, *encoded);
}

auto append_toml_bool(std::string &out, std::string_view key, bool value)
    -> void {
  out += std::format("{} = {}\n", key, value ? "true" : "false");
}

auto append_task_array(std::string &out, std::string_view key,
                       const std::vector<std::string> &items) -> void {
  if (items.empty()) {
    return;
  }
  out += std::format("{} = [", key);
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out += ", ";
    }
    out += items[i];
  }
  out += "]\n";
}

auto append_task_toml(std::string &out, const TaskConfig &task) -> void {
  out += "\n[[tasks]]\n";
  append_toml_string(out, "id", task.task_id.str());
  if (!task.name.empty() && task.name != task.task_id.str()) {
    append_toml_string(out, "name", task.name);
  }
  if (!task.command.empty()) {
    append_toml_string(out, "command", task.command);
  }
  if (!task.working_dir.empty()) {
    append_toml_string(out, "working_dir", task.working_dir);
  }
  append_toml_string(out, "executor", enum_to_string(task.executor));
  append_toml_scalar(out, "timeout",
                     static_cast<int>(task.execution_timeout.count()));
  append_toml_scalar(out, "retry_interval",
                     static_cast<int>(task.retry_interval.count()));
  append_toml_scalar(out, "max_retries", task.max_retries);
  append_toml_string(out, "trigger_rule", enum_to_string(task.trigger_rule));
  if (task.is_branch) {
    append_toml_bool(out, "is_branch", true);
  }
  if (!task.branch_xcom_key.empty() && task.branch_xcom_key != "branch") {
    append_toml_string(out, "branch_xcom_key", task.branch_xcom_key);
  }
  if (task.depends_on_past) {
    append_toml_bool(out, "depends_on_past", true);
  }

  if (!task.dependencies.empty()) {
    auto deps = task.dependencies |
                std::views::transform([](const TaskDependency &dep) {
                  return toml_quote(dep.task_id.str());
                }) |
                std::ranges::to<std::vector>();
    append_task_array(out, "dependencies", deps);
  }
  if (const auto *docker = task.executor_config.as<DockerExecutorConfig>()) {
    if (!docker->image.empty()) {
      append_toml_string(out, "docker_image", docker->image);
    }
    if (!docker->docker_socket.empty()) {
      append_toml_string(out, "docker_socket", docker->docker_socket);
    }
    append_toml_string(out, "pull_policy", enum_to_string(docker->pull_policy));
  } else if (const auto *sensor =
                 task.executor_config.as<SensorExecutorConfig>()) {
    append_toml_string(out, "sensor_type", enum_to_string(sensor->type));
    append_toml_scalar(out, "sensor_interval",
                       static_cast<int>(sensor->poke_interval.count()));
    if (sensor->soft_fail) {
      append_toml_bool(out, "soft_fail", true);
    }
    append_toml_scalar(out, "sensor_expected_status", sensor->expected_status);
    if (!sensor->http_method.empty()) {
      append_toml_string(out, "sensor_http_method", sensor->http_method);
    }
    if (!sensor->target.empty()) {
      append_toml_string(out, "target", sensor->target);
    }
  } else if (const auto *lua = task.executor_config.as<LuaExecutorConfig>()) {
    if (!lua->script.empty()) {
      append_toml_string(out, "script", lua->script);
    }
    if (!lua->script_file.empty()) {
      append_toml_string(out, "script_file", lua->script_file);
    }
    append_toml_scalar(out, "max_instructions", lua->max_instructions);
    append_toml_scalar(out, "max_memory_mb",
                       lua->max_memory_bytes / (1024ULL * 1024ULL));
  }
}

[[nodiscard]] auto validate_dag_info(const DAGInfo &def)
    -> std::vector<std::string> {
  std::vector<std::string> errors;

  if (def.name.empty()) {
    errors.emplace_back("DAG name cannot be empty");
  }

  if (!def.cron.empty()) {
    if (auto res = CronExpr::parse(def.cron); !res) {
      errors.emplace_back(std::format("Invalid cron expression '{}': {}",
                                      def.cron, res.error().message()));
    }
  }

  if (def.tasks.empty()) {
    errors.emplace_back("DAG must have at least one task");
    return errors;
  }

  std::unordered_set<TaskId> task_ids;
  for (const auto &task : def.tasks) {
    if (task.task_id.value().empty()) {
      errors.emplace_back("Task ID cannot be empty");
      continue;
    }
    if (!is_valid_id_text(task.task_id.value())) {
      errors.emplace_back(std::format(
          "Task ID '{}' contains control characters", task.task_id));
      continue;
    }

    if (!task_ids.insert(task.task_id).second) {
      errors.emplace_back(std::format("Duplicate task ID: '{}'", task.task_id));
    }

    if (task.command.empty() && task.executor != ExecutorType::Sensor &&
        task.executor != ExecutorType::Lua &&
        task.executor != ExecutorType::Noop) {
      errors.emplace_back(
          std::format("Task '{}': command cannot be empty", task.task_id));
    }

    if (task.execution_timeout.count() < 0) {
      errors.emplace_back(
          std::format("Task '{}': negative timeout not allowed", task.task_id));
    }
    if (task.retry_interval.count() < 0) {
      errors.emplace_back(std::format(
          "Task '{}': negative retry interval not allowed", task.task_id));
    }
    if (task.max_retries < 0) {
      errors.emplace_back(std::format(
          "Task '{}': negative max retries not allowed", task.task_id));
    }

    ExecutorRegistry::instance().validate_task(task, errors);
  }

  for (const auto &task : def.tasks) {
    if (task.task_id.value().empty()) {
      continue;
    }
    for (const auto &dep : task.dependencies) {
      if (!is_valid_id_text(dep.task_id.value())) {
        errors.emplace_back(std::format(
            "Task '{}': dependency ID '{}' contains control characters",
            task.task_id, dep.task_id));
        continue;
      }
      if (dep.task_id == task.task_id) {
        errors.emplace_back(std::format(
            "Task '{}': self-dependency not allowed", task.task_id));
      } else if (!task_ids.contains(dep.task_id)) {
        errors.emplace_back(std::format("Task '{}': dependency '{}' not found",
                                        task.task_id, dep.task_id));
      }
    }
  }

  if (errors.empty()) {
    DAGInfo info;
    info.dag_id = def.dag_id;
    info.tasks = def.tasks;
    if (auto res = DAGValidator::validate(info); !res) {
      errors.emplace_back("Circular dependency detected");
    }
  }

  return errors;
}

[[nodiscard]] auto parse_dag_info_from_text(std::string_view text,
                                            std::string *diagnostic)
    -> Result<DAGInfo> {
  auto raw_result =
      toml_util::parse_toml<detail::ParsedDagToml>(text, diagnostic);
  if (!raw_result)
    return fail(raw_result.error());
  auto raw = std::move(*raw_result);

  if (raw.dag.dag_id.empty()) {
    constexpr auto kErr =
        "DAG parse error: missing required top-level field 'id'\n"
        "Hint: add `id = \"your_dag_id\"` before any [[tasks]] section.";
    log::error("{}", kErr);
    if (diagnostic) {
      *diagnostic = kErr;
    }
    return fail(Error::InvalidArgument);
  }
  auto dag = std::move(raw.dag);
  if (raw.start_date) {
    dag.start_date = std::chrono::sys_days{*raw.start_date};
  }
  if (raw.end_date) {
    dag.end_date = std::chrono::sys_days{*raw.end_date};
  }
  if (dag.name.empty()) {
    dag.name = dag.dag_id.str();
  }

  dag.tasks.reserve(raw.tasks.size());
  for (std::size_t i = 0; i < raw.tasks.size(); ++i) {
    auto &parsed_task = raw.tasks[i];
    if (parsed_task.task.task_id.empty()) {
      if (diagnostic) {
        *diagnostic = std::format(
            "DAG parse error: task #{} is missing required field 'id'\n"
            "Hint: add `id = \"task_name\"` under [[tasks]].",
            i + 1);
      }
      return fail(Error::InvalidArgument);
    }
    apply_task_defaults(parsed_task.task, raw.default_args,
                        parsed_task.presence);
    parsed_task.task.dependencies =
        materialize_task_dependencies(parsed_task.dependencies);
    parsed_task.task.xcom_push = std::move(parsed_task.xcom_push);
    for (auto &push : parsed_task.task.xcom_push) {
      if (auto prepared = push.prepare(); !prepared) {
        if (diagnostic) {
          *diagnostic =
              "DAG parse error: invalid xcom_push json_pointer or regex";
        }
        return fail(prepared.error());
      }
    }
    parsed_task.task.xcom_pull = std::move(parsed_task.xcom_pull);
    for (auto &pull : parsed_task.task.xcom_pull) {
      if (!pull.has_default_value) {
        continue;
      }
      auto rendered = xcom::render_serialized_json(pull.default_value_json);
      if (!rendered) {
        if (diagnostic) {
          *diagnostic = "DAG parse error: invalid xcom_pull default_value";
        }
        return fail(rendered.error());
      }
      pull.default_value_rendered = std::move(*rendered);
    }
    finalize_task(parsed_task.task);
    dag.tasks.emplace_back(std::move(parsed_task.task));
  }

  auto now = std::chrono::system_clock::now();
  dag.created_at = now;
  dag.updated_at = now;
  dag.rebuild_task_index();

  return ok(std::move(dag));
}

} // namespace

auto DAGInfoLoader::load_from_file(std::string_view path,
                                   std::string *diagnostic)
    -> Result<DAGInfo> {
  auto text = toml_util::read_file(path);
  if (!text) {
    if (diagnostic) {
      *diagnostic = text.error().message();
    }
    return fail(text.error());
  }

  return load_from_string(*text, diagnostic);
}

auto DAGInfoLoader::load_from_string(std::string_view toml_str,
                                     std::string *diagnostic)
    -> Result<DAGInfo> {
  try {
    auto result = parse_dag_info_from_text(toml_str, diagnostic);
    if (!result) {
      return fail(result.error());
    }

    auto errors = validate_dag_info(*result);
    if (!errors.empty()) {
      if (diagnostic) {
        *diagnostic = errors | std::views::join_with("; "sv) |
                      std::ranges::to<std::string>();
      }
      for (const auto &err : errors) {
        log::error("DAG validation error: {}", err);
      }
      return fail(Error::InvalidArgument);
    }

    return result;
  } catch (const std::exception &e) {
    log::error("TOML parse error: {}", e.what());
    if (diagnostic) {
      *diagnostic = e.what();
    }
    return fail(Error::ParseError);
  } catch (...) {
    log::error("TOML parse error: unknown exception");
    if (diagnostic) {
      *diagnostic = "unknown parse error";
    }
    return fail(Error::ParseError);
  }
}

auto DAGInfoLoader::to_string(const DAGInfo &dag) -> std::string {
  std::string out;
  out.reserve(512 + dag.tasks.size() * 256);

  append_toml_string(out, "id", dag.dag_id.str());
  append_toml_string(out, "name", dag.name);
  if (!dag.description.empty()) {
    append_toml_string(out, "description", dag.description);
  }
  if (!dag.cron.empty()) {
    append_toml_string(out, "cron", dag.cron);
  }
  if (dag.start_date) {
    append_toml_local_date(
        out, "start_date",
        std::chrono::year_month_day{
            std::chrono::floor<std::chrono::days>(*dag.start_date)});
  }
  if (dag.end_date) {
    append_toml_local_date(
        out, "end_date",
        std::chrono::year_month_day{
            std::chrono::floor<std::chrono::days>(*dag.end_date)});
  }
  append_toml_scalar(out, "max_active_runs", dag.max_concurrent_runs);
  append_toml_bool(out, "catchup", dag.catchup);

  for (const auto &task : dag.tasks) {
    append_task_toml(out, task);
  }

  return out;
}

} // namespace dagforge
