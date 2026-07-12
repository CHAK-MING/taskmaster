#include "dagforge/executor/executor.hpp"

#include "dagforge/config/task_config.hpp"
#include "dagforge/executor/executor_dto.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

namespace dagforge {
namespace {

auto build_shell_config(const TaskConfig &task) -> Result<ExecutorConfig> {
  ShellExecutorConfig exec;
  return ok(ExecutorConfig{std::move(exec)});
}

auto build_docker_config(const TaskConfig &task) -> Result<ExecutorConfig> {
  DockerExecutorConfig exec;
  if (const auto *docker_cfg = task.executor_config.as<DockerExecutorConfig>()) {
    exec.image = docker_cfg->image;
    exec.docker_socket = docker_cfg->docker_socket;
    exec.pull_policy = docker_cfg->pull_policy;
    exec.env = docker_cfg->env;
  }
  return ok(ExecutorConfig{std::move(exec)});
}

auto build_lua_config(const TaskConfig &task) -> Result<ExecutorConfig> {
  LuaExecutorConfig exec;
  if (const auto *lua_cfg = task.executor_config.as<LuaExecutorConfig>()) {
    exec.script = lua_cfg->script;
    exec.script_file = lua_cfg->script_file;
    exec.max_instructions = lua_cfg->max_instructions;
    exec.max_memory_bytes = lua_cfg->max_memory_bytes;
  }
  return ok(ExecutorConfig{std::move(exec)});
}

auto build_noop_config(const TaskConfig &task) -> Result<ExecutorConfig> {
  NoopExecutorConfig exec;
  if (const auto *noop_cfg = task.executor_config.as<NoopExecutorConfig>()) {
    exec.exit_code = noop_cfg->exit_code;
  }
  return ok(ExecutorConfig{std::move(exec)});
}

auto serialize_default_config(const ExecutorConfig &) -> std::string {
  return "{}";
}

auto parse_default_config(ExecutorType type, std::string_view)
    -> Result<ExecutorConfig> {
  switch (type) {
  case ExecutorType::Shell:
    return ok(ExecutorConfig{ShellExecutorConfig{}});
  case ExecutorType::Noop:
    return ok(ExecutorConfig{NoopExecutorConfig{}});
  case ExecutorType::Docker:
    return ok(ExecutorConfig{DockerExecutorConfig{}});
  case ExecutorType::Lua:
    return ok(ExecutorConfig{LuaExecutorConfig{}});
  }
  return fail(Error::InvalidArgument);
}

auto parse_shell_config(std::string_view input) -> Result<ExecutorConfig> {
  return parse_default_config(ExecutorType::Shell, input);
}

auto parse_noop_config(std::string_view input) -> Result<ExecutorConfig> {
  return parse_default_config(ExecutorType::Noop, input);
}

auto serialize_docker_config(const ExecutorConfig &config) -> std::string {
  const auto *docker = config.as<DockerExecutorConfig>();
  if (docker == nullptr) {
    return "{}";
  }
  executor_dto::DockerExecutorConfigJson j{
      .image = docker->image,
      .socket = docker->docker_socket,
      .pull_policy = enum_to_string(docker->pull_policy),
  };
  if (auto out = serialize_json(j); out) {
    return std::move(*out);
  }
  return "{}";
}

auto parse_docker_config(std::string_view input) -> Result<ExecutorConfig> {
  if (input.empty() || input == "{}") {
    return ok(ExecutorConfig{DockerExecutorConfig{}});
  }

  auto parsed = parse_json_as<executor_dto::DockerExecutorConfigJson>(input);
  if (!parsed) {
    return fail(parsed.error());
  }
  auto j = std::move(*parsed);

  DockerExecutorConfig cfg{};
  cfg.image = std::move(j.image);
  cfg.docker_socket = std::move(j.socket);
  if (cfg.docker_socket.empty()) {
    cfg.docker_socket = "/var/run/docker.sock";
  }
  if (!j.pull_policy.empty()) {
    cfg.pull_policy = parse<ImagePullPolicy>(j.pull_policy);
  }
  return ok(ExecutorConfig{std::move(cfg)});
}

auto validate_docker_task(const TaskConfig &task, std::vector<std::string> &errors)
    -> void {
  const auto *docker = task.executor_config.as<DockerExecutorConfig>();
  if (docker != nullptr && docker->image.empty()) {
    errors.emplace_back(
        std::format("Task '{}': docker image cannot be empty", task.task_id));
  }
}

auto serialize_lua_config(const ExecutorConfig &config) -> std::string {
  const auto *lua = config.as<LuaExecutorConfig>();
  if (lua == nullptr) {
    return "{}";
  }
  executor_dto::LuaExecutorConfigJson j{
      .script = lua->script,
      .script_file = lua->script_file,
      .max_instructions = lua->max_instructions,
      .max_memory_bytes = lua->max_memory_bytes,
  };
  if (auto out = serialize_json(j); out) {
    return std::move(*out);
  }
  return "{}";
}

auto parse_lua_config(std::string_view input) -> Result<ExecutorConfig> {
  if (input.empty() || input == "{}") {
    return ok(ExecutorConfig{LuaExecutorConfig{}});
  }

  auto parsed = parse_json_as<executor_dto::LuaExecutorConfigJson>(input);
  if (!parsed) {
    return fail(parsed.error());
  }
  auto j = std::move(*parsed);

  LuaExecutorConfig cfg{};
  cfg.script = std::move(j.script);
  cfg.script_file = std::move(j.script_file);
  cfg.max_instructions = j.max_instructions;
  cfg.max_memory_bytes = j.max_memory_bytes;
  return ok(ExecutorConfig{std::move(cfg)});
}

auto validate_lua_task(const TaskConfig &task, std::vector<std::string> &errors)
    -> void {
  const auto *lua = task.executor_config.as<LuaExecutorConfig>();
  if (lua == nullptr) {
    errors.emplace_back(
        std::format("Task '{}': lua executor config missing", task.task_id));
    return;
  }
  if (lua->script.empty() == lua->script_file.empty()) {
    errors.emplace_back(std::format(
        "Task '{}': lua executor requires exactly one of script or script_file",
        task.task_id));
  }
  if (lua->max_instructions == 0) {
    errors.emplace_back(std::format(
        "Task '{}': lua max_instructions must be greater than zero",
        task.task_id));
  }
  if (lua->max_memory_bytes == 0) {
    errors.emplace_back(std::format(
        "Task '{}': lua max_memory_bytes must be greater than zero",
        task.task_id));
  }
}

} // namespace

} // namespace dagforge

namespace dagforge {

auto ExecutorRegistry::instance() -> ExecutorRegistry & {
  static ExecutorRegistry registry = [] {
    ExecutorRegistry value;
    value.register_type(
        ExecutorType::Shell, [](Runtime &rt) { return create_shell_executor(rt); },
        build_shell_config, serialize_default_config, parse_shell_config);
    value.register_type(
        ExecutorType::Docker,
        [](Runtime &rt) { return create_docker_executor(rt); },
        build_docker_config, serialize_docker_config, parse_docker_config,
        validate_docker_task);
    value.register_type(
        ExecutorType::Lua, [](Runtime &rt) { return create_lua_executor(rt); },
        build_lua_config, serialize_lua_config, parse_lua_config,
        validate_lua_task);
    value.register_type(
        ExecutorType::Noop, [](Runtime &rt) { return create_noop_executor(rt); },
        build_noop_config, serialize_default_config, parse_noop_config);
    return value;
  }();
  return registry;
}

auto ExecutorRegistry::register_type(ExecutorType type, Creator creator,
                                     ConfigBuilder builder,
                                     ConfigSerializer serializer,
                                     ConfigParser parser,
                                     TaskValidator validator) -> void {
  entries_[type] = Entry{.creator = std::move(creator),
                         .builder = std::move(builder),
                         .serializer = std::move(serializer),
                         .parser = std::move(parser),
                         .validator = std::move(validator)};
}

auto ExecutorRegistry::create(ExecutorType type, Runtime &rt) const
    -> std::unique_ptr<IExecutor> {
  auto it = entries_.find(type);
  if (it == entries_.end()) {
    return nullptr;
  }
  return it->second.creator(rt);
}

auto ExecutorRegistry::build_config(const TaskConfig &task) const
    -> Result<ExecutorConfig> {
  auto it = entries_.find(task.executor);
  if (it == entries_.end()) {
    return fail(Error::InvalidArgument);
  }
  return it->second.builder(task);
}

auto ExecutorRegistry::serialize_config(const ExecutorConfig &config) const
    -> std::string {
  auto it = entries_.find(config.type());
  if (it == entries_.end() || !it->second.serializer) {
    return "{}";
  }
  return it->second.serializer(config);
}

auto ExecutorRegistry::parse_persisted_config(
    ExecutorType type, std::string_view persisted_config) const
    -> Result<ExecutorConfig> {
  auto it = entries_.find(type);
  if (it == entries_.end()) {
    return fail(Error::InvalidArgument);
  }
  if (!it->second.parser) {
    return fail(Error::InvalidArgument);
  }
  return it->second.parser(persisted_config);
}

auto ExecutorRegistry::validate_task(const TaskConfig &task,
                                     std::vector<std::string> &errors) const
    -> void {
  auto it = entries_.find(task.executor);
  if (it == entries_.end() || !it->second.validator) {
    return;
  }
  it->second.validator(task, errors);
}

auto ExecutorRegistry::registered_types() const -> std::vector<ExecutorType> {
  return entries_ | std::views::keys | std::ranges::to<std::vector>();
}

} // namespace dagforge
