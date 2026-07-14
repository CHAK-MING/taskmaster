#include "dagforge/executors/command/executor.hpp"

#include "dagforge/util/json.hpp"

#include "../../sandbox/detail/command_policy.hpp"
#include "../../sandbox/detail/minijail_command_runner.hpp"
#include "../detail/task_executor_utils.hpp"
#include "detail/testing.hpp"

#include <cstdint>
#include <filesystem>
#include <format>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::executors::command::detail {

struct EnvironmentEntry {
  std::string key;
  std::string value;
};

struct InputEnvironmentBinding {
  std::string input;
  std::string environment;
};

struct NodeConfig {
  std::string program;
  std::vector<std::string> arguments;
  std::vector<EnvironmentEntry> env;
  std::vector<InputEnvironmentBinding> input_env;
};

} // namespace dagforge::executors::command::detail

namespace glz {

template <>
struct meta<dagforge::executors::command::detail::EnvironmentEntry> {
  using T = dagforge::executors::command::detail::EnvironmentEntry;
  static constexpr auto value = object("key", &T::key, "value", &T::value);
};

template <>
struct meta<dagforge::executors::command::detail::InputEnvironmentBinding> {
  using T = dagforge::executors::command::detail::InputEnvironmentBinding;
  static constexpr auto value =
      object("input", &T::input, "environment", &T::environment);
};

template <> struct meta<dagforge::executors::command::detail::NodeConfig> {
  using T = dagforge::executors::command::detail::NodeConfig;
  static constexpr auto value =
      object("program", &T::program, "arguments", &T::arguments, "env", &T::env,
             "input_env", &T::input_env);
};

} // namespace glz

namespace dagforge::executors::command {
namespace {

using executors::detail::add_output;
using executors::detail::input_exists;

[[nodiscard]] auto parse_node_config(const JsonValue &config)
    -> Result<detail::NodeConfig> {
  return parse_json_as<detail::NodeConfig>(dump_json(config));
}

[[nodiscard]] auto encode_node_config(const detail::NodeConfig &config)
    -> Result<JsonValue> {
  auto encoded = serialize_json(config);
  if (!encoded) {
    return fail(encoded.error());
  }
  return parse_json(*encoded);
}

[[nodiscard]] auto value_to_string(const workflow::WorkflowValue &value)
    -> std::string {
  return std::visit(
      [](const auto &typed) -> std::string {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return {};
        } else if constexpr (std::same_as<T, bool>) {
          return typed ? "true" : "false";
        } else if constexpr (std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return std::format("{}", typed);
        } else if constexpr (std::same_as<T, std::string>) {
          return typed;
        } else if constexpr (std::same_as<T, JsonValue>) {
          return dump_json(typed);
        } else if constexpr (std::same_as<T, workflow::ArtifactRef>) {
          return typed.artifact_id.str();
        }
        return {};
      },
      value);
}

class CommandTaskExecutor final : public workflow::ITaskExecutor {
public:
  CommandTaskExecutor(std::unique_ptr<sandbox::ICommandRunner> runner,
                      std::shared_ptr<const sandbox::detail::CommandPolicy> policy)
      : runner_(std::move(runner)), policy_(std::move(policy)) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "command";
  }

  [[nodiscard]] auto compile(
      JsonValue config, workflow::ExecutorCompileContext context) const
      -> Result<JsonValue> override {
    auto parsed = parse_node_config(config);
    if (!parsed) {
      return fail(parsed.error());
    }
    auto canonical = policy_->canonical_program(parsed->program);
    if (!canonical) {
      return fail(canonical.error());
    }
    parsed->program = std::move(*canonical);
    std::unordered_set<std::string> environment;
    for (const auto &entry : parsed->env) {
      auto valid = policy_->validate_environment(entry.key, entry.value);
      if (!valid || !environment.emplace(entry.key).second) {
        return fail(valid ? Error::InvalidArgument : valid.error());
      }
    }
    for (const auto &binding : parsed->input_env) {
      auto valid = policy_->validate_environment_key(binding.environment);
      if (!valid) {
        return fail(valid.error());
      }
      if (binding.input.empty() || !input_exists(context, binding.input) ||
          !environment.emplace(binding.environment).second) {
        return fail(Error::InvalidArgument);
      }
    }

    return encode_node_config(*parsed);
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    auto parsed = parse_node_config(request.config);
    if (!parsed) {
      return fail(parsed.error());
    }

    sandbox::CommandSpec command{
        .program = std::move(parsed->program),
        .arguments = std::move(parsed->arguments),
    };
    for (auto &entry : parsed->env) {
      if (!command.environment
               .emplace(std::move(entry.key), std::move(entry.value))
               .second) {
        return fail(Error::InvalidArgument);
      }
    }
    for (const auto &binding : parsed->input_env) {
      const auto input = request.inputs.find(binding.input);
      if (input == request.inputs.end() ||
          !command.environment
               .emplace(binding.environment, value_to_string(*input->second))
               .second) {
        return fail(Error::InvalidArgument);
      }
    }

    auto on_complete = std::move(sink.on_complete);
    auto outputs = std::move(request.outputs);
    sandbox::CommandRunSink command_sink;
    command_sink.on_state = std::move(sink.on_state);
    command_sink.on_complete =
        [outputs = std::move(outputs),
         on_complete = std::move(on_complete)](
            const InstanceId &instance_id,
            sandbox::CommandRunResult result) mutable {
          if (!on_complete) {
            return;
          }
          if (result.timed_out) {
            on_complete(instance_id, fail(Error::Timeout));
            return;
          }
          if (result.resource_exhausted) {
            on_complete(instance_id, fail(Error::ResourceExhausted));
            return;
          }
          if (result.exit_code != 0) {
            on_complete(instance_id, fail(Error::Unknown));
            return;
          }

          workflow::ExecutorOutputs task_outputs;
          add_output(task_outputs, outputs, "stdout",
                     std::string{result.stdout_output});
          add_output(task_outputs, outputs, "stderr",
                     std::string{result.stderr_output});
          add_output(task_outputs, outputs, "exit_code",
                     static_cast<std::int64_t>(result.exit_code));
          add_output(task_outputs, outputs, "result",
                     std::string{result.stdout_output});
          on_complete(instance_id, ok(std::move(task_outputs)));
        };

    return runner_->start(
        sandbox::CommandRunRequest{
            .instance_id = std::move(request.instance_id),
            .execution_timeout = request.timeout,
            .command = std::move(command),
            .memory_resource = {}},
        std::move(command_sink));
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    runner_->cancel(instance_id);
  }

  auto quiesce(std::chrono::milliseconds timeout) -> Result<void> override {
    return runner_->quiesce(timeout);
  }

private:
  std::unique_ptr<sandbox::ICommandRunner> runner_;
  std::shared_ptr<const sandbox::detail::CommandPolicy> policy_;
};

} // namespace

namespace detail {

auto create_task_executor(std::unique_ptr<sandbox::ICommandRunner> runner,
                          const config::CommandPolicyConfig &policy_config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  if (!runner) {
    return fail(Error::InvalidArgument);
  }
  auto policy = sandbox::detail::CommandPolicy::create(policy_config);
  if (!policy) {
    return fail(policy.error());
  }
  return ok(std::shared_ptr<workflow::ITaskExecutor>{
      std::make_shared<CommandTaskExecutor>(
          std::move(runner), std::move(*policy))});
}

} // namespace detail

auto create_task_executor(Runtime &runtime,
                          const config::CommandExecutorConfig &config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  auto policy = sandbox::detail::CommandPolicy::create(config.policy);
  if (!policy) {
    return fail(policy.error());
  }
  auto runner = sandbox::detail::create_minijail_command_runner(
      runtime, config.minijail, *policy);
  if (!runner) {
    return fail(runner.error());
  }
  return ok(std::shared_ptr<workflow::ITaskExecutor>{
      std::make_shared<CommandTaskExecutor>(
          std::move(*runner), std::move(*policy))});
}

} // namespace dagforge::executors::command
