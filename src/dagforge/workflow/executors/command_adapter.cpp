#include "dagforge/workflow/executors/command_adapter.hpp"

#include "dagforge/executor/command_executor.hpp"
#include "../../executor/detail/command_validation.hpp"
#include "dagforge/util/json.hpp"

#include "detail/adapter_utils.hpp"

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

namespace dagforge::detail {

struct CommandEnvironmentEntry {
  std::string key;
  std::string value;
};

struct CommandInputEnvironmentBinding {
  std::string input;
  std::string environment;
};

struct CommandNodeConfig {
  std::string program;
  std::vector<std::string> arguments;
  std::vector<CommandEnvironmentEntry> env;
  std::vector<CommandInputEnvironmentBinding> input_env;
};

} // namespace dagforge::detail

namespace glz {

template <> struct meta<dagforge::detail::CommandEnvironmentEntry> {
  using T = dagforge::detail::CommandEnvironmentEntry;
  static constexpr auto value = object("key", &T::key, "value", &T::value);
};

template <> struct meta<dagforge::detail::CommandInputEnvironmentBinding> {
  using T = dagforge::detail::CommandInputEnvironmentBinding;
  static constexpr auto value = object(
      "input", &T::input, "environment", &T::environment);
};

template <> struct meta<dagforge::detail::CommandNodeConfig> {
  using T = dagforge::detail::CommandNodeConfig;
  static constexpr auto value = object(
      "program", &T::program, "arguments", &T::arguments, "env", &T::env,
      "input_env", &T::input_env);
};

} // namespace glz

namespace dagforge {
namespace {

using workflow::executor_detail::add_output;
using workflow::executor_detail::input_exists;

[[nodiscard]] auto allowed(std::span<const std::string> values,
                           std::string_view value) -> bool {
  return std::ranges::find(values, value) != values.end();
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

class CommandWorkflowAdapter final : public workflow::ITaskExecutor {
public:
  CommandWorkflowAdapter(ICommandExecutor &command_executor,
                         SandboxConfig sandbox)
      : command_executor_(command_executor), sandbox_(std::move(sandbox)) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "command";
  }

  [[nodiscard]] auto compile(
      JsonValue config, workflow::ExecutorCompileContext context) const
      -> Result<JsonValue> override {
    auto parsed = parse_json_as<detail::CommandNodeConfig>(dump_json(config));
    if (!parsed || parsed->program.empty() ||
        !std::filesystem::path(parsed->program).is_absolute()) {
      return fail(Error::InvalidArgument);
    }
    if (!sandbox_.allow_unlisted_programs &&
        !allowed(sandbox_.allowed_programs, parsed->program)) {
      return fail(Error::Unauthorized);
    }

    std::unordered_set<std::string> environment;
    for (const auto &entry : parsed->env) {
      if (!executor_detail::is_valid_environment_key(entry.key) ||
          !environment.emplace(entry.key).second) {
        return fail(Error::InvalidArgument);
      }
      if (!sandbox_.allow_unlisted_environment &&
          !allowed(sandbox_.allowed_environment, entry.key)) {
        return fail(Error::Unauthorized);
      }
    }
    for (const auto &binding : parsed->input_env) {
      if (binding.input.empty() || !input_exists(context, binding.input) ||
          !executor_detail::is_valid_environment_key(binding.environment) ||
          !environment.emplace(binding.environment).second) {
        return fail(Error::InvalidArgument);
      }
      if (!sandbox_.allow_unlisted_environment &&
          !allowed(sandbox_.allowed_environment, binding.environment)) {
        return fail(Error::Unauthorized);
      }
    }

    auto encoded = serialize_json(*parsed);
    if (!encoded) {
      return fail(encoded.error());
    }
    return parse_json(*encoded);
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    auto parsed =
        parse_json_as<detail::CommandNodeConfig>(dump_json(request.config));
    if (!parsed) {
      return fail(parsed.error());
    }

    CommandSpec command{
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
    CommandExecutionSink command_sink;
    command_sink.on_state = std::move(sink.on_state);
    command_sink.on_complete =
        [outputs = std::move(outputs),
         on_complete = std::move(on_complete)](
            const InstanceId &instance_id,
            CommandExecutionResult result) mutable {
          if (!on_complete) {
            return;
          }
          if (result.timed_out) {
            on_complete(instance_id, fail(Error::Timeout));
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

    return command_executor_.start(
        CommandExecutionRequest{
            .instance_id = std::move(request.instance_id),
            .execution_timeout = request.timeout,
            .command = std::move(command),
            .memory_resource = {}},
        std::move(command_sink));
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    command_executor_.cancel(instance_id);
  }

private:
  ICommandExecutor &command_executor_;
  SandboxConfig sandbox_;
};

} // namespace

namespace workflow {

auto create_command_executor_adapter(ICommandExecutor &command_executor,
                                     SandboxConfig sandbox)
    -> std::shared_ptr<ITaskExecutor> {
  return std::make_shared<CommandWorkflowAdapter>(command_executor,
                                                   std::move(sandbox));
}

} // namespace workflow

} // namespace dagforge
