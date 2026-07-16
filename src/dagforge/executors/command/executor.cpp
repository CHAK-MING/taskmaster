#include "dagforge/executors/command/executor.hpp"

#include "dagforge/util/json.hpp"

#include "../../sandbox/detail/minijail_command_runner.hpp"
#include "../../sandbox/detail/policy_command_runner.hpp"
#include "../detail/task_executor_utils.hpp"
#include "detail/testing.hpp"

#include <array>
#include <cstdint>
#include <filesystem>
#include <format>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
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

struct CompiledCommand {
  sandbox::CommandSpec command;
  std::vector<InputEnvironmentBinding> input_env;
};

} // namespace dagforge::executors::command::detail

namespace dagforge::executors::command {
namespace {

using executors::detail::add_output;
using executors::detail::input_exists;
using executors::detail::outputs_supported;

inline constexpr std::array<std::string_view, 4> kSupportedOutputs{
    "stdout", "stderr", "exit_code", "result"};

[[nodiscard]] auto parse_node_config(const JsonPayload &config)
    -> Result<detail::NodeConfig> {
  return parse_json_as<detail::NodeConfig>(config.encoded());
}

[[nodiscard]] auto encode_node_config(const detail::NodeConfig &config)
    -> Result<JsonPayload> {
  return JsonPayload::from(config);
}

[[nodiscard]] auto command_failure(sandbox::CommandRunResult result)
    -> workflow::ExecutionFailure {
  auto details = JsonPayload::from(glz::obj{
      "exit_code", result.exit_code, "stdout", result.stdout_output, "stderr",
      result.stderr_output, "runner_error", result.error, "stdout_streamed",
      result.stdout_streamed, "stderr_streamed", result.stderr_streamed});
  if (!details) {
    return workflow::make_execution_failure(
        Error::ProtocolError, "command_failure_details_encode_failed",
        "Command failure diagnostics could not be encoded");
  }
  if (result.timed_out) {
    return workflow::make_execution_failure(
        Error::Timeout, "command_timed_out",
        result.error.empty() ? "Command execution timed out"
                             : std::string{result.error},
        std::move(*details));
  }
  if (result.resource_exhausted) {
    return workflow::make_execution_failure(
        Error::ResourceExhausted, "command_resource_exhausted",
        result.error.empty() ? "Command exceeded a configured resource limit"
                             : std::string{result.error},
        std::move(*details));
  }
  if (result.exit_code != 0) {
    return workflow::make_execution_failure(
        Error::Unknown,
        result.exit_code < 0 && !result.error.empty()
            ? "command_runner_failed"
            : "command_exit_nonzero",
        result.error.empty()
            ? std::format("Command exited with status {}", result.exit_code)
            : std::string{result.error},
        std::move(*details));
  }
  return workflow::make_execution_failure(
      Error::Unknown, "command_runner_failed",
      result.error.empty() ? "Command runner failed"
                           : std::string{result.error},
      std::move(*details));
}

class CommandTaskExecutor final : public workflow::ITaskExecutor {
public:
  explicit CommandTaskExecutor(
      std::unique_ptr<sandbox::ICommandRunner> runner)
      : runner_(std::move(runner)) {}

  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "command";
  }

  [[nodiscard]] auto compile(
      JsonPayload config, workflow::ExecutorCompileContext context) const
      -> Result<workflow::CompiledExecutorConfig> override {
    auto parsed = parse_node_config(config);
    if (!parsed) {
      return fail(parsed.error());
    }
    sandbox::CommandSpec command{
        .program = parsed->program,
        .arguments = parsed->arguments,
    };
    std::unordered_set<std::string> environment;
    for (const auto &entry : parsed->env) {
      if (!environment.emplace(entry.key).second ||
          !command.environment.emplace(entry.key, entry.value).second) {
        return fail(Error::InvalidArgument);
      }
    }
    std::vector<std::string> deferred_environment;
    deferred_environment.reserve(parsed->input_env.size());
    for (const auto &binding : parsed->input_env) {
      if (binding.input.empty() || !input_exists(context, binding.input) ||
          !environment.emplace(binding.environment).second) {
        return fail(Error::InvalidArgument);
      }
      deferred_environment.push_back(binding.environment);
    }
    if (!outputs_supported(context.outputs, kSupportedOutputs)) {
      return fail(Error::InvalidArgument);
    }

    auto prepared = runner_->prepare(sandbox::CommandPreparationRequest{
        .command = std::move(command),
        .deferred_environment_keys = std::move(deferred_environment),
    });
    if (!prepared) {
      return fail(prepared.error());
    }
    parsed->program = prepared->program;
    parsed->arguments = prepared->arguments;
    auto encoded = encode_node_config(*parsed);
    if (!encoded) {
      return fail(encoded.error());
    }
    return ok(workflow::CompiledExecutorConfig::make(
        std::move(*encoded),
        detail::CompiledCommand{
            .command = std::move(*prepared),
            .input_env = std::move(parsed->input_env),
        }));
  }

  auto start(workflow::TaskExecutionRequest request,
             workflow::TaskExecutionSink sink) -> Result<void> override {
    const auto *compiled = request.config.get<detail::CompiledCommand>();
    if (compiled == nullptr) {
      return fail(Error::InvalidState);
    }

    auto command = compiled->command;
    for (const auto &binding : compiled->input_env) {
      const auto input = request.inputs.find(binding.input);
      if (input == request.inputs.end() ||
          !command.environment
               .emplace(binding.environment,
                        workflow::workflow_value_text(*input->second))
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
          if (result.timed_out || result.resource_exhausted ||
              result.exit_code != 0 || !result.error.empty()) {
            on_complete(instance_id,
                        workflow::task_failed(command_failure(
                            std::move(result))));
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
          on_complete(instance_id,
                      workflow::task_succeeded(std::move(task_outputs)));
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
};

} // namespace

namespace detail {

auto create_task_executor(std::unique_ptr<sandbox::ICommandRunner> runner,
                          const config::CommandPolicyConfig &policy_config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  if (!runner) {
    return fail(Error::InvalidArgument);
  }
  auto protected_runner = sandbox::detail::create_policy_command_runner(
      std::move(runner), policy_config);
  if (!protected_runner) {
    return fail(protected_runner.error());
  }
  return ok(std::shared_ptr<workflow::ITaskExecutor>{
      std::make_shared<CommandTaskExecutor>(std::move(*protected_runner))});
}

} // namespace detail

auto create_task_executor(Runtime &runtime,
                          const config::CommandExecutorConfig &config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>> {
  auto runner = sandbox::detail::create_minijail_command_runner(
      runtime, config.minijail, config.policy);
  if (!runner) {
    return fail(runner.error());
  }
  return ok(std::shared_ptr<workflow::ITaskExecutor>{
      std::make_shared<CommandTaskExecutor>(std::move(*runner))});
}

} // namespace dagforge::executors::command
