#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/command_spec.hpp"
#include "dagforge/util/id.hpp"
#endif

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <boost/asio/async_result.hpp>

namespace dagforge {

using CommandHeartbeatCallback =
    std::move_only_function<void(const InstanceId &instance_id)>;

struct CommandExecutionResult {
  int exit_code{0};
  pmr::string stdout_output{current_memory_resource_or_default()};
  pmr::string stderr_output{current_memory_resource_or_default()};
  pmr::string error{current_memory_resource_or_default()};
  bool timed_out{false};
  bool stdout_streamed{false};
  bool stderr_streamed{false};
};

[[nodiscard]] inline auto make_command_execution_result(
    pmr::memory_resource *resource = current_memory_resource_or_default())
    -> CommandExecutionResult {
  CommandExecutionResult result;
  result.stdout_output = pmr::string(resource);
  result.stderr_output = pmr::string(resource);
  result.error = pmr::string(resource);
  return result;
}

struct CommandExecutionRequest {
  InstanceId instance_id;
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  CommandSpec command;
  std::shared_ptr<pmr::memory_resource> memory_resource;

  [[nodiscard]] auto resource() const noexcept -> pmr::memory_resource * {
    return memory_resource != nullptr ? memory_resource.get()
                                      : current_memory_resource_or_default();
  }
};

struct CommandExecutionSink {
  CommandHeartbeatCallback on_heartbeat;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view message)>
      on_state;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view data)>
      on_stdout;
  std::move_only_function<void(const InstanceId &instance_id,
                               std::string_view data)>
      on_stderr;
  std::move_only_function<void(const InstanceId &instance_id,
                               CommandExecutionResult result)>
      on_complete;
};

class ICommandExecutor;

[[nodiscard]] auto create_command_executor(Runtime &runtime,
                                           SandboxConfig sandbox)
    -> std::unique_ptr<ICommandExecutor>;

class ICommandExecutor {
public:
  virtual ~ICommandExecutor() = default;

  virtual auto start(CommandExecutionRequest request,
                     CommandExecutionSink sink)
      -> Result<void> = 0;

  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
};

inline auto execute_command_async(
    ICommandExecutor &executor, CommandExecutionRequest request,
    std::shared_ptr<pmr::memory_resource> memory_resource = {},
    std::move_only_function<void(std::string_view)> on_stdout = {},
    std::move_only_function<void(std::string_view)> on_stderr = {},
    CommandHeartbeatCallback on_heartbeat = {},
    std::move_only_function<void(std::string_view)> on_state = {})
    -> task<Result<CommandExecutionResult>> {
  request.memory_resource = std::move(memory_resource);

  return boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                     void(Result<CommandExecutionResult>)>(
      [&executor, request = std::move(request),
       on_stdout = std::move(on_stdout),
       on_stderr = std::move(on_stderr),
       on_heartbeat = std::move(on_heartbeat),
       on_state = std::move(on_state)](auto handler) mutable {
        CommandExecutionSink sink;
        // Capture handler by shared_ptr so we can call it on start failure too.
        auto shared_h = std::make_shared<decltype(handler)>(std::move(handler));
        if (on_stdout) {
          sink.on_stdout =
              [cb = std::move(on_stdout)](const InstanceId &,
                                          std::string_view data) mutable {
                cb(data);
              };
        }
        if (on_stderr) {
          sink.on_stderr =
              [cb = std::move(on_stderr)](const InstanceId &,
                                          std::string_view data) mutable {
                cb(data);
              };
        }
        if (on_heartbeat) {
          sink.on_heartbeat =
              [cb = std::move(on_heartbeat)](const InstanceId &id) mutable {
                cb(id);
              };
        }
        if (on_state) {
          sink.on_state =
              [cb = std::move(on_state)](const InstanceId &,
                                         std::string_view state) mutable {
                cb(state);
              };
        }
        sink.on_complete = [shared_h](const InstanceId &,
                                      CommandExecutionResult result) mutable {
          std::move(*shared_h)(ok(std::move(result)));
        };

        auto start_res =
            executor.start(std::move(request), std::move(sink));
        if (!start_res) {
          std::move(*shared_h)(fail(start_res.error()));
        }
      },
      boost::asio::use_awaitable);
}

inline auto execute_command_async(
    ICommandExecutor &executor, InstanceId instance_id,
    CommandSpec command,
    std::shared_ptr<pmr::memory_resource> memory_resource = {},
    std::move_only_function<void(std::string_view)> on_stdout = {},
    std::move_only_function<void(std::string_view)> on_stderr = {},
    CommandHeartbeatCallback on_heartbeat = {},
    std::chrono::seconds execution_timeout = std::chrono::seconds(3600),
    std::move_only_function<void(std::string_view)> on_state = {})
    -> task<Result<CommandExecutionResult>> {
  return execute_command_async(
      executor,
      CommandExecutionRequest{.instance_id = std::move(instance_id),
                              .execution_timeout = execution_timeout,
                              .command = std::move(command),
                              .memory_resource = {}},
      std::move(memory_resource), std::move(on_stdout), std::move(on_stderr),
      std::move(on_heartbeat), std::move(on_state));
}

} // namespace dagforge
