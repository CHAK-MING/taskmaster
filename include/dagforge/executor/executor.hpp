#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/executor/executor_types.hpp"
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

class ISandboxBackend;

using ExecutorHeartbeatCallback =
    std::move_only_function<void(const InstanceId &instance_id)>;

struct ExecutorResult {
  int exit_code{0};
  pmr::string stdout_output{current_memory_resource_or_default()};
  pmr::string stderr_output{current_memory_resource_or_default()};
  pmr::string error{current_memory_resource_or_default()};
  bool timed_out{false};
  bool stdout_streamed{false};
  bool stderr_streamed{false};
};

[[nodiscard]] inline auto make_executor_result(
    pmr::memory_resource *resource = current_memory_resource_or_default())
    -> ExecutorResult {
  ExecutorResult result;
  result.stdout_output = pmr::string(resource);
  result.stderr_output = pmr::string(resource);
  result.error = pmr::string(resource);
  return result;
}

struct ExecutorRequest {
  InstanceId instance_id;
  std::chrono::seconds execution_timeout{std::chrono::seconds(3600)};
  CommandExecutorConfig command;
  std::shared_ptr<pmr::memory_resource> memory_resource;

  [[nodiscard]] auto resource() const noexcept -> pmr::memory_resource * {
    return memory_resource != nullptr ? memory_resource.get()
                                      : current_memory_resource_or_default();
  }
};

struct ExecutionSink {
  ExecutorHeartbeatCallback on_heartbeat;
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
                               ExecutorResult result)>
      on_complete;
};

class IExecutor;

[[nodiscard]] auto create_command_executor(Runtime &runtime,
                                           SandboxConfig sandbox)
    -> std::unique_ptr<IExecutor>;
[[nodiscard]] auto create_command_executor(
    std::unique_ptr<ISandboxBackend> sandbox_backend)
    -> std::unique_ptr<IExecutor>;

class IExecutor {
public:
  virtual ~IExecutor() = default;

  virtual auto start(ExecutorRequest req, ExecutionSink sink)
      -> Result<void> = 0;

  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
};

inline auto execute_async(Runtime & /*runtime*/, IExecutor &executor,
                          ExecutorRequest req,
                          std::shared_ptr<pmr::memory_resource>
                              memory_resource = {},
                          std::move_only_function<void(std::string_view)>
                              on_stdout = {},
                          std::move_only_function<void(std::string_view)>
                              on_stderr = {},
                          ExecutorHeartbeatCallback on_heartbeat = {},
                          std::move_only_function<void(std::string_view)>
                              on_state = {})
    -> task<Result<ExecutorResult>> {
  req.memory_resource = std::move(memory_resource);

  return boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                     void(Result<ExecutorResult>)>(
      [&executor, req = std::move(req), on_stdout = std::move(on_stdout),
       on_stderr = std::move(on_stderr),
       on_heartbeat = std::move(on_heartbeat),
       on_state = std::move(on_state)](auto handler) mutable {
        ExecutionSink sink;
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
                                      ExecutorResult res) mutable {
          std::move(*shared_h)(ok(std::move(res)));
        };

        auto start_res = executor.start(std::move(req), std::move(sink));
        if (!start_res) {
          std::move(*shared_h)(fail(start_res.error()));
        }
      },
      boost::asio::use_awaitable);
}

inline auto execute_async(Runtime &runtime, IExecutor &executor,
                          InstanceId instance_id, CommandExecutorConfig command,
                          std::shared_ptr<pmr::memory_resource>
                              memory_resource = {},
                          std::move_only_function<void(std::string_view)>
                              on_stdout = {},
                          std::move_only_function<void(std::string_view)>
                              on_stderr = {},
                          ExecutorHeartbeatCallback on_heartbeat = {},
                          std::chrono::seconds execution_timeout =
                              std::chrono::seconds(3600),
                          std::move_only_function<void(std::string_view)>
                              on_state = {})
    -> task<Result<ExecutorResult>> {
  return execute_async(runtime, executor,
                       ExecutorRequest{.instance_id = std::move(instance_id),
                                       .execution_timeout = execution_timeout,
                                       .command = std::move(command),
                                       .memory_resource = {}},
                       std::move(memory_resource), std::move(on_stdout),
                       std::move(on_stderr), std::move(on_heartbeat),
                       std::move(on_state));
}

} // namespace dagforge
