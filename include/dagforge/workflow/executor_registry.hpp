#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include <boost/asio/async_result.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

class ExecutorRegistry {
public:
  [[nodiscard]] auto register_executor(std::shared_ptr<ITaskExecutor> executor)
      -> Result<void>;
  [[nodiscard]] auto compile(std::string_view type, JsonValue config,
                             ExecutorCompileContext context) const
      -> Result<JsonValue>;
  auto start(std::string_view type, TaskExecutionRequest request,
             TaskExecutionSink sink) -> Result<void>;
  auto cancel(std::string_view type, const InstanceId &instance_id) -> void;
  [[nodiscard]] auto quiesce(std::chrono::milliseconds timeout)
      -> Result<void>;
  [[nodiscard]] auto contains(std::string_view type) const -> bool;

private:
  std::unordered_map<std::string, std::shared_ptr<ITaskExecutor>> executors_;
  std::atomic_bool quiescing_{false};
};

inline auto execute_task_async(
    Runtime &runtime, shard_id owner, ExecutorRegistry &registry,
    std::string executor_type,
    TaskExecutionRequest request,
    std::move_only_function<void(std::string_view)> on_state = {})
    -> task<TaskExecutionResult> {
  return boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                     void(TaskExecutionResult)>(
      [&runtime, owner, &registry, executor_type = std::move(executor_type),
       request = std::move(request),
       on_state = std::move(on_state)](auto handler) mutable {
        auto shared_handler =
            std::make_shared<decltype(handler)>(std::move(handler));
        auto completed = std::make_shared<std::atomic_bool>(false);
        auto complete =
            [&runtime, owner, shared_handler,
             completed](TaskExecutionResult result) mutable {
              if (completed->exchange(true, std::memory_order_acq_rel)) {
                return;
              }
              runtime.post_to(
                  owner,
                  [shared_handler, result = std::move(result)]() mutable {
                    std::move(*shared_handler)(std::move(result));
                  });
            };

        TaskExecutionSink sink;
        if (on_state) {
          sink.on_state =
              [callback = std::move(on_state)](const InstanceId &,
                                                std::string_view state) mutable {
                callback(state);
              };
        }
        sink.on_complete =
            [complete](const InstanceId &,
                       TaskExecutionResult result) mutable {
              complete(std::move(result));
            };
        auto started = registry.start(executor_type, std::move(request),
                                      std::move(sink));
        if (!started) {
          JsonValue details = JsonValue::object_t{};
          details["executor"] = executor_type;
          complete(task_failed(make_execution_failure(
              started.error(), "executor_start_failed",
              "Task executor rejected the start request",
              std::move(details))));
        }
      },
      boost::asio::use_awaitable);
}

} // namespace dagforge::workflow
