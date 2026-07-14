#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <boost/asio/async_result.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

using ExecutorInputs = std::unordered_map<
    std::string, std::shared_ptr<const WorkflowValue>>;
using ExecutorOutputs =
    std::vector<std::pair<WorkflowPortId, WorkflowValue>>;

struct ExecutorCompileContext {
  std::span<const InputBinding> inputs;
  std::span<const WorkflowPortId> outputs;
};

struct TaskExecutionRequest {
  InstanceId instance_id;
  JsonValue config{JsonValue::object_t{}};
  ExecutorInputs inputs;
  std::vector<WorkflowPortId> outputs;
  std::chrono::seconds timeout{std::chrono::minutes(5)};
};

struct TaskExecutionSink {
  std::move_only_function<void(const InstanceId &, std::string_view)> on_state;
  std::move_only_function<void(const InstanceId &,
                               Result<ExecutorOutputs>)>
      on_complete;
};

class ITaskExecutor {
public:
  virtual ~ITaskExecutor() = default;

  [[nodiscard]] virtual auto type() const noexcept -> std::string_view = 0;
  [[nodiscard]] virtual auto compile(JsonValue config,
                                     ExecutorCompileContext context) const
      -> Result<JsonValue> = 0;
  virtual auto start(TaskExecutionRequest request, TaskExecutionSink sink)
      -> Result<void> = 0;
  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
};

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
  [[nodiscard]] auto contains(std::string_view type) const -> bool;

private:
  std::unordered_map<std::string, std::shared_ptr<ITaskExecutor>> executors_;
};

inline auto execute_task_async(
    Runtime &runtime, shard_id owner, ExecutorRegistry &registry,
    std::string executor_type,
    TaskExecutionRequest request,
    std::move_only_function<void(std::string_view)> on_state = {})
    -> task<Result<ExecutorOutputs>> {
  return boost::asio::async_initiate<const boost::asio::use_awaitable_t<>,
                                     void(Result<ExecutorOutputs>)>(
      [&runtime, owner, &registry, executor_type = std::move(executor_type),
       request = std::move(request),
       on_state = std::move(on_state)](auto handler) mutable {
        auto shared_handler =
            std::make_shared<decltype(handler)>(std::move(handler));
        auto completed = std::make_shared<std::atomic_bool>(false);
        auto complete =
            [&runtime, owner, shared_handler,
             completed](Result<ExecutorOutputs> result) mutable {
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
                       Result<ExecutorOutputs> result) mutable {
              complete(std::move(result));
            };
        auto started = registry.start(executor_type, std::move(request),
                                      std::move(sink));
        if (!started) {
          complete(fail(started.error()));
        }
      },
      boost::asio::use_awaitable);
}

} // namespace dagforge::workflow
