#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/execution_failure.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <chrono>
#include <expected>
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

using ExecutorInputs =
    std::unordered_map<std::string, std::shared_ptr<const WorkflowValue>>;
using ExecutorOutputs = std::vector<std::pair<WorkflowPortId, WorkflowValue>>;
using TaskExecutionResult = std::expected<ExecutorOutputs, ExecutionFailure>;

[[nodiscard]] inline auto task_succeeded(ExecutorOutputs outputs)
    -> TaskExecutionResult {
  return outputs;
}

[[nodiscard]] inline auto task_failed(ExecutionFailure failure)
    -> TaskExecutionResult {
  return TaskExecutionResult{std::unexpect, std::move(failure)};
}

struct ExecutorCompileContext {
  std::span<const InputBinding> inputs;
  std::span<const WorkflowPortId> outputs;
};

struct TaskExecutionRequest {
  InstanceId instance_id;
  Principal principal;
  TraceContext trace;
  CompiledExecutorConfig config;
  ExecutorInputs inputs;
  std::vector<WorkflowPortId> outputs;
  std::chrono::seconds timeout{std::chrono::minutes(5)};
};

struct TaskExecutionSink {
  std::move_only_function<void(const InstanceId &, std::string_view)> on_state;
  std::move_only_function<void(const InstanceId &, TaskExecutionResult)>
      on_complete;
};

class ITaskExecutor {
public:
  virtual ~ITaskExecutor() = default;

  [[nodiscard]] virtual auto type() const noexcept -> std::string_view = 0;
  [[nodiscard]] virtual auto compile(JsonPayload config,
                                     ExecutorCompileContext context) const
      -> Result<CompiledExecutorConfig> = 0;
  virtual auto start(TaskExecutionRequest request, TaskExecutionSink sink)
      -> Result<void> = 0;
  virtual auto cancel(const InstanceId &instance_id) -> void = 0;
  virtual auto quiesce(std::chrono::milliseconds timeout) -> Result<void> = 0;
};

} // namespace dagforge::workflow
