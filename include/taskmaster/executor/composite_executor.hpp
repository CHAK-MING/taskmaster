#pragma once

#include "taskmaster/executor/executor.hpp"

#include <memory>
#include <unordered_map>

namespace taskmaster {

class CompositeExecutor : public IExecutor {
public:
  CompositeExecutor() = default;
  ~CompositeExecutor() override = default;

  CompositeExecutor(const CompositeExecutor&) = delete;
  auto operator=(const CompositeExecutor&) -> CompositeExecutor& = delete;
  CompositeExecutor(CompositeExecutor&&) noexcept = default;
  auto operator=(CompositeExecutor&&) noexcept -> CompositeExecutor& = default;

  auto register_executor(ExecutorType type, std::unique_ptr<IExecutor> executor)
      -> void;

  auto start(ExecutorContext ctx, ExecutorRequest req, ExecutionSink sink)
      -> void override;

  auto cancel(const InstanceId& instance_id) -> void override;

private:
  std::unordered_map<ExecutorType, std::unique_ptr<IExecutor>> executors_;
  std::unordered_map<InstanceId, ExecutorType> instance_executor_map_;
  std::mutex mutex_;
};

[[nodiscard]] auto create_composite_executor(Runtime& rt)
    -> std::unique_ptr<IExecutor>;

}  // namespace taskmaster
