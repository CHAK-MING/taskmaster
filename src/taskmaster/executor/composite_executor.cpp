#include "taskmaster/executor/composite_executor.hpp"

#include "taskmaster/executor/docker_executor.hpp"
#include "taskmaster/util/log.hpp"

namespace taskmaster {

auto CompositeExecutor::register_executor(ExecutorType type,
                                          std::unique_ptr<IExecutor> executor)
    -> void {
  executors_[type] = std::move(executor);
}

auto CompositeExecutor::start(ExecutorContext ctx, ExecutorRequest req,
                              ExecutionSink sink) -> void {
  ExecutorType type = ExecutorType::Shell;

  if (std::holds_alternative<ShellExecutorConfig>(req.config)) {
    type = ExecutorType::Shell;
  } else if (std::holds_alternative<DockerExecutorConfig>(req.config)) {
    type = ExecutorType::Docker;
  }

  auto it = executors_.find(type);
  if (it == executors_.end()) {
    log::error("CompositeExecutor: no executor registered for type {}",
               to_string_view(type));
    ExecutorResult result;
    result.exit_code = -1;
    result.error = "No executor available for requested type";
    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
    return;
  }

  InstanceId instance_id = req.instance_id;
  {
    std::lock_guard lock(mutex_);
    instance_executor_map_[instance_id] = type;
  }

  // Wrap on_complete to cleanup instance_executor_map_ when task completes
  auto original_on_complete = std::move(sink.on_complete);
  sink.on_complete = [this, instance_id,
                      on_complete = std::move(original_on_complete)](
                         const InstanceId& id, ExecutorResult result) mutable {
    {
      std::lock_guard lock(mutex_);
      instance_executor_map_.erase(instance_id);
    }
    if (on_complete) {
      on_complete(id, std::move(result));
    }
  };

  it->second->start(ctx, std::move(req), std::move(sink));
}

auto CompositeExecutor::cancel(const InstanceId& instance_id) -> void {
  ExecutorType type;
  {
    std::lock_guard lock(mutex_);
    auto it = instance_executor_map_.find(instance_id);
    if (it == instance_executor_map_.end()) {
      log::warn("CompositeExecutor: no executor mapping for instance {}",
                instance_id);
      return;
    }
    type = it->second;
    instance_executor_map_.erase(it);
  }

  auto exec_it = executors_.find(type);
  if (exec_it != executors_.end()) {
    exec_it->second->cancel(instance_id);
  }
}

auto create_composite_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  auto composite = std::make_unique<CompositeExecutor>();
  composite->register_executor(ExecutorType::Shell, create_shell_executor(rt));
  composite->register_executor(ExecutorType::Docker, create_docker_executor(rt));
  composite->register_executor(ExecutorType::Sensor, create_sensor_executor(rt));
  return composite;
}

}  // namespace taskmaster
