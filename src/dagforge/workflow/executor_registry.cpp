#include "dagforge/workflow/executor_registry.hpp"

#include <string>
#include <utility>

namespace dagforge::workflow {

auto ExecutorRegistry::register_executor(
    std::shared_ptr<ITaskExecutor> executor) -> Result<void> {
  if (!executor) {
    return fail(Error::InvalidArgument);
  }
  const auto type = executor->type();
  if (type.empty()) {
    return fail(Error::InvalidArgument);
  }
  if (!executors_.emplace(std::string{type}, std::move(executor)).second) {
    return fail(Error::AlreadyExists);
  }
  return ok();
}

auto ExecutorRegistry::compile(std::string_view type, JsonValue config,
                               ExecutorCompileContext context) const
    -> Result<JsonValue> {
  const auto executor = executors_.find(std::string{type});
  if (executor == executors_.end()) {
    return fail(Error::Unsupported);
  }
  return executor->second->compile(std::move(config), context);
}

auto ExecutorRegistry::start(std::string_view type,
                             TaskExecutionRequest request,
                             TaskExecutionSink sink) -> Result<void> {
  const auto executor = executors_.find(std::string{type});
  if (executor == executors_.end()) {
    return fail(Error::Unsupported);
  }
  return executor->second->start(std::move(request), std::move(sink));
}

auto ExecutorRegistry::cancel(std::string_view type,
                              const InstanceId &instance_id) -> void {
  const auto executor = executors_.find(std::string{type});
  if (executor != executors_.end()) {
    executor->second->cancel(instance_id);
  }
}

auto ExecutorRegistry::contains(std::string_view type) const -> bool {
  return executors_.contains(std::string{type});
}

} // namespace dagforge::workflow
