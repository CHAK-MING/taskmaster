#include "dagforge/workflow/executor_registry.hpp"

#include <chrono>
#include <string>
#include <utility>

namespace dagforge::workflow {

auto ExecutorRegistry::register_executor(
    std::shared_ptr<ITaskExecutor> executor) -> Result<void> {
  if (!executor) {
    return fail(Error::InvalidArgument);
  }
  if (quiescing_.load(std::memory_order_acquire)) {
    return fail(Error::InvalidState);
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

auto ExecutorRegistry::compile(std::string_view type, JsonPayload config,
                               ExecutorCompileContext context) const
    -> Result<CompiledExecutorConfig> {
  const auto executor = executors_.find(std::string{type});
  if (executor == executors_.end()) {
    return fail(Error::Unsupported);
  }
  return executor->second->compile(std::move(config), context);
}

auto ExecutorRegistry::start(std::string_view type,
                             TaskExecutionRequest request,
                             TaskExecutionSink sink) -> Result<void> {
  if (quiescing_.load(std::memory_order_acquire)) {
    return fail(Error::InvalidState);
  }
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

auto ExecutorRegistry::quiesce(std::chrono::milliseconds timeout)
    -> Result<void> {
  quiescing_.store(true, std::memory_order_release);
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  std::error_code first_error;
  for (const auto &[_, executor] : executors_) {
    const auto now = std::chrono::steady_clock::now();
    const auto remaining =
        now < deadline
            ? std::chrono::duration_cast<std::chrono::milliseconds>(deadline -
                                                                     now)
            : std::chrono::milliseconds::zero();
    auto result = executor->quiesce(remaining);
    if (!result && !first_error) {
      first_error = result.error();
    }
  }
  return first_error ? fail(first_error) : ok();
}

auto ExecutorRegistry::contains(std::string_view type) const -> bool {
  return executors_.contains(std::string{type});
}

} // namespace dagforge::workflow
