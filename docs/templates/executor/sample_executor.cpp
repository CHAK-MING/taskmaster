#include "dagforge/executor/executor.hpp"

#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/util/log.hpp"

#include <chrono>
#include <exception>
#include <memory>

namespace dagforge {

namespace {

class SampleExecutor final : public IExecutor {
public:
  explicit SampleExecutor(Runtime &rt) : runtime_(rt) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto *cfg = req.config.as<NoopExecutorConfig>();
    if (cfg == nullptr) {
      return fail(Error::InvalidArgument);
    }

    if (!runtime_.is_running()) {
      return fail(Error::SystemNotRunning);
    }

    const auto exit_code = cfg->exit_code;
    runtime_.spawn(execute(std::move(req), std::move(sink), exit_code));
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    log::debug("sample executor cancel: {}", instance_id);
  }

private:
  static auto execute(ExecutorRequest req, ExecutionSink sink, int exit_code)
      -> spawn_task {
    if (sink.on_state) {
      sink.on_state(req.instance_id, "started");
    }

    try {
      // Replace this delay with the real async operation.
      co_await async_sleep_on_timing_wheel(std::chrono::milliseconds(1));
    } catch (const std::exception &e) {
      log::error("sample executor async operation failed: {}", e.what());
      auto result = make_executor_result(req.resource());
      result.exit_code = 1;
      if (sink.on_complete) {
        sink.on_complete(req.instance_id, std::move(result));
      }
      co_return;
    }

    auto result = make_executor_result(req.resource());
    result.exit_code = exit_code;
    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
  }

  Runtime &runtime_;
};

} // namespace

auto create_sample_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<SampleExecutor>(rt);
}

} // namespace dagforge
