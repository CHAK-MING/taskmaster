#include "dagforge/executor/executor.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/steady_timer.hpp>

#include <chrono>
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

    auto ex = runtime_.io().get_executor();
    boost::asio::co_spawn(
        ex,
        [req = std::move(req), sink = std::move(sink), ex,
         exit_code = cfg->exit_code]() mutable -> spawn_task {
          if (sink.on_state) {
            sink.on_state(req.instance_id, "started");
          }

          // Replace this timer with the real async operation.
          boost::asio::steady_timer timer(ex);
          timer.expires_after(std::chrono::milliseconds(1));
          auto wait_res =
              co_await co_as_result(timer.async_wait(dagforge::use_nothrow));
          if (!wait_res) {
            log::debug("sample executor wait failed: {}",
                       wait_res.error().message());
          }

          auto result = make_executor_result(req.resource());
          result.exit_code = exit_code;

          if (sink.on_complete) {
            sink.on_complete(req.instance_id, std::move(result));
          }
        },
        detached);

    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    log::debug("sample executor cancel: {}", instance_id);
  }

private:
  Runtime &runtime_;
};

} // namespace

auto create_sample_executor(Runtime &rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<SampleExecutor>(rt);
}

} // namespace dagforge
