#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"

namespace taskmaster {

class NoopExecutor : public IExecutor {
public:
  explicit NoopExecutor(Runtime& rt) : runtime_{&rt} {}

  ~NoopExecutor() override = default;

  auto start(ExecutorContext exec_ctx, ExecutorRequest req, ExecutionSink sink)
      -> void override {
    (void)exec_ctx;

    auto t = execute_task(std::move(req), std::move(sink));
    runtime_->schedule_external(t.take());
  }

  auto cancel(const InstanceId& instance_id) -> void override {
    (void)instance_id;
  }

private:
  auto execute_task(ExecutorRequest req, ExecutionSink sink) -> task<void> {
    co_await async_yield();

    ExecutorResult result;
    result.exit_code = 0;

    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
  }

  Runtime* runtime_;
};

auto create_noop_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<NoopExecutor>(rt);
}

}  // namespace taskmaster
