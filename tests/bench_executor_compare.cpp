// bench_executor_compare.cpp
// Compare executor overhead for noop and shell against the Lua baseline.

#include "bench_utils.hpp"

#include "dagforge/executor/executor.hpp"

#include "benchmark_compat.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <latch>
#include <memory>
#include <string>
#include <utility>

namespace dagforge {
namespace {

class ExecutorBenchBase {
public:
  explicit ExecutorBenchBase(
      std::move_only_function<std::unique_ptr<IExecutor>(Runtime &)> creator)
      : runtime_(1) {
    executor_ = creator(runtime_.runtime);
    if (executor_ == nullptr) {
      throw std::runtime_error("failed to create executor");
    }
  }

  [[nodiscard]] auto run(ExecutorRequest req) -> ExecutorResult {
    std::latch done{1};
    ExecutorResult result;

    ExecutionSink sink;
    sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
      result = std::move(r);
      done.count_down();
    };

    const auto start_res = executor_->start(std::move(req), std::move(sink));
    if (!start_res) {
      throw std::runtime_error(start_res.error().message());
    }

    done.wait();
    return result;
  }

protected:
  bench::RuntimeGuard runtime_;
  std::unique_ptr<IExecutor> executor_;
};

class NoopExecutorBench final : public ExecutorBenchBase {
public:
  NoopExecutorBench() : ExecutorBenchBase(create_noop_executor) {}
};

class ShellExecutorBench final : public ExecutorBenchBase {
public:
  ShellExecutorBench() : ExecutorBenchBase(create_shell_executor) {}
};

[[nodiscard]] auto make_noop_request() -> ExecutorRequest {
  ExecutorRequest req;
  req.instance_id = InstanceId{"bench_noop"};
  req.execution_timeout = std::chrono::seconds(5);
  req.config = NoopExecutorConfig{.exit_code = 0};
  return req;
}

[[nodiscard]] auto make_shell_request() -> ExecutorRequest {
  ExecutorRequest req;
  req.instance_id = InstanceId{"bench_shell"};
  req.command = "true";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = ShellExecutorConfig{};
  return req;
}

[[nodiscard]] auto make_shell_sleep_request() -> ExecutorRequest {
  ExecutorRequest req;
  req.instance_id = InstanceId{"bench_shell_sleep"};
  req.command = "sleep 1";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = ShellExecutorConfig{};
  return req;
}

void BM_NoopExecutorImmediate(benchmark::State &state) {
  NoopExecutorBench bench;
  const auto req = make_noop_request();

  for (auto _ : state) {
    auto result = bench.run(req);
    benchmark::DoNotOptimize(result.exit_code);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_ShellExecutorTrue(benchmark::State &state) {
  ShellExecutorBench bench;
  const auto req = make_shell_request();

  for (auto _ : state) {
    auto result = bench.run(req);
    benchmark::DoNotOptimize(result.exit_code);
    benchmark::DoNotOptimize(result.stdout_output);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_ShellExecutorSleep(benchmark::State &state) {
  ShellExecutorBench bench;
  const auto req = make_shell_sleep_request();

  for (auto _ : state) {
    auto result = bench.run(req);
    benchmark::DoNotOptimize(result.exit_code);
    benchmark::DoNotOptimize(result.stdout_output);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

BENCHMARK(BM_NoopExecutorImmediate)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ShellExecutorTrue)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ShellExecutorSleep)->Unit(benchmark::kMillisecond);

} // namespace
} // namespace dagforge
