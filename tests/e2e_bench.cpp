#include <benchmark/benchmark.h>

#include <latch>
#include <string>

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"

namespace {

using namespace taskmaster;

void BM_E2E_TaskScheduling(benchmark::State& state) {
  const int num_tasks = static_cast<int>(state.range(0));

  const InstanceId instance_id{"0"};
  const ExecutorConfig config = ShellExecutorConfig{.command = "true", .working_dir = ""};

  Runtime runtime(16);
  runtime.start();
  auto executor = create_noop_executor(runtime);

  for (auto _ : state) {
    std::latch completion_latch(num_tasks);

    for (int i = 0; i < num_tasks; ++i) {
      ExecutorRequest req;
      req.instance_id = instance_id;
      req.config = config;

      ExecutionSink sink;
      sink.on_complete = [&](const InstanceId&, ExecutorResult) {
        completion_latch.count_down();
      };

      executor->start({.runtime = runtime}, std::move(req), std::move(sink));
    }

    completion_latch.wait();
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations() * num_tasks);
  state.counters["tasks"] = num_tasks;
}

BENCHMARK(BM_E2E_TaskScheduling)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);

void BM_E2E_TaskLatency(benchmark::State& state) {
  const ExecutorConfig config = ShellExecutorConfig{.command = "true", .working_dir = ""};

  Runtime runtime(16);
  runtime.start();
  auto executor = create_noop_executor(runtime);

  for (auto _ : state) {
    std::latch done(1);

    ExecutorRequest req;
    req.instance_id = InstanceId{"latency-test"};
    req.config = config;

    ExecutionSink sink;
    sink.on_complete = [&](const InstanceId&, ExecutorResult) {
      done.count_down();
    };

    executor->start({.runtime = runtime}, std::move(req), std::move(sink));
    done.wait();
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_E2E_TaskLatency)->Unit(benchmark::kNanosecond);

}  // namespace
