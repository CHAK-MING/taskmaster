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

void BM_E2E_TaskScheduling_Batch(benchmark::State& state) {
  const int num_tasks = static_cast<int>(state.range(0));

  Runtime runtime(16);
  runtime.start();

  for (auto _ : state) {
    std::atomic<int> completed{0};
    std::vector<task<void>> tasks;
    tasks.reserve(num_tasks);

    for (int i = 0; i < num_tasks; ++i) {
      tasks.push_back([](std::atomic<int>& counter) -> task<void> {
        co_await async_yield();
        counter.fetch_add(1, std::memory_order_relaxed);
      }(completed));
    }

    std::vector<std::coroutine_handle<>> handles;
    handles.reserve(num_tasks);
    for (auto& t : tasks) {
      handles.push_back(t.take());
    }

    runtime.schedule_external_batch(handles);
    
    while (completed.load(std::memory_order_acquire) < num_tasks) {
      std::this_thread::yield();
    }
  }

  runtime.stop();

  state.SetItemsProcessed(state.iterations() * num_tasks);
  state.counters["tasks"] = num_tasks;
}

BENCHMARK(BM_E2E_TaskScheduling_Batch)
    ->Arg(1000)
    ->Arg(10000)
    ->Arg(100000)
    ->Unit(benchmark::kMillisecond);

}  // namespace
