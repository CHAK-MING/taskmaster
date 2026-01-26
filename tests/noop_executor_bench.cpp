#include <benchmark/benchmark.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"

namespace {

using namespace taskmaster;

[[nodiscard]] auto to_positive_int(long v, int fallback) -> int {
  if (v <= 0) return fallback;
  if (v > std::numeric_limits<int>::max()) return fallback;
  return static_cast<int>(v);
}

// Per-shard counter to reduce atomic contention
struct alignas(64) PerShardCounter {
  std::atomic<long> count{0};
};

struct BenchState {
  std::vector<PerShardCounter> per_shard_counts;
  std::atomic<bool> keep_running{true};
  std::vector<InstanceId> instance_ids;  // Pre-allocated IDs
  
  explicit BenchState(int num_shards, int num_tasks) 
    : per_shard_counts(num_shards) {
    instance_ids.reserve(num_tasks);
    for (int i = 0; i < num_tasks; ++i) {
      instance_ids.emplace_back("noop_" + std::to_string(i));
    }
  }
  
  [[nodiscard]] auto total() const -> long {
    long sum = 0;
    for (const auto& counter : per_shard_counts) {
      sum += counter.count.load(std::memory_order_relaxed);
    }
    return sum;
  }
};

auto noop_loop_task(Runtime& runtime, IExecutor& executor, BenchState* state,
                   int task_id) -> task<> {
  while (state->keep_running.load(std::memory_order_relaxed)) {
    // Use pre-allocated InstanceId (no allocation in hot path)
    [[maybe_unused]] auto res = co_await execute_async(
        runtime, executor, state->instance_ids[task_id], NoopExecutorConfig{});
    
    // Use per-shard counter to reduce contention
    auto shard = runtime.current_shard();
    if (shard < state->per_shard_counts.size()) {
      state->per_shard_counts[shard].count.fetch_add(1, std::memory_order_relaxed);
    }
  }
  co_return;
}

void BM_NoopExecutor_Throughput(benchmark::State& bench_state) {
  const int shards = to_positive_int(bench_state.range(0), 32);
  constexpr int kInFlight = 4096;

  // Setup
  Runtime runtime(static_cast<unsigned>(shards));
  runtime.start();
  auto noop = create_noop_executor(runtime);

  BenchState state(shards, kInFlight);
  state.keep_running.store(true);

  std::vector<task<>> tasks;
  tasks.reserve(kInFlight);
  
  for (int i = 0; i < kInFlight; ++i) {
    tasks.push_back(noop_loop_task(runtime, *noop, &state, i));
  }

  // Kick off all tasks
  for (auto& t : tasks) {
    runtime.schedule_external(t.take());
  }

  // Warm up
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Measure
  for (auto _ : bench_state) {
    const auto start = state.total();
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto end = state.total();
    
    const auto count = end - start;
    bench_state.SetIterationTime(1.0);
    bench_state.counters["ops_per_sec"] = benchmark::Counter(
        static_cast<double>(count), benchmark::Counter::kIsRate);
  }

  // Shutdown
  state.keep_running.store(false);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  runtime.stop();

  const auto total = state.total();
  bench_state.SetItemsProcessed(total);
}

BENCHMARK(BM_NoopExecutor_Throughput)
    ->Arg(1)
    ->Arg(4)
    ->Arg(16)
    ->Arg(32)
    ->ArgName("shards")
    ->Iterations(5)
    ->UseManualTime()
    ->Unit(benchmark::kSecond);

} // namespace
