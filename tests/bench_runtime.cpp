// bench_runtime.cpp

#include "bench_utils.hpp"

#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"

#include "benchmark_compat.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <latch>
#include <thread>
#include <vector>

namespace dagforge {
namespace {

auto signal_latch(std::latch *done) -> spawn_task {
  if (done != nullptr) {
    done->count_down();
  }
  co_return;
}

auto yield_counter(std::uint64_t hops, std::uint64_t *sink, std::latch *done)
    -> spawn_task {
  for (std::uint64_t i = 0; i < hops; ++i) {
    ++(*sink);
    co_await async_yield();
  }
  done->count_down();
}

auto cross_shard_probe(std::chrono::steady_clock::time_point started_at,
                       std::uint64_t *latency_ns, std::latch *done)
    -> spawn_task {
  const auto now = std::chrono::steady_clock::now();
  *latency_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now - started_at)
          .count());
  done->count_down();
  co_return;
}

auto cross_shard_hop(Runtime &rt, shard_id target,
                     std::chrono::steady_clock::time_point started_at,
                     std::uint64_t *latency_ns, std::latch *done)
    -> spawn_task {
  rt.spawn_on(target, cross_shard_probe(started_at, latency_ns, done));
  co_return;
}

void BM_RuntimeSameShardLatency(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  if (shards == 0) {
    state.SkipWithError("requires >= 1 shards");
    return;
  }

  bench::RuntimeGuard guard(shards);
  auto &rt = guard.runtime;
  constexpr shard_id kTarget = 0;
  constexpr std::uint64_t kSamplesPerIteration = 64;

  std::uint64_t total_latency_ns = 0;
  std::uint64_t total_samples = 0;
  for (auto _ : state) {
    for (std::uint64_t i = 0; i < kSamplesPerIteration; ++i) {
      std::latch done{1};
      std::uint64_t latency_ns = 0;
      const auto started_at = std::chrono::steady_clock::now();
      rt.spawn_on(kTarget, cross_shard_probe(started_at, &latency_ns, &done));
      done.wait();
      total_latency_ns += latency_ns;
    }
    total_samples += kSamplesPerIteration;
  }

  if (total_samples > 0) {
    state.counters["avg_latency_ns"] =
        benchmark::Counter(static_cast<double>(total_latency_ns) /
                               static_cast<double>(total_samples),
                           benchmark::Counter::kAvgThreads);
  }
  state.SetItemsProcessed(static_cast<int64_t>(total_samples));
}

auto contention_target(std::atomic<std::uint64_t> *completed, std::latch *done)
    -> spawn_task {
  completed->fetch_add(1, std::memory_order_relaxed);
  done->count_down();
  co_return;
}

auto contention_source(Runtime &rt, shard_id target, int n,
                       std::atomic<std::uint64_t> *completed, std::latch *done)
    -> spawn_task {
  for (int i = 0; i < n; ++i) {
    rt.spawn_on(target, contention_target(completed, done));
  }
  co_return;
}

void BM_RuntimeStopLatency(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  if (shards == 0) {
    state.SkipWithError("requires >= 1 shards");
    return;
  }

  for (auto _ : state) {
    state.PauseTiming();
    Runtime runtime(shards);
    auto start_res = runtime.start();
    if (!start_res) {
      const auto msg = start_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    std::latch ready{static_cast<std::ptrdiff_t>(shards)};
    for (shard_id sid = 0; sid < shards; ++sid) {
      runtime.post_to(sid, [&ready] { ready.count_down(); });
    }
    ready.wait();
    state.ResumeTiming();

    runtime.stop();
  }

  state.SetItemsProcessed(state.iterations());
}

void BM_RuntimeTaskCreateDestroy(benchmark::State &state) {
  const auto count = static_cast<int>(state.range(0));
  bench::RuntimeGuard guard(1);

  for (auto _ : state) {
    std::latch done{count};
    for (int i = 0; i < count; ++i) {
      guard.runtime.spawn_external(signal_latch(&done));
    }
    done.wait();
  }

  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

void BM_RuntimeCoroutineSuspendResume(benchmark::State &state) {
  const auto hops = static_cast<std::uint64_t>(state.range(0));
  bench::RuntimeGuard guard(1);

  for (auto _ : state) {
    std::latch done{1};
    std::uint64_t sink = 0;
    guard.runtime.spawn_external(yield_counter(hops, &sink, &done));
    done.wait();
    benchmark::DoNotOptimize(sink);
  }

  state.SetItemsProcessed(static_cast<int64_t>(hops) * state.iterations());
}

void BM_RuntimeCrossShardLatency(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  if (shards < 2) {
    state.SkipWithError("requires >= 2 shards");
    return;
  }

  bench::RuntimeGuard guard(shards);
  auto &rt = guard.runtime;
  constexpr shard_id kSource = 0;
  constexpr std::uint64_t kSamplesPerIteration = 64;

  std::uint64_t total_latency_ns = 0;
  std::uint64_t total_samples = 0;
  for (auto _ : state) {
    for (std::uint64_t i = 0; i < kSamplesPerIteration; ++i) {
      std::latch done{1};
      std::uint64_t latency_ns = 0;
      const auto target = static_cast<shard_id>((i % (shards - 1)) + 1);
      const auto started_at = std::chrono::steady_clock::now();
      rt.spawn_on(kSource,
                  cross_shard_hop(rt, target, started_at, &latency_ns, &done));
      done.wait();
      total_latency_ns += latency_ns;
    }
    total_samples += kSamplesPerIteration;
  }

  if (total_samples > 0) {
    state.counters["avg_latency_ns"] =
        benchmark::Counter(static_cast<double>(total_latency_ns) /
                               static_cast<double>(total_samples),
                           benchmark::Counter::kAvgThreads);
  }
  state.SetItemsProcessed(static_cast<int64_t>(total_samples));
}

void BM_RuntimeShardThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto task_count = static_cast<int>(state.range(1));

  bench::RuntimeGuard guard(shards);
  auto &rt = guard.runtime;

  for (auto _ : state) {
    std::latch done{task_count};
    for (int i = 0; i < task_count; ++i) {
      rt.spawn_external(signal_latch(&done));
    }
    done.wait();
  }

  state.SetItemsProcessed(static_cast<int64_t>(task_count) *
                          state.iterations());
}

void BM_RuntimeExternalInjection(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto producer_threads = static_cast<int>(state.range(1));
  const auto tasks_per_thread = static_cast<int>(state.range(2));
  const auto total_tasks = producer_threads * tasks_per_thread;

  bench::RuntimeGuard guard(shards);
  auto &rt = guard.runtime;

  for (auto _ : state) {
    std::latch done{total_tasks};
    std::vector<std::jthread> producers;
    producers.reserve(static_cast<std::size_t>(producer_threads));

    for (int tid = 0; tid < producer_threads; ++tid) {
      producers.emplace_back([&rt, &done, tasks_per_thread] {
        for (int i = 0; i < tasks_per_thread; ++i) {
          rt.spawn_external(signal_latch(&done));
        }
      });
    }

    for (auto &p : producers) {
      p.join();
    }
    done.wait();
  }

  state.SetItemsProcessed(static_cast<int64_t>(total_tasks) *
                          state.iterations());
}

void BM_RuntimeCrossShardContention(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto tasks_per_pair = static_cast<int>(state.range(1));
  if (shards < 2 || tasks_per_pair <= 0) {
    state.SkipWithError("requires shards >= 2 and tasks_per_pair > 0");
    return;
  }

  bench::RuntimeGuard guard(shards);
  auto &rt = guard.runtime;

  const auto pair_count = static_cast<int>(shards * (shards - 1));
  const auto total_tasks = pair_count * tasks_per_pair;

  for (auto _ : state) {
    std::atomic<std::uint64_t> completed{0};
    std::latch done{total_tasks};

    for (shard_id src = 0; src < shards; ++src) {
      for (shard_id dst = 0; dst < shards; ++dst) {
        if (src == dst) {
          continue;
        }
        rt.spawn_on(
            src, contention_source(rt, dst, tasks_per_pair, &completed, &done));
      }
    }

    done.wait();
    benchmark::DoNotOptimize(completed.load(std::memory_order_relaxed));
  }

  state.SetItemsProcessed(static_cast<int64_t>(total_tasks) *
                          state.iterations());
}

BENCHMARK(BM_RuntimeStopLatency)
    ->Arg(1)
    ->Arg(8)
    ->Arg(16)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeTaskCreateDestroy)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeCoroutineSuspendResume)
    ->RangeMultiplier(10)
    ->Range(1000, 100000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeCrossShardLatency)
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeSameShardLatency)
    ->Args({1})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(benchmark::kNanosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeShardThroughput)
    ->Args({1, 100000})
    ->Args({2, 100000})
    ->Args({4, 100000})
    ->Args({8, 100000})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeExternalInjection)
    ->Args({4, 4, 25000})
    ->Args({4, 8, 12500})
    ->Args({8, 8, 12500})
    ->Args({16, 16, 6250})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeCrossShardContention)
    ->Args({4, 1000})
    ->Args({8, 500})
    ->Args({16, 250})
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();

} // namespace
} // namespace dagforge
