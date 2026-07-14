#include "bench_utils.hpp"

#include "dagforge/core/runtime.hpp"

#include "benchmark_compat.hpp"

#include <atomic>
#include <cstdint>
#include <latch>
#include <thread>
#include <vector>

namespace dagforge {
namespace {

void BM_RuntimeSameShardBatch(benchmark::State &state) {
  const auto task_count = static_cast<int>(state.range(0));
  bench::RuntimeGuard guard(1);

  for (auto _ : state) {
    std::latch done{task_count};
    for (int task = 0; task < task_count; ++task) {
      guard.runtime.post_to(0, [&done] { done.count_down(); });
    }
    done.wait();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(task_count) *
                          state.iterations());
}

void BM_RuntimeFanOutBarrier(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto tasks_per_shard = static_cast<int>(state.range(1));
  bench::RuntimeGuard guard(shards);
  const auto total_tasks = static_cast<int>(shards) * tasks_per_shard;

  for (auto _ : state) {
    std::latch done{total_tasks};
    for (shard_id target = 0; target < shards; ++target) {
      for (int task = 0; task < tasks_per_shard; ++task) {
        guard.runtime.post_to(target, [&done] { done.count_down(); });
      }
    }
    done.wait();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(total_tasks) *
                          state.iterations());
}

void BM_RuntimeOwnerShardFanIn(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto producer_count = static_cast<int>(state.range(1));
  const auto tasks_per_producer = static_cast<int>(state.range(2));
  const auto total_tasks = producer_count * tasks_per_producer;
  bench::RuntimeGuard guard(shards);

  for (auto _ : state) {
    std::latch done{total_tasks};
    std::vector<std::jthread> producers;
    producers.reserve(static_cast<std::size_t>(producer_count));
    for (int producer = 0; producer < producer_count; ++producer) {
      producers.emplace_back([&] {
        for (int task = 0; task < tasks_per_producer; ++task) {
          guard.runtime.post_to(0, [&done] { done.count_down(); });
        }
      });
    }
    producers.clear();
    done.wait();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(total_tasks) *
                          state.iterations());
}

void BM_RuntimeExternalRoundRobin(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto task_count = static_cast<int>(state.range(1));
  bench::RuntimeGuard guard(shards);

  for (auto _ : state) {
    std::latch done{task_count};
    for (int task = 0; task < task_count; ++task) {
      const auto target = static_cast<shard_id>(task % shards);
      guard.runtime.post_to(target, [&done] { done.count_down(); });
    }
    done.wait();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(task_count) *
                          state.iterations());
}

void BM_RuntimeColdStartStop(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  for (auto _ : state) {
    Runtime runtime(shards, false, 0);
    auto started = runtime.start();
    if (!started) {
      state.SkipWithError(started.error().message().c_str());
      return;
    }
    runtime.stop();
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_RuntimeSameShardBatch)
    ->Arg(1'000)
    ->Arg(10'000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeFanOutBarrier)
    ->Args({2, 1'000})
    ->Args({4, 1'000})
    ->Args({8, 1'000})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeOwnerShardFanIn)
    ->Args({4, 4, 2'500})
    ->Args({8, 8, 1'250})
    ->Args({16, 16, 625})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeExternalRoundRobin)
    ->Args({1, 10'000})
    ->Args({4, 10'000})
    ->Args({8, 10'000})
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_RuntimeColdStartStop)
    ->Arg(1)
    ->Arg(4)
    ->Arg(8)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

} // namespace
} // namespace dagforge
