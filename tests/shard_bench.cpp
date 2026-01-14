#include "taskmaster/core/shard.hpp"

#include <benchmark/benchmark.h>

using namespace taskmaster;

static void BM_ShardConstruction(benchmark::State& state) {
  for (auto _ : state) {
    Shard shard(0);
    benchmark::DoNotOptimize(shard.id());
  }
}

static void BM_ShardId(benchmark::State& state) {
  Shard shard(0);

  for (auto _ : state) {
    benchmark::DoNotOptimize(shard.id());
  }
}

static void BM_ShardWakeFd(benchmark::State& state) {
  Shard shard(0);

  for (auto _ : state) {
    benchmark::DoNotOptimize(shard.wake_fd());
  }
}

static void BM_ShardMemoryResource(benchmark::State& state) {
  Shard shard(0);

  for (auto _ : state) {
    benchmark::DoNotOptimize(shard.memory_resource());
  }
}

static void BM_ShardHasWork(benchmark::State& state) {
  Shard shard(0);

  for (auto _ : state) {
    benchmark::DoNotOptimize(shard.has_work());
  }
}

static void BM_ShardIsSleeping(benchmark::State& state) {
  Shard shard(0);

  for (auto _ : state) {
    benchmark::DoNotOptimize(shard.is_sleeping());
  }
}

BENCHMARK(BM_ShardConstruction);
BENCHMARK(BM_ShardId);
BENCHMARK(BM_ShardWakeFd);
BENCHMARK(BM_ShardMemoryResource);
BENCHMARK(BM_ShardHasWork);
BENCHMARK(BM_ShardIsSleeping);
