#include "taskmaster/core/runtime.hpp"

#include <benchmark/benchmark.h>

using namespace taskmaster;

static void BM_RuntimeStartStop(benchmark::State& state) {
  const int num_shards = state.range(0);

  for (auto _ : state) {
    Runtime rt(num_shards);
    rt.start();
    rt.stop();
  }
}

static void BM_RuntimeGetShardCount(benchmark::State& state) {
  Runtime rt(4);
  rt.start();

  for (auto _ : state) {
    benchmark::DoNotOptimize(rt.shard_count());
  }

  rt.stop();
}

static void BM_RuntimeCurrentShard(benchmark::State& state) {
  Runtime rt(4);
  rt.start();

  for (auto _ : state) {
    benchmark::DoNotOptimize(rt.current_shard());
  }

  rt.stop();
}

static void BM_RuntimeIsRunning(benchmark::State& state) {
  Runtime rt(1);
  rt.start();

  for (auto _ : state) {
    benchmark::DoNotOptimize(rt.is_running());
  }

  rt.stop();
}

BENCHMARK(BM_RuntimeStartStop)->Arg(1)->Arg(2)->Arg(4);
BENCHMARK(BM_RuntimeGetShardCount);
BENCHMARK(BM_RuntimeCurrentShard);
BENCHMARK(BM_RuntimeIsRunning);
