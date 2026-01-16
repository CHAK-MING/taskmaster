#include "taskmaster/executor/executor.hpp"

#include <benchmark/benchmark.h>

using namespace taskmaster;

static void BM_ExecutorTypeRegistryToString(benchmark::State& state) {
  auto& registry = ExecutorTypeRegistry::instance();

  for (auto _ : state) {
    auto name = registry.to_string(ExecutorType::Shell);
    benchmark::DoNotOptimize(name);
  }
}

static void BM_ExecutorTypeRegistryFromString(benchmark::State& state) {
  auto& registry = ExecutorTypeRegistry::instance();

  for (auto _ : state) {
    auto type = registry.from_string("shell");
    benchmark::DoNotOptimize(type);
  }
}

static void BM_ExecutorTypeToString(benchmark::State& state) {
  for (auto _ : state) {
    auto name = to_string_view(ExecutorType::Shell);
    benchmark::DoNotOptimize(name);
  }
}

static void BM_StringToExecutorType(benchmark::State& state) {
  for (auto _ : state) {
    auto type = parse<ExecutorType>("shell");
    benchmark::DoNotOptimize(type);
  }
}

BENCHMARK(BM_ExecutorTypeRegistryToString);
BENCHMARK(BM_ExecutorTypeRegistryFromString);
BENCHMARK(BM_ExecutorTypeToString);
BENCHMARK(BM_StringToExecutorType);
