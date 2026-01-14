#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_run.hpp"

#include <benchmark/benchmark.h>

#include "test_utils.hpp"

#include <format>

using namespace taskmaster;


static void BM_DAGRunConstruction(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    benchmark::DoNotOptimize(run.state());
  }
}

static void BM_DAGRunGetState(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    benchmark::DoNotOptimize(run.state());
  }
}

static void BM_DAGRunIsComplete(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    benchmark::DoNotOptimize(run.is_complete());
  }
}

static void BM_DAGRunHasFailed(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    benchmark::DoNotOptimize(run.has_failed());
  }
}

static void BM_DAGRunReadyCount(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    benchmark::DoNotOptimize(run.ready_count());
  }
}

static void BM_DAGRunGetReadyTasks(benchmark::State& state) {
  DAG dag;
  auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
  auto& run = *run_result;

  for (auto _ : state) {
    auto tasks = run.get_ready_tasks();
    benchmark::DoNotOptimize(tasks);
  }
}

static void BM_DAGRunWithNodes(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(taskmaster::test::task_id(std::format("task_{}", i)));
  }

  for (auto _ : state) {
    auto run_result = DAGRun::create(taskmaster::test::dag_run_id("test_run_id"), dag);
    benchmark::DoNotOptimize(run_result->ready_count());
  }
}

BENCHMARK(BM_DAGRunConstruction);
BENCHMARK(BM_DAGRunGetState);
BENCHMARK(BM_DAGRunIsComplete);
BENCHMARK(BM_DAGRunHasFailed);
BENCHMARK(BM_DAGRunReadyCount);
BENCHMARK(BM_DAGRunGetReadyTasks);
BENCHMARK(BM_DAGRunWithNodes)->Arg(10)->Arg(100)->Arg(1000);
