#include "taskmaster/dag/dag_manager.hpp"

#include <benchmark/benchmark.h>

#include "test_utils.hpp"

#include <chrono>
#include <format>

using namespace taskmaster;

namespace {

[[nodiscard]] auto make_dag_info() -> DAGInfo {
  const auto now = std::chrono::system_clock::now();
  DAGInfo info;
  info.name = "bench";
  info.description = "";
  info.cron = "";
  info.max_concurrent_runs = 1;
  info.created_at = now;
  info.updated_at = now;
  return info;
}

[[nodiscard]] auto make_dag_id(int i) -> DAGId {
  return taskmaster::test::dag_id(std::format("dag_{}", i));
}

}

static void BM_DAGManagerConstruction(benchmark::State& state) {
  for (auto _ : state) {
    DAGManager manager;
    benchmark::DoNotOptimize(manager.dag_count());
  }
}

static void BM_DAGManagerCreateDag(benchmark::State& state) {
  const int num_dags = state.range(0);

  for (auto _ : state) {
    DAGManager manager;
    for (int i = 0; i < num_dags; ++i) {
      auto created = manager.create_dag(make_dag_id(i), make_dag_info());
      benchmark::DoNotOptimize(created);
    }
  }

  state.SetItemsProcessed(num_dags * state.iterations());
}

static void BM_DAGManagerListDags(benchmark::State& state) {
  const int num_dags = state.range(0);

  DAGManager manager;
  for (int i = 0; i < num_dags; ++i) {
    (void)manager.create_dag(make_dag_id(i), make_dag_info());
  }

  for (auto _ : state) {
    auto dags = manager.list_dags();
    benchmark::DoNotOptimize(dags);
  }
}

static void BM_DAGManagerGetDag(benchmark::State& state) {
  DAGManager manager;
  const DAGId dag_id = taskmaster::test::dag_id("test_dag");
  (void)manager.create_dag(dag_id, make_dag_info());

  for (auto _ : state) {
    auto dag = manager.get_dag(dag_id);
    benchmark::DoNotOptimize(dag);
  }
}

static void BM_DAGManagerAddTask(benchmark::State& state) {
  const int num_tasks = state.range(0);

  DAGManager manager;
  const DAGId dag_id = taskmaster::test::dag_id("test_dag");
  (void)manager.create_dag(dag_id, make_dag_info());

  for (auto _ : state) {
    for (int i = 0; i < num_tasks; ++i) {
      TaskConfig task;
      task.task_id = taskmaster::test::task_id(std::format("task_{}", i));
      auto added = manager.add_task(dag_id, task);
      benchmark::DoNotOptimize(added);
    }
  }

  state.SetItemsProcessed(num_tasks * state.iterations());
}

BENCHMARK(BM_DAGManagerConstruction);
BENCHMARK(BM_DAGManagerCreateDag)->Arg(10)->Arg(100);
BENCHMARK(BM_DAGManagerListDags)->Arg(10)->Arg(100);
BENCHMARK(BM_DAGManagerGetDag);
BENCHMARK(BM_DAGManagerAddTask)->Arg(10)->Arg(100);
