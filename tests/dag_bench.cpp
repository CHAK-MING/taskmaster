#include "taskmaster/dag/dag.hpp"

#include <benchmark/benchmark.h>

#include "test_utils.hpp"

#include <format>

using namespace taskmaster;

namespace {
[[nodiscard]] auto make_task_id(int i) -> TaskId {
  return taskmaster::test::task_id(std::format("task_{}", i));
}
}

static void BM_DAGAddNode(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (auto _ : state) {
    dag.clear();
    for (int i = 0; i < num_nodes; ++i) {
      dag.add_node(make_task_id(i));
    }
  }

  state.SetItemsProcessed(num_nodes * state.iterations());
}

static void BM_DAGAddEdge(benchmark::State& state) {
  const int num_nodes = state.range(0);
  const int num_edges = state.range(1);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
  }

  for (auto _ : state) {
    for (int i = 0; i < num_edges; ++i) {
      auto from = i % num_nodes;
      auto to = (i + 1) % num_nodes;
      (void)dag.add_edge(from, to);
    }
  }

  state.SetItemsProcessed(num_edges * state.iterations());
}

static void BM_DAGGetTopologicalOrder(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
    if (i > 0) {
      (void)dag.add_edge(i - 1, i);
    }
  }

  for (auto _ : state) {
    auto order = dag.get_topological_order();
    benchmark::DoNotOptimize(order);
  }
}

static void BM_DAGGetDeps(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
  }

  (void)dag.add_edge(0, 1);
  (void)dag.add_edge(0, 2);
  (void)dag.add_edge(0, 3);

  for (auto _ : state) {
    auto deps = dag.get_deps(1);
    benchmark::DoNotOptimize(deps);
  }
}

static void BM_DAGGetDependents(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
  }

  (void)dag.add_edge(0, 1);
  (void)dag.add_edge(0, 2);
  (void)dag.add_edge(0, 3);

  for (auto _ : state) {
    auto dependents = dag.get_dependents(0);
    benchmark::DoNotOptimize(dependents);
  }
}

static void BM_DAGHasNode(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(dag.has_node(taskmaster::test::task_id("task_0")));
  }
}

static void BM_DAGGetIndex(benchmark::State& state) {
  const int num_nodes = state.range(0);
  DAG dag;

  for (int i = 0; i < num_nodes; ++i) {
    dag.add_node(make_task_id(i));
  }

  for (auto _ : state) {
    benchmark::DoNotOptimize(dag.get_index(taskmaster::test::task_id("task_0")));
  }
}

static void BM_DAGClear(benchmark::State& state) {
  const int num_nodes = state.range(0);

  for (auto _ : state) {
    DAG dag;
    for (int i = 0; i < num_nodes; ++i) {
      dag.add_node(make_task_id(i));
    }
    dag.clear();
  }
}

BENCHMARK(BM_DAGAddNode)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGAddEdge)->Args({10, 10})->Args({100, 100})->Args({1000, 1000});
BENCHMARK(BM_DAGGetTopologicalOrder)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGGetDeps)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGGetDependents)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGHasNode)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGGetIndex)->Arg(10)->Arg(100)->Arg(1000);
BENCHMARK(BM_DAGClear)->Arg(10)->Arg(100)->Arg(1000);
