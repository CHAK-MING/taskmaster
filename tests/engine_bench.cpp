#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/scheduler/task.hpp"

#include <benchmark/benchmark.h>

#include "test_utils.hpp"

#include <format>

using namespace taskmaster;


static void BM_EngineConstruction(benchmark::State& state) {
  Runtime rt(1);
  for (auto _ : state) {
    Engine engine(rt);
    benchmark::DoNotOptimize(engine.is_running());
  }
}

static void BM_EngineStartStop(benchmark::State& state) {
  Runtime rt(2);
  rt.start();
  for (auto _ : state) {
    Engine engine(rt);
    engine.start();
    engine.stop();
  }
  rt.stop();
}

static void BM_EngineIsRunning(benchmark::State& state) {
  Runtime rt(2);
  rt.start();
  Engine engine(rt);
  engine.start();

  for (auto _ : state) {
    benchmark::DoNotOptimize(engine.is_running());
  }

  engine.stop();
  rt.stop();
}

static void BM_EngineAddTask(benchmark::State& state) {
  const int num_tasks = state.range(0);
  Runtime rt(2);
  rt.start();
  Engine engine(rt);
  engine.start();

  auto cron = CronExpr::parse("* * * * *");
  if (!cron) {
    engine.stop();
    rt.stop();
    return;
  }

  const DAGId dag_id = taskmaster::test::dag_id("bench_dag");

  for (auto _ : state) {
    for (int i = 0; i < num_tasks; ++i) {
      ExecutionInfo exec_info{
          .dag_id = dag_id,
          .task_id = taskmaster::test::task_id(std::format("task_{}", i)),
          .name = std::string{},
          .cron_expr = *cron,
      };
      benchmark::DoNotOptimize(engine.add_task(std::move(exec_info)));
    }
  }

  state.SetItemsProcessed(num_tasks * state.iterations());
  engine.stop();
  rt.stop();
}

BENCHMARK(BM_EngineConstruction);
BENCHMARK(BM_EngineStartStop);
BENCHMARK(BM_EngineIsRunning);
BENCHMARK(BM_EngineAddTask)->Arg(10)->Arg(100);
