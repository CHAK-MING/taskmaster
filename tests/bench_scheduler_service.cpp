#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/config/task_config.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/log.hpp"

#include "bench_utils.hpp"

#include "benchmark_compat.hpp"
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <format>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace dagforge {
namespace {

[[nodiscard]] auto make_bench_dag_id(std::string_view prefix, unsigned shards,
                                     std::uint64_t run_id, int i) -> DAGId {
  return DAGId{std::string(prefix) + "_s" + std::to_string(shards) + "_iter" +
               std::to_string(run_id) + "_dag" + std::to_string(i)};
}

[[nodiscard]] auto make_catchup_dag_info() -> DAGInfo {
  DAGInfo info;
  info.name = "bench_scheduler";
  info.cron = "* * * * *";
  info.catchup = true;

  const auto now = std::chrono::system_clock::now();
  const auto minute = std::chrono::floor<std::chrono::minutes>(now);
  // Ensure exactly one historical schedule is emitted in catchup path.
  info.start_date = minute - std::chrono::minutes(1);
  info.end_date = minute;
  return info;
}

[[nodiscard]] auto make_single_fire_dag_info() -> DAGInfo {
  DAGInfo info;
  info.name = "bench_cron_single_fire";
  info.cron = "* * * * *";
  info.catchup = true;

  const auto now = std::chrono::system_clock::now();
  const auto minute = std::chrono::floor<std::chrono::minutes>(now);
  info.start_date = minute - std::chrono::minutes(1);
  info.end_date = minute;
  return info;
}

[[nodiscard]] auto make_cron_only_dag_info() -> DAGInfo {
  DAGInfo info;
  info.name = "bench_cron";
  info.cron = "* * * * *";
  info.catchup = false;
  return info;
}

[[nodiscard]] auto owner_shard_for(const DAGRunId &dag_run_id,
                                   unsigned shards) noexcept -> shard_id {
  return static_cast<shard_id>(
      util::shard_of(dag_run_id.value(), std::max(1U, shards)));
}

[[nodiscard]] auto make_small_template_dag() -> std::shared_ptr<DAG> {
  auto dag = std::make_shared<DAG>();
  (void)dag->add_node(TaskId{"root"});
  (void)dag->add_node(TaskId{"mid"});
  (void)dag->add_node(TaskId{"leaf"});
  (void)dag->add_edge(0, 1);
  (void)dag->add_edge(1, 2);
  return dag;
}

void BM_SchedulerCatchupBatchTrigger(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    SchedulerService scheduler(runtime_guard.runtime, 1);

    std::mutex mu;
    std::condition_variable cv;
    std::atomic<int> fired{0};

    scheduler.set_on_dag_trigger(
        [&](const DAGId &, std::chrono::system_clock::time_point) {
          const auto after = fired.fetch_add(1, std::memory_order_acq_rel) + 1;
          if (after >= dag_count) {
            std::scoped_lock lock(mu);
            cv.notify_one();
          }
        });
    scheduler.start();
    const auto info = make_catchup_dag_info();
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      scheduler.register_dag(make_bench_dag_id("bench_sched", shards, run_id, i),
                             info);
    }

    {
      std::unique_lock lock(mu);
      const auto done = cv.wait_for(lock, std::chrono::seconds(10), [&] {
        return fired.load(std::memory_order_acquire) >= dag_count;
      });
      if (!done) {
        state.SkipWithError("timeout waiting for catchup triggers");
        return;
      }
    }

    state.PauseTiming();
    auto stop_res = scheduler.stop();
    runtime_guard.runtime.stop();
    state.ResumeTiming();
    if (!stop_res) {
      const auto msg = stop_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    total_processed += dag_count;
    ++run_id;
    state.counters["triggers"] =
        benchmark::Counter(fired.load(std::memory_order_relaxed),
                           benchmark::Counter::kIsIterationInvariantRate);
  }
  state.SetItemsProcessed(total_processed);
}

BENCHMARK(BM_SchedulerCatchupBatchTrigger)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({2, 1000})
    ->Args({4, 1000})
    ->Args({8, 1000})
    ->UseRealTime();

struct DispatchBenchState {
  std::atomic<int> fired{0};
  std::atomic<int> executed{0};
  int expected{0};
};

auto run_dispatch_benchmark_execution(
    std::shared_ptr<DispatchBenchState> state, int yields) -> spawn_task {
  for (int i = 0; i < yields; ++i) {
    co_await async_yield();
  }
  state->executed.fetch_add(1, std::memory_order_acq_rel);
}

struct HandoffBenchState {
  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> created{0};
  int expected{0};
};

struct FullPathBenchState {
  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> completed{0};
  int expected{0};
  std::vector<std::atomic<int>> dag_owner_hits;
  std::vector<std::atomic<int>> run_owner_hits;
  std::vector<std::atomic<int>> executor_start_hits;
  std::vector<std::atomic<int>> run_complete_hits;

  explicit FullPathBenchState(unsigned shards)
      : dag_owner_hits(shards), run_owner_hits(shards),
        executor_start_hits(shards), run_complete_hits(shards) {
    for (unsigned i = 0; i < shards; ++i) {
      dag_owner_hits[i].store(0, std::memory_order_relaxed);
      run_owner_hits[i].store(0, std::memory_order_relaxed);
      executor_start_hits[i].store(0, std::memory_order_relaxed);
      run_complete_hits[i].store(0, std::memory_order_relaxed);
    }
  }
};

class ImmediateCountingExecutor final : public IExecutor {
public:
  ImmediateCountingExecutor(Runtime &runtime,
                            std::shared_ptr<FullPathBenchState> shared)
      : runtime_(runtime), shared_(std::move(shared)) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    auto sid =
        runtime_.is_current_shard() ? runtime_.current_shard() : shard_id{0};
    shared_->executor_start_hits[sid].fetch_add(1, std::memory_order_relaxed);

    runtime_.spawn(complete_immediately(std::move(req), std::move(sink)));
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

private:
  static auto complete_immediately(ExecutorRequest req, ExecutionSink sink)
      -> spawn_task {
    ExecutorResult result;
    result.exit_code = 0;
    if (sink.on_complete) {
      sink.on_complete(req.instance_id, std::move(result));
    }
    co_return;
  }

  Runtime &runtime_;
  std::shared_ptr<FullPathBenchState> shared_;
};

struct DispatchStormBenchState {
  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> completed{0};
  std::atomic<int> executor_starts{0};
  int expected{0};
};

class YieldingSuccessExecutor final : public IExecutor {
public:
  YieldingSuccessExecutor(Runtime &runtime,
                          std::shared_ptr<DispatchStormBenchState> shared,
                          int yields_before_complete)
      : runtime_(runtime), shared_(std::move(shared)),
        yields_before_complete_(yields_before_complete) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    shared_->executor_starts.fetch_add(1, std::memory_order_relaxed);
    runtime_.spawn(complete_after_yields(std::move(req), std::move(sink),
                                         yields_before_complete_));
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

private:
  static auto complete_after_yields(ExecutorRequest req, ExecutionSink sink,
                                    int yields_before_complete) -> spawn_task {
    for (int i = 0; i < yields_before_complete; ++i) {
      co_await async_yield();
    }
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = 0;
      sink.on_complete(req.instance_id, std::move(result));
    }
    co_return;
  }

  Runtime &runtime_;
  std::shared_ptr<DispatchStormBenchState> shared_;
  int yields_before_complete_{0};
};

auto create_bench_dag_run(std::shared_ptr<HandoffBenchState> shared,
                          DAGRunId dag_run_id,
                          std::shared_ptr<const DAG> dag_template)
    -> spawn_task {
  auto run = DAGRun::create(dag_run_id.clone(), std::move(dag_template));
  if (run) {
    const auto after =
        shared->created.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (after >= shared->expected) {
      std::scoped_lock lock(shared->mu);
      shared->cv.notify_one();
    }
  }
  co_return;
}

[[nodiscard]] auto make_single_task_template_dag() -> std::shared_ptr<DAG> {
  auto dag = std::make_shared<DAG>();
  (void)dag->add_node(TaskId{"worker"});
  return dag;
}

[[nodiscard]] auto make_single_task_config() -> TaskConfig {
  TaskConfig cfg;
  cfg.task_id = TaskId{"worker"};
  cfg.name = "worker";
  cfg.executor = ExecutorType::Shell;
  cfg.command = ":";
  return cfg;
}

[[nodiscard]] auto make_fullpath_run_context(const DAGRunId &dag_run_id,
                                             const DAGId &dag_id)
    -> Result<ExecutionService::RunContext> {
  auto dag = make_single_task_template_dag();
  auto run_res = DAGRun::create(dag_run_id.clone(), dag);
  if (!run_res) {
    return fail(run_res.error());
  }

  auto run = std::make_unique<DAGRun>(std::move(*run_res));
  const auto now = std::chrono::system_clock::now();
  run->set_trigger_type(TriggerType::Manual);
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  std::vector<ExecutorConfig> executor_configs;
  executor_configs.emplace_back(ShellExecutorConfig{});

  std::vector<TaskConfig::Compiled> task_configs;
  task_configs.emplace_back(make_single_task_config().compiled());

  return ok(ExecutionService::RunContext{
      .run = std::move(run),
      .executor_configs = std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs)),
      .task_configs = std::make_shared<const std::vector<TaskConfig::Compiled>>(
          std::move(task_configs)),
      .dag_id = dag_id.clone()});
}

[[nodiscard]] auto make_two_task_template_dag() -> std::shared_ptr<DAG> {
  auto dag = std::make_shared<DAG>();
  (void)dag->add_node(TaskId{"t0"});
  (void)dag->add_node(TaskId{"t1"});
  (void)dag->add_edge(0, 1);
  return dag;
}

[[nodiscard]] auto make_two_task_run_context(const DAGRunId &dag_run_id,
                                             const DAGId &dag_id)
    -> Result<ExecutionService::RunContext> {
  auto dag = make_two_task_template_dag();
  auto run_res = DAGRun::create(dag_run_id.clone(), dag);
  if (!run_res) {
    return fail(run_res.error());
  }

  auto run = std::make_unique<DAGRun>(std::move(*run_res));
  const auto now = std::chrono::system_clock::now();
  run->set_trigger_type(TriggerType::Manual);
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_execution_date(now);

  std::vector<ExecutorConfig> executor_configs;
  executor_configs.emplace_back(ShellExecutorConfig{});
  executor_configs.emplace_back(ShellExecutorConfig{});

  std::vector<TaskConfig::Compiled> task_configs;
  for (const auto &task_name : {"t0", "t1"}) {
    TaskConfig cfg;
    cfg.task_id = TaskId{task_name};
    cfg.name = task_name;
    cfg.executor = ExecutorType::Shell;
    cfg.command = ":";
    task_configs.emplace_back(cfg.compiled());
  }

  return ok(ExecutionService::RunContext{
      .run = std::move(run),
      .executor_configs = std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs)),
      .task_configs = std::make_shared<const std::vector<TaskConfig::Compiled>>(
          std::move(task_configs)),
      .dag_id = dag_id.clone()});
}

void BM_SchedulerCatchupScheduleAndExecute(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  const auto exec_yields = static_cast<int>(state.range(2));
  if (shards == 0 || dag_count <= 0 || exec_yields < 0) {
    state.SkipWithError("requires shards > 0, dag_count > 0, exec_yields >= 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    SchedulerService scheduler(runtime_guard.runtime, 1);
    auto shared = std::make_shared<DispatchBenchState>();
    shared->expected = dag_count;

    scheduler.set_on_dag_trigger(
        [&runtime_guard, shared, exec_yields](
            const DAGId &, std::chrono::system_clock::time_point) {
          shared->fired.fetch_add(1, std::memory_order_relaxed);
          runtime_guard.runtime.spawn_external(
              run_dispatch_benchmark_execution(shared, exec_yields));
        });

    scheduler.start();
    const auto info = make_catchup_dag_info();
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      scheduler.register_dag(
          make_bench_dag_id("bench_sched_exec", shards, run_id, i), info);
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (shared->executed.load(std::memory_order_acquire) < shared->expected &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (shared->executed.load(std::memory_order_acquire) < shared->expected) {
      state.SkipWithError("timeout waiting for dispatched execution completion");
      return;
    }

    state.PauseTiming();
    auto stop_res = scheduler.stop();
    runtime_guard.runtime.stop();
    state.ResumeTiming();
    if (!stop_res) {
      const auto msg = stop_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    total_processed += dag_count;
    ++run_id;
    state.counters["fired"] =
        benchmark::Counter(shared->fired.load(std::memory_order_relaxed),
                           benchmark::Counter::kIsIterationInvariantRate);
    state.counters["executed"] =
        benchmark::Counter(shared->executed.load(std::memory_order_relaxed),
                           benchmark::Counter::kIsIterationInvariantRate);
  }
  state.SetItemsProcessed(total_processed);
}

void BM_CronRegisterThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    SchedulerService scheduler(runtime_guard.runtime, 1);
    scheduler.start();
    const auto info = make_cron_only_dag_info();
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      scheduler.register_dag(
          make_bench_dag_id("bench_cron_register", shards, run_id, i), info);
    }

    state.PauseTiming();
    auto stop_res = scheduler.stop();
    runtime_guard.runtime.stop();
    state.ResumeTiming();
    if (!stop_res) {
      const auto msg = stop_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    total_processed += dag_count;
    ++run_id;
  }
  state.SetItemsProcessed(total_processed);
}

void BM_CronFireThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    std::mutex mu;
    std::condition_variable cv;
    std::atomic<int> fired{0};
    std::vector<std::shared_ptr<boost::asio::steady_timer>> timers;
    timers.reserve(static_cast<std::size_t>(dag_count));
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      auto sid = static_cast<shard_id>(i % shards);
      auto timer = std::make_shared<boost::asio::steady_timer>(
          runtime_guard.runtime.shard(sid).ctx());
      timers.push_back(timer);
      boost::asio::post(runtime_guard.runtime.shard(sid).ctx(), [&, timer]() {
        timer->expires_after(std::chrono::milliseconds(0));
        timer->async_wait([&](const boost::system::error_code &ec) {
          if (ec) {
            return;
          }
          const auto after = fired.fetch_add(1, std::memory_order_acq_rel) + 1;
          if (after >= dag_count) {
            std::scoped_lock lock(mu);
            cv.notify_one();
          }
        });
      });
    }

    {
      std::unique_lock lock(mu);
      const auto done = cv.wait_for(lock, std::chrono::seconds(10), [&] {
        return fired.load(std::memory_order_acquire) >= dag_count;
      });
      if (!done) {
        state.SkipWithError("timeout waiting for cron fire callbacks");
        return;
      }
    }

    state.PauseTiming();
    timers.clear();
    runtime_guard.runtime.stop();
    state.ResumeTiming();

    total_processed += dag_count;
  }
  state.SetItemsProcessed(total_processed);
}

void BM_CronTriggerRunHandoffThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  const auto dag_template = make_small_template_dag();
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    auto shared = std::make_shared<HandoffBenchState>();
    shared->expected = dag_count;
    std::vector<std::shared_ptr<boost::asio::steady_timer>> timers;
    timers.reserve(static_cast<std::size_t>(dag_count));
    const auto run_nonce = static_cast<std::uint64_t>(total_processed);
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      auto sid = static_cast<shard_id>(i % shards);
      auto timer = std::make_shared<boost::asio::steady_timer>(
          runtime_guard.runtime.shard(sid).ctx());
      timers.push_back(timer);
      auto dag_id =
          make_bench_dag_id("bench_cron_handoff", shards, run_nonce, i);

      boost::asio::post(runtime_guard.runtime.shard(sid).ctx(),
                        [&, timer, dag_id = std::move(dag_id), shared,
                         dag_template]() mutable {
                          timer->expires_after(std::chrono::milliseconds(0));
                          timer->async_wait(
                              [&, dag_id = std::move(dag_id), shared,
                               dag_template](
                                  const boost::system::error_code &ec) mutable {
                                if (ec) {
                                  return;
                                }
                                auto dag_run_id = generate_dag_run_id(dag_id);
                                auto owner = owner_shard_for(
                                    dag_run_id,
                                    runtime_guard.runtime.shard_count());
                                runtime_guard.runtime.spawn_on(
                                    owner, create_bench_dag_run(
                                               shared, std::move(dag_run_id),
                                               dag_template));
                              });
                        });
    }

    {
      std::unique_lock lock(shared->mu);
      const auto done = shared->cv.wait_for(
          lock, std::chrono::seconds(10), [&] {
            return shared->created.load(std::memory_order_acquire) >=
                   shared->expected;
      });
      if (!done) {
        state.SkipWithError("timeout waiting for dag run handoff completion");
        return;
      }
    }

    state.PauseTiming();
    timers.clear();
    runtime_guard.runtime.stop();
    state.ResumeTiming();

    total_processed += dag_count;
  }
  state.SetItemsProcessed(total_processed);
}

void BM_CronProductionPathThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  const auto dag_template = make_small_template_dag();
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    SchedulerService scheduler(runtime_guard.runtime, 1);
    auto shared = std::make_shared<HandoffBenchState>();
    shared->expected = dag_count;

    scheduler.set_on_dag_trigger(
        [&runtime_guard, shared, dag_template](const DAGId &dag_id,
                                               std::chrono::system_clock::time_point) {
          auto dag_run_id = generate_dag_run_id(dag_id);
          auto owner = owner_shard_for(dag_run_id, runtime_guard.runtime.shard_count());
          runtime_guard.runtime.spawn_on(
              owner, create_bench_dag_run(shared, std::move(dag_run_id), dag_template));
        });

    scheduler.start();
    const auto info = make_single_fire_dag_info();
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      scheduler.register_dag(
          make_bench_dag_id("bench_cron_prod", shards, run_id, i), info);
    }

    {
      std::unique_lock lock(shared->mu);
      const auto done = shared->cv.wait_for(
          lock, std::chrono::seconds(10), [&] {
            return shared->created.load(std::memory_order_acquire) >=
                   shared->expected;
          });
      if (!done) {
        state.SkipWithError("timeout waiting for production cron path completion");
        return;
      }
    }

    state.PauseTiming();
    auto stop_res = scheduler.stop();
    runtime_guard.runtime.stop();
    state.ResumeTiming();
    if (!stop_res) {
      const auto msg = stop_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    total_processed += dag_count;
    ++run_id;
  }
  state.SetItemsProcessed(total_processed);
}

void BM_FullPathFakeExecutorThroughput(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto dag_count = static_cast<int>(state.range(1));
  if (shards == 0 || dag_count <= 0) {
    state.SkipWithError("requires shards > 0 and dag_count > 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    auto shared = std::make_shared<FullPathBenchState>(shards);
    shared->expected = dag_count;
    ImmediateCountingExecutor executor(runtime_guard.runtime, shared);
    ExecutionService execution(runtime_guard.runtime, executor);
    SchedulerService scheduler(runtime_guard.runtime, 1);

    ExecutionCallbacks callbacks;
    callbacks.get_dag_id_by_run =
        [](const DAGRunId &) -> task<Result<DAGId>> { co_return fail(Error::NotFound); };
    callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
    callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
      return std::chrono::seconds(0);
    };
    callbacks.on_run_status =
        [&runtime_guard, shared](const DAGRunId &, DAGRunState) {
          auto sid = runtime_guard.runtime.is_current_shard()
                         ? runtime_guard.runtime.current_shard()
                         : shard_id{0};
          shared->run_complete_hits[sid].fetch_add(1, std::memory_order_relaxed);
          const auto after =
              shared->completed.fetch_add(1, std::memory_order_acq_rel) + 1;
          if (after >= shared->expected) {
            std::scoped_lock lock(shared->mu);
            shared->cv.notify_one();
          }
        };
    execution.set_callbacks(std::move(callbacks));

    scheduler.set_on_dag_trigger(
        [&runtime_guard, &execution, shared](const DAGId &dag_id,
                                             std::chrono::system_clock::time_point) {
          auto dag_owner = runtime_guard.runtime.is_current_shard()
                               ? runtime_guard.runtime.current_shard()
                               : shard_id{0};
          shared->dag_owner_hits[dag_owner].fetch_add(1, std::memory_order_relaxed);

          auto dag_run_id = generate_dag_run_id(dag_id);
          auto run_owner = owner_shard_for(dag_run_id, runtime_guard.runtime.shard_count());
          shared->run_owner_hits[run_owner].fetch_add(1, std::memory_order_relaxed);

          auto ctx = make_fullpath_run_context(dag_run_id, dag_id);
          if (ctx) {
            execution.start_run(dag_run_id, std::move(*ctx));
          }
        });

    scheduler.start();
    const auto info = make_single_fire_dag_info();
    state.ResumeTiming();

    for (int i = 0; i < dag_count; ++i) {
      scheduler.register_dag(
          make_bench_dag_id("bench_fullpath_fake", shards, run_id, i), info);
    }

    {
      std::unique_lock lock(shared->mu);
      const auto done = shared->cv.wait_for(lock, std::chrono::seconds(10), [&] {
        return shared->completed.load(std::memory_order_acquire) >=
               shared->expected;
      });
      if (!done) {
        state.SkipWithError("timeout waiting for full-path fake executor completion");
        return;
      }
    }

    state.PauseTiming();
    auto stop_res = scheduler.stop();
    runtime_guard.runtime.stop();
    state.ResumeTiming();
    if (!stop_res) {
      const auto msg = stop_res.error().message();
      state.SkipWithError(msg.c_str());
      return;
    }

    for (unsigned sid = 0; sid < shards; ++sid) {
      state.counters[std::format("dag_owner_shard_{}", sid)] =
          shared->dag_owner_hits[sid].load(std::memory_order_relaxed);
      state.counters[std::format("run_owner_shard_{}", sid)] =
          shared->run_owner_hits[sid].load(std::memory_order_relaxed);
      state.counters[std::format("executor_shard_{}", sid)] =
          shared->executor_start_hits[sid].load(std::memory_order_relaxed);
      state.counters[std::format("complete_shard_{}", sid)] =
          shared->run_complete_hits[sid].load(std::memory_order_relaxed);
    }

    total_processed += dag_count;
    ++run_id;
  }
  state.SetItemsProcessed(total_processed);
}

void BM_ExecutionDispatchStorm(benchmark::State &state) {
  const auto shards = static_cast<unsigned>(state.range(0));
  const auto run_count = static_cast<int>(state.range(1));
  const auto max_concurrency = static_cast<int>(state.range(2));
  const auto yields_before_complete = static_cast<int>(state.range(3));
  if (shards == 0 || run_count <= 0 || max_concurrency <= 0 ||
      yields_before_complete < 0) {
    state.SkipWithError(
        "requires shards > 0, run_count > 0, max_concurrency > 0, yields >= 0");
    return;
  }

  std::int64_t total_processed = 0;
  std::uint64_t run_id = 0;
  log::set_level(log::Level::Warn);
  for (auto _ : state) {
    state.PauseTiming();
    bench::RuntimeGuard runtime_guard(shards);
    auto shared = std::make_shared<DispatchStormBenchState>();
    shared->expected = run_count;
    YieldingSuccessExecutor executor(runtime_guard.runtime, shared,
                                     yields_before_complete);
    ExecutionService execution(runtime_guard.runtime, executor);
    execution.set_max_concurrency(max_concurrency);

    ExecutionCallbacks callbacks;
    callbacks.get_dag_id_by_run =
        [](const DAGRunId &) -> task<Result<DAGId>> { co_return ok(DAGId{"bench_exec"}); };
    callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
    callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
      return std::chrono::seconds(0);
    };
    callbacks.on_run_status = [shared](const DAGRunId &, DAGRunState run_state) {
      if (run_state != DAGRunState::Success && run_state != DAGRunState::Failed) {
        return;
      }
      const auto after =
          shared->completed.fetch_add(1, std::memory_order_acq_rel) + 1;
      if (after >= shared->expected) {
        std::scoped_lock lock(shared->mu);
        shared->cv.notify_one();
      }
    };
    execution.set_callbacks(std::move(callbacks));
    state.ResumeTiming();

    for (int i = 0; i < run_count; ++i) {
      auto dag_id =
          make_bench_dag_id("bench_exec_dispatch", shards, run_id, i);
      auto dag_run_id = generate_dag_run_id(dag_id);
      auto ctx = make_two_task_run_context(dag_run_id, dag_id);
      if (!ctx) {
        state.SkipWithError("failed to create two-task run context");
        return;
      }
      execution.start_run(dag_run_id, std::move(*ctx));
    }

    {
      std::unique_lock lock(shared->mu);
      const auto done = shared->cv.wait_for(lock, std::chrono::seconds(10), [&] {
        return shared->completed.load(std::memory_order_acquire) >=
               shared->expected;
      });
      if (!done) {
        state.SkipWithError("timeout waiting for execution completion");
        return;
      }
    }

    state.PauseTiming();
    state.counters["dispatch_invocations"] =
        execution.dispatch_invocations();
    state.counters["dispatch_scan_invocations"] =
        execution.dispatch_scan_invocations();
    state.counters["dispatch_per_run"] = benchmark::Counter(
        static_cast<double>(execution.dispatch_invocations()) / run_count,
        benchmark::Counter::kAvgThreads);
    state.counters["executor_starts"] =
        shared->executor_starts.load(std::memory_order_relaxed);
    runtime_guard.runtime.stop();
    state.ResumeTiming();

    total_processed += run_count;
    ++run_id;
  }
  state.SetItemsProcessed(total_processed);
}

BENCHMARK(BM_SchedulerCatchupScheduleAndExecute)
    ->Args({1, 1000, 0})
    ->Args({2, 1000, 0})
    ->Args({4, 1000, 0})
    ->Args({8, 1000, 0})
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_CronRegisterThroughput)
    ->Args({1, 1000})
    ->Args({4, 1000})
    ->Args({8, 1000})
    ->Args({16, 1000})
    ->UseRealTime();

BENCHMARK(BM_CronFireThroughput)
    ->Args({1, 1000})
    ->Args({4, 1000})
    ->Args({8, 1000})
    ->Args({16, 1000})
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_CronTriggerRunHandoffThroughput)
    ->Args({1, 1000})
    ->Args({4, 1000})
    ->Args({8, 1000})
    ->Args({16, 1000})
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_CronProductionPathThroughput)
    ->Args({1, 1000})
    ->Args({4, 1000})
    ->Args({8, 1000})
    ->Args({16, 1000})
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_FullPathFakeExecutorThroughput)
    ->Args({8, 1000})
    ->Args({16, 1000})
    ->Iterations(1)
    ->UseRealTime();

BENCHMARK(BM_ExecutionDispatchStorm)
    ->Args({1, 1000, 100, 8})
    ->Args({4, 1000, 100, 8})
    ->Iterations(1)
    ->UseRealTime();

} // namespace
} // namespace dagforge
