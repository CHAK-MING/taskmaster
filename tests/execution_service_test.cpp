#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/core/sync_wait.hpp"

#include "gtest/gtest.h"

#include <boost/asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

using namespace dagforge;

namespace {

class ImmediateSuccessExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = 0;
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

class ImmediateTimeoutExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = kExitCodeTimeout;
      result.timed_out = true;
      result.error = "Execution timeout";
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

class ImmediateFailureExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = 1;
      result.error = "retry me";
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

class ImmediateExit127Executor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    if (sink.on_complete) {
      ExecutorResult result;
      result.exit_code = 127;
      result.error = "command not found";
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}
};

class FailOnceThenSucceedExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    const auto starts =
        start_count_.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (sink.on_complete) {
      ExecutorResult result;
      if (starts == 1) {
        result.exit_code = 1;
        result.error = "retry once";
      } else {
        result.exit_code = 0;
      }
      sink.on_complete(req.instance_id, std::move(result));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

  [[nodiscard]] auto start_count() const -> int {
    return start_count_.load(std::memory_order_acquire);
  }

private:
  std::atomic<int> start_count_{0};
};

class HeartbeatThenHangExecutor final : public IExecutor {
public:
  explicit HeartbeatThenHangExecutor(Runtime &runtime) : runtime_(&runtime) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    auto slot = std::make_shared<ActiveSlot>();
    slot->instance_id = req.instance_id.clone();
    slot->sink = std::make_shared<ExecutionSink>(std::move(sink));
    {
      std::scoped_lock lock(mu_);
      active_.insert_or_assign(std::string(slot->instance_id.value()), slot);
    }
    if (slot->sink->on_heartbeat) {
      slot->sink->on_heartbeat(slot->instance_id);
    }
    return ok();
  }

  auto cancel(const InstanceId &instance_id) -> void override {
    std::shared_ptr<ActiveSlot> slot;
    {
      std::scoped_lock lock(mu_);
      if (auto it = active_.find(std::string(instance_id.value()));
          it != active_.end()) {
        slot = it->second;
        active_.erase(it);
      }
    }

    cancel_count_.fetch_add(1, std::memory_order_acq_rel);
    if (!slot || !slot->sink) {
      return;
    }

    boost::asio::post(
        runtime_->shard(0).ctx(), [slot = std::move(slot)]() mutable {
          if (!slot->sink->on_complete) {
            return;
          }
          ExecutorResult result;
          result.exit_code = 0;
          slot->sink->on_complete(slot->instance_id, std::move(result));
        });
  }

  [[nodiscard]] auto cancel_count() const -> int {
    return cancel_count_.load(std::memory_order_acquire);
  }

private:
  struct ActiveSlot {
    InstanceId instance_id;
    std::shared_ptr<ExecutionSink> sink;
  };

  Runtime *runtime_;
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<ActiveSlot>> active_;
  std::atomic<int> cancel_count_{0};
};

class BlockingExecutor final : public IExecutor {
public:
  explicit BlockingExecutor(Runtime &runtime) : runtime_(&runtime) {}

  auto start(ExecutorRequest req, ExecutionSink sink) -> Result<void> override {
    auto slot = std::make_shared<ActiveSlot>();
    slot->instance_id = req.instance_id.clone();
    slot->sink = std::make_shared<ExecutionSink>(std::move(sink));
    {
      std::scoped_lock lock(mu_);
      active_.push_back(slot);
      start_count_.fetch_add(1, std::memory_order_acq_rel);
    }
    cv_.notify_all();
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

  [[nodiscard]] auto start_count() const -> int {
    return start_count_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto wait_for_starts(int expected,
                                     std::chrono::milliseconds timeout) -> bool {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [&] {
      return start_count_.load(std::memory_order_acquire) >= expected;
    });
  }

  auto complete_next_success() -> bool {
    std::shared_ptr<ActiveSlot> slot;
    {
      std::scoped_lock lock(mu_);
      if (active_.empty()) {
        return false;
      }
      slot = active_.front();
      active_.pop_front();
    }

    boost::asio::post(
        runtime_->shard(0).ctx(), [slot = std::move(slot)]() mutable {
          if (!slot->sink || !slot->sink->on_complete) {
            return;
          }
          ExecutorResult result;
          result.exit_code = 0;
          slot->sink->on_complete(slot->instance_id, std::move(result));
        });
    return true;
  }

private:
  struct ActiveSlot {
    InstanceId instance_id;
    std::shared_ptr<ExecutionSink> sink;
  };

  Runtime *runtime_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::shared_ptr<ActiveSlot>> active_;
  std::atomic<int> start_count_{0};
};

auto make_single_task_run_context(
    const DAGRunId &dag_run_id,
    ExecutorType executor_type = ExecutorType::Shell)
    -> Result<ExecutionService::RunContext> {
  auto dag = std::make_shared<DAG>();
  if (auto r = dag->add_node(TaskId{"worker"}); !r) {
    return fail(r.error());
  }

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

  TaskConfig task_cfg;
  task_cfg.task_id = TaskId{"worker"};
  task_cfg.name = "worker";
  task_cfg.executor = executor_type;
  task_cfg.command = executor_type == ExecutorType::Sensor ? "" : ":";

  std::vector<ExecutorConfig> executor_configs;
  switch (executor_type) {
  case ExecutorType::Shell:
    executor_configs.emplace_back(ShellExecutorConfig{.env = {}});
    break;
  case ExecutorType::Sensor:
    executor_configs.emplace_back(
        SensorExecutorConfig{.type = SensorType::File, .target = "/tmp/never"});
    break;
  case ExecutorType::Docker:
    executor_configs.emplace_back(DockerExecutorConfig{});
    break;
  case ExecutorType::Noop:
    executor_configs.emplace_back(NoopExecutorConfig{});
    break;
  case ExecutorType::Lua:
    executor_configs.emplace_back(LuaExecutorConfig{.script = "return 0"});
    break;
  }

  std::vector<TaskConfig::Compiled> task_configs;
  task_configs.emplace_back(task_cfg.compiled());

  return ok(ExecutionService::RunContext{
      .run = std::move(run),
      .executor_configs = std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs)),
      .task_configs = std::make_shared<const std::vector<TaskConfig::Compiled>>(
          std::move(task_configs)),
      .dag_id = DAGId{"persist_test_dag"}});
}

} // namespace

TEST(ExecutionServiceTest, CompletedSingleTaskRunPersistsOnlyFinalRunSnapshot) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"persist_test_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Success);
}

TEST(ExecutionServiceTest, CompletedRunSnapshotIsReusedAfterCompletion) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::shared_ptr<const DAGRun> persisted_snapshot;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_snapshot = std::move(run);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"persist_snapshot_reuse_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
    ASSERT_TRUE(static_cast<bool>(persisted_snapshot));
  }

  auto snapshot_res =
      sync_wait_on_runtime(runtime, execution.get_run_snapshot(dag_run_id));
  ASSERT_TRUE(snapshot_res.has_value());
  ASSERT_TRUE(static_cast<bool>(*snapshot_res));
  EXPECT_EQ(snapshot_res->get(), persisted_snapshot.get());

  runtime.stop();
}

TEST(ExecutionServiceTest, TimeoutFailsRunWithoutRetrying) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateTimeoutExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  std::vector<TaskState> persisted_task_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 3; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"timeout_test_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Failed);
  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Failed);
}

TEST(ExecutionServiceTest, ShellExit127DoesNotRetryEvenIfRetriesConfigured) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateExit127Executor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<TaskState> persisted_task_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 3; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"shell_127_run"};
  auto ctx = make_single_task_run_context(dag_run_id, ExecutorType::Shell);
  ASSERT_TRUE(ctx.has_value());
  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Failed);
  EXPECT_EQ(std::count(persisted_task_states.begin(),
                       persisted_task_states.end(), TaskState::Retrying),
            0);
}

TEST(ExecutionServiceTest, SensorDefaultRetryPolicyDoesNotRetry) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateFailureExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<TaskState> persisted_task_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_dag_id_by_run = [](const DAGRunId &) -> task<Result<DAGId>> {
    co_return ok(DAGId{"persist_test_dag"});
  };
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) {
    return task_defaults::kMaxRetries;
  };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"sensor_default_retry_run"};
  auto ctx = make_single_task_run_context(dag_run_id, ExecutorType::Sensor);
  ASSERT_TRUE(ctx.has_value());
  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Failed);
  EXPECT_EQ(std::count(persisted_task_states.begin(),
                       persisted_task_states.end(), TaskState::Retrying),
            0);
}

TEST(ExecutionServiceTest, ConsolidatesRetryWakeupsIntoFewIoTimers) {
  constexpr int kRunCount = 24;

  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateFailureExecutor executor;
  ExecutionService execution(runtime, executor);

  std::atomic<int> retrying_seen{0};

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 1; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(2);
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    if (info.state == TaskState::Retrying) {
      retrying_seen.fetch_add(1, std::memory_order_acq_rel);
    }
  };
  execution.set_callbacks(std::move(callbacks));

  for (int i = 0; i < kRunCount; ++i) {
    const DAGRunId dag_run_id{"retry_depth_run_" + std::to_string(i)};
    auto ctx = make_single_task_run_context(dag_run_id);
    ASSERT_TRUE(ctx.has_value());
    execution.start_run(dag_run_id, std::move(*ctx));
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (retrying_seen.load(std::memory_order_acquire) < kRunCount &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_EQ(retrying_seen.load(std::memory_order_acquire), kRunCount);

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_LE(runtime.io_context_timer_depth(0), 4U);

  runtime.stop();
}

TEST(ExecutionServiceTest, RetryingTaskRestartsExactlyOnceAndSucceeds) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  FailOnceThenSucceedExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  std::vector<TaskState> persisted_task_states;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 1; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"retry_once_success_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  EXPECT_EQ(executor.start_count(), 2);
  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Success);
  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Success);
  EXPECT_NE(std::find(persisted_task_states.begin(),
                      persisted_task_states.end(), TaskState::Retrying),
            persisted_task_states.end());
}

TEST(ExecutionServiceTest, MaxConcurrencyDelaysAdditionalRunsUntilCapacityFrees) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  BlockingExecutor executor(runtime);
  ExecutionService execution(runtime, executor);
  execution.set_max_concurrency(2);

  std::mutex mu;
  std::condition_variable cv;
  int completed = 0;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    ++completed;
    cv.notify_all();
  };
  execution.set_callbacks(std::move(callbacks));

  for (int i = 0; i < 3; ++i) {
    const DAGRunId dag_run_id{"max_concurrency_run_" + std::to_string(i)};
    auto ctx = make_single_task_run_context(dag_run_id);
    ASSERT_TRUE(ctx.has_value());
    execution.start_run(dag_run_id, std::move(*ctx));
  }

  ASSERT_TRUE(executor.wait_for_starts(2, std::chrono::seconds(2)));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(executor.start_count(), 2);

  ASSERT_TRUE(executor.complete_next_success());
  ASSERT_TRUE(executor.wait_for_starts(3, std::chrono::seconds(2)));

  ASSERT_TRUE(executor.complete_next_success());
  ASSERT_TRUE(executor.complete_next_success());

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                            [&] { return completed == 3; }));
  }

  runtime.stop();

  EXPECT_EQ(executor.start_count(), 3);
}

TEST(ExecutionServiceTest, LocalLeaseExpiryFailsRunAndIgnoresLateCompletion) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  HeartbeatThenHangExecutor executor(runtime);
  ExecutionService execution(runtime, executor);
  execution.set_local_task_lease_timeout(std::chrono::milliseconds(120));

  std::mutex mu;
  std::condition_variable cv;
  std::vector<DAGRunState> persisted_states;
  std::vector<TaskState> persisted_task_states;
  std::vector<TaskState> task_statuses;
  int run_status_events = 0;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_persist_run = [&](std::shared_ptr<const DAGRun> run) {
    std::scoped_lock lock(mu);
    persisted_states.push_back(run->state());
  };
  callbacks.on_persist_task = [&](const DAGRunId &,
                                  const TaskInstanceInfo &info) {
    std::scoped_lock lock(mu);
    persisted_task_states.push_back(info.state);
  };
  callbacks.on_task_status = [&](const DAGRunId &, const TaskId &,
                                 TaskState state) {
    std::scoped_lock lock(mu);
    task_statuses.push_back(state);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    ++run_status_events;
    completed = true;
    cv.notify_one();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"lease_expiry_test_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  runtime.stop();

  EXPECT_EQ(executor.cancel_count(), 1);
  EXPECT_EQ(run_status_events, 1);
  ASSERT_EQ(persisted_states.size(), 1U);
  EXPECT_EQ(persisted_states.front(), DAGRunState::Failed);
  ASSERT_FALSE(persisted_task_states.empty());
  EXPECT_EQ(persisted_task_states.back(), TaskState::Failed);
  EXPECT_EQ(std::count(task_statuses.begin(), task_statuses.end(),
                       TaskState::Success),
            0);
}

TEST(ExecutionServiceTest, ExecutorHeartbeatPublishesTaskInstanceKey) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  HeartbeatThenHangExecutor executor(runtime);
  ExecutionService execution(runtime, executor);
  execution.set_local_task_lease_timeout(std::chrono::milliseconds(120));

  std::mutex mu;
  std::condition_variable cv;
  std::optional<TaskInstanceKey> heartbeat_key;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.on_task_heartbeat = [&](const TaskInstanceKey &key) {
    std::scoped_lock lock(mu);
    heartbeat_key = key;
    cv.notify_all();
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_all();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"heartbeat_key_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());
  ctx->run->set_run_rowid(77);
  ASSERT_TRUE(ctx->run->set_task_rowid(0, 91).has_value());
  auto mutable_tasks = std::make_shared<std::vector<TaskConfig::Compiled>>(
      *ctx->task_configs);
  (*mutable_tasks)[0].task_rowid = 91;
  ctx->task_configs = mutable_tasks;

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(2),
                            [&] { return heartbeat_key.has_value(); }));
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_TRUE(heartbeat_key.has_value());
  EXPECT_EQ(heartbeat_key->run_rowid, 77);
  EXPECT_EQ(heartbeat_key->task_rowid, 91);
  EXPECT_EQ(heartbeat_key->attempt, 1);
}

TEST(ExecutionServiceTest, DependsOnPastCallbackUsesTaskRowidFromRunSnapshot) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  ImmediateSuccessExecutor executor;
  ExecutionService execution(runtime, executor);

  std::mutex mu;
  std::condition_variable cv;
  std::optional<std::int64_t> observed_task_rowid;
  bool completed = false;

  ExecutionCallbacks callbacks;
  callbacks.get_max_retries = [](const DAGRunId &, NodeIndex) { return 0; };
  callbacks.get_retry_interval = [](const DAGRunId &, NodeIndex) {
    return std::chrono::seconds(0);
  };
  callbacks.check_previous_task_state =
      [&](std::int64_t task_rowid,
          std::chrono::system_clock::time_point,
          const DAGRunId &) -> task<Result<TaskState>> {
    std::scoped_lock lock(mu);
    observed_task_rowid = task_rowid;
    co_return ok(TaskState::Success);
  };
  callbacks.on_run_status = [&](const DAGRunId &, DAGRunState) {
    std::scoped_lock lock(mu);
    completed = true;
    cv.notify_all();
  };
  execution.set_callbacks(std::move(callbacks));

  const DAGRunId dag_run_id{"depends_on_past_rowid_run"};
  auto ctx = make_single_task_run_context(dag_run_id);
  ASSERT_TRUE(ctx.has_value());
  auto mutable_tasks = std::make_shared<std::vector<TaskConfig::Compiled>>(
      *ctx->task_configs);
  (*mutable_tasks)[0].depends_on_past = true;
  (*mutable_tasks)[0].task_rowid = 314;
  ctx->task_configs = mutable_tasks;
  ASSERT_TRUE(ctx->run->set_task_rowid(0, 314).has_value());

  execution.start_run(dag_run_id, std::move(*ctx));

  {
    std::unique_lock lock(mu);
    ASSERT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(2), [&] { return completed; }));
  }

  runtime.stop();

  ASSERT_TRUE(observed_task_rowid.has_value());
  EXPECT_EQ(*observed_task_rowid, 314);
}
