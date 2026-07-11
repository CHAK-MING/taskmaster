#include "dagforge/core/runtime.hpp"
#include "dagforge/scheduler/cron.hpp"
#include "dagforge/scheduler/engine.hpp"
#include "dagforge/util/id.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "gtest/gtest.h"

using namespace dagforge;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(10);
constexpr int kMultipleTaskCount = 10;

} // namespace

class EngineTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(2);
    ASSERT_TRUE(runtime_->start().has_value());
    engine_ = std::make_unique<Engine>(*runtime_);
  }

  void TearDown() override {
    if (engine_) {
      engine_->stop();
      engine_.reset();
    }
    if (runtime_) {
      runtime_->stop();
      runtime_.reset();
    }
  }

  auto make_execution_info(std::string dag, std::string task) -> ExecutionInfo {
    ExecutionInfo info;
    info.dag_id = DAGId(std::move(dag));
    info.task_id = TaskId(std::move(task));
    info.name = info.task_id.str();
    return info;
  }

  auto make_execution_info_with_cron(std::string dag, std::string task,
                                     const std::string &cron_str)
      -> ExecutionInfo {
    auto info = make_execution_info(std::move(dag), std::move(task));
    if (auto cron = CronExpr::parse(cron_str)) {
      info.cron_expr = *cron;
    }
    return info;
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<Engine> engine_;
};

TEST_F(EngineTest, BasicInitialization) { EXPECT_FALSE(engine_->is_running()); }

TEST_F(EngineTest, StartStop) {
  engine_->start();
  EXPECT_TRUE(engine_->is_running());

  engine_->stop();
  EXPECT_FALSE(engine_->is_running());
}

TEST_F(EngineTest, AddTask) {
  engine_->start();

  auto info =
      make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  auto result = engine_->add_task(info);
  EXPECT_TRUE(result);
}

TEST_F(EngineTest, AddTaskWithoutCron) {
  engine_->start();

  auto info = make_execution_info("test_dag", "test_task");
  auto result = engine_->add_task(info);
  EXPECT_TRUE(result);
}

TEST_F(EngineTest, RemoveTask) {
  engine_->start();

  auto info =
      make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  ASSERT_TRUE(engine_->add_task(info));

  auto result = engine_->remove_task(DAGId("test_dag"), TaskId("test_task"));
  EXPECT_TRUE(result);
}

TEST_F(EngineTest, RemoveNonExistentTask) {
  engine_->start();

  // remove_task reports successful submission, not whether the task exists.
  auto result =
      engine_->remove_task(DAGId("nonexistent"), TaskId("nonexistent"));
  EXPECT_TRUE(result);
}

TEST_F(EngineTest, AddMultipleTasks) {
  engine_->start();

  for (int i = 0; i < kMultipleTaskCount; ++i) {
    auto info = make_execution_info_with_cron(
        "test_dag", "task_" + std::to_string(i), "* * * * *");
    EXPECT_TRUE(engine_->add_task(info));
  }
}

TEST_F(EngineTest, RunningFlag) {
  EXPECT_FALSE(engine_->is_running());

  engine_->start();
  EXPECT_TRUE(engine_->is_running());

  std::this_thread::sleep_for(kPollInterval);
  EXPECT_TRUE(engine_->is_running());

  engine_->stop();
  EXPECT_FALSE(engine_->is_running());
}

TEST_F(EngineTest, SetOnDagTriggerCallback) {
  engine_->start();

  std::atomic<bool> callback_set = false;
  engine_->set_on_dag_trigger(
      [&callback_set](const DAGId &, std::chrono::system_clock::time_point) {
        callback_set.store(true);
      });

  EXPECT_FALSE(callback_set.load());
}

TEST_F(EngineTest, RoutesWorkToConfiguredOwnerShard) {
  Engine owner_engine(*runtime_, shard_id{1});
  auto observed_shard = std::make_shared<std::atomic<shard_id>>(kInvalidShard);
  owner_engine.set_get_watermark_callback(
      [runtime = runtime_.get(), observed_shard](const DAGId &)
          -> boost::asio::awaitable<
              Result<std::optional<std::chrono::system_clock::time_point>>> {
        observed_shard->store(runtime->current_shard(),
                              std::memory_order_release);
        co_return ok(std::optional<std::chrono::system_clock::time_point>{});
      });
  owner_engine.start();

  auto info =
      make_execution_info_with_cron("owner_dag", "schedule", "* * * * *");
  ASSERT_TRUE(owner_engine.add_task(std::move(info)));

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (observed_shard->load(std::memory_order_acquire) == kInvalidShard &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  owner_engine.stop();
  EXPECT_EQ(observed_shard->load(std::memory_order_acquire), shard_id{1});
}

TEST_F(EngineTest, UsesRuntimeTimingWheelForCronSchedule) {
  Engine owner_engine(*runtime_, shard_id{1});
  owner_engine.start();

  auto info =
      make_execution_info_with_cron("wheel_dag", "schedule", "* * * * *");
  ASSERT_TRUE(owner_engine.add_task(std::move(info)));

  const auto schedule_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (runtime_->timing_wheel_pending_count(shard_id{1}) == 0 &&
         std::chrono::steady_clock::now() < schedule_deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_EQ(runtime_->timing_wheel_pending_count(shard_id{1}), 1U);
  EXPECT_EQ(owner_engine.scheduled_task_count(), 1U);
  ASSERT_TRUE(owner_engine.remove_task(DAGId{"wheel_dag"}, TaskId{"schedule"}));

  const auto cancel_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while ((runtime_->timing_wheel_pending_count(shard_id{1}) != 0 ||
          owner_engine.scheduled_task_count() != 0) &&
         std::chrono::steady_clock::now() < cancel_deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  owner_engine.stop();
  EXPECT_EQ(runtime_->timing_wheel_pending_count(shard_id{1}), 0U);
  EXPECT_EQ(owner_engine.scheduled_task_count(), 0U);
}

TEST_F(EngineTest, AddDuplicateTask) {
  engine_->start();

  auto info =
      make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  EXPECT_TRUE(engine_->add_task(info));
  EXPECT_TRUE(engine_->add_task(info));
}

TEST_F(EngineTest, RemoveAfterAdd) {
  engine_->start();

  auto dag_id = DAGId("test_dag");
  auto task_id = TaskId("test_task");

  auto info =
      make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  ASSERT_TRUE(engine_->add_task(info));

  // Both calls report successful submission; actual removal is asynchronous.
  EXPECT_TRUE(engine_->remove_task(dag_id, task_id));
  EXPECT_TRUE(engine_->remove_task(dag_id, task_id));
}

TEST_F(EngineTest, CatchupUsesBatchedExistenceCallbackWhenAvailable) {
  engine_->start();

  std::atomic<int> batch_calls = 0;
  std::atomic<int> single_calls = 0;
  std::atomic<int> trigger_calls = 0;

  engine_->set_run_exists_callback(
      [&single_calls](const DAGId &, std::chrono::system_clock::time_point)
          -> boost::asio::awaitable<Result<bool>> {
        single_calls.fetch_add(1, std::memory_order_relaxed);
        co_return ok(false);
      });
  engine_->set_list_run_execution_dates_callback(
      [&batch_calls](const DAGId &, std::chrono::system_clock::time_point,
                     std::chrono::system_clock::time_point)
          -> boost::asio::awaitable<
              Result<std::vector<std::chrono::system_clock::time_point>>> {
        batch_calls.fetch_add(1, std::memory_order_relaxed);
        co_return ok(std::vector<std::chrono::system_clock::time_point>{});
      });
  engine_->set_on_dag_trigger(
      [&trigger_calls](const DAGId &, std::chrono::system_clock::time_point) {
        trigger_calls.fetch_add(1, std::memory_order_relaxed);
      });

  auto now = std::chrono::system_clock::now();
  auto info =
      make_execution_info_with_cron("catchup_dag", "schedule", "* * * * *");
  ASSERT_TRUE(info.cron_expr.has_value());
  info.catchup = true;
  info.start_date = now - std::chrono::minutes(3);

  const auto expected_first = info.cron_expr->next_after(*info.start_date);
  int expected_runs = 0;
  for (auto ts = expected_first; ts <= now; ts = info.cron_expr->next_after(ts)) {
    ++expected_runs;
  }
  ASSERT_GT(expected_runs, 0);

  ASSERT_TRUE(engine_->add_task(info));

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (trigger_calls.load(std::memory_order_relaxed) < expected_runs &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_EQ(batch_calls.load(std::memory_order_relaxed), 1);
  EXPECT_EQ(single_calls.load(std::memory_order_relaxed), 0);
  EXPECT_EQ(trigger_calls.load(std::memory_order_relaxed), expected_runs);
}
