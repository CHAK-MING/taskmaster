#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/scheduler/cron.hpp"
#include "taskmaster/util/id.hpp"

#include <atomic>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"

using namespace taskmaster;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(10);
constexpr int kMultipleTaskCount = 10;

}  // namespace

class EngineTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(2);
    runtime_->start();
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
                                      const std::string& cron_str) -> ExecutionInfo {
    auto info = make_execution_info(std::move(dag), std::move(task));
    if (auto cron = CronExpr::parse(cron_str)) {
      info.cron_expr = *cron;
    }
    return info;
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<Engine> engine_;
};

TEST_F(EngineTest, BasicInitialization) {
  EXPECT_FALSE(engine_->is_running());
}

TEST_F(EngineTest, StartStop) {
  engine_->start();
  std::jthread runner([this] { engine_->run_loop(); });
  EXPECT_TRUE(engine_->is_running());

  engine_->stop();
  EXPECT_FALSE(engine_->is_running());
}

TEST_F(EngineTest, AddTask) {
  engine_->start();

  auto info = make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
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

  auto info = make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  ASSERT_TRUE(engine_->add_task(info));

  auto result = engine_->remove_task(DAGId("test_dag"), TaskId("test_task"));
  EXPECT_TRUE(result);
}

TEST_F(EngineTest, RemoveNonExistentTask) {
  engine_->start();

  // remove_task returns true if event was queued, not if task exists
  auto result = engine_->remove_task(DAGId("nonexistent"), TaskId("nonexistent"));
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
  std::jthread runner([this] { engine_->run_loop(); });
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
      [&callback_set](const DAGId&, std::chrono::system_clock::time_point) { 
        callback_set.store(true); 
      });

  EXPECT_FALSE(callback_set.load());
}

TEST_F(EngineTest, AddDuplicateTask) {
  engine_->start();

  auto info = make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  EXPECT_TRUE(engine_->add_task(info));
  EXPECT_TRUE(engine_->add_task(info));
}

TEST_F(EngineTest, RemoveAfterAdd) {
  engine_->start();

  auto dag_id = DAGId("test_dag");
  auto task_id = TaskId("test_task");

  auto info = make_execution_info_with_cron("test_dag", "test_task", "* * * * *");
  ASSERT_TRUE(engine_->add_task(info));

  // Both calls return true - they queue events, actual removal is async
  EXPECT_TRUE(engine_->remove_task(dag_id, task_id));
  EXPECT_TRUE(engine_->remove_task(dag_id, task_id));
}
