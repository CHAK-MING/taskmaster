#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/scheduler/cron.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

#include "gtest/gtest.h"

using namespace taskmaster;

class RecoveryTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Setup temporary DB
    db_path_ = "test_recovery.db";
    if (std::filesystem::exists(db_path_)) {
      std::filesystem::remove(db_path_);
    }
    persistence_ = std::make_unique<Persistence>(db_path_);
    ASSERT_TRUE(persistence_->open());

    runtime_ = std::make_unique<Runtime>(2);
    runtime_->start();
    // Inject persistence
    engine_ = std::make_unique<Engine>(*runtime_, persistence_.get());
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
    if (persistence_) {
      persistence_->close();
      persistence_.reset();
    }
    if (std::filesystem::exists(db_path_)) {
      std::filesystem::remove(db_path_);
    }
  }

  auto make_execution_info_with_cron(std::string dag, std::string task,
                                      const std::string& cron_str, bool catchup = true) -> ExecutionInfo {
    ExecutionInfo info;
    info.dag_id = DAGId(std::move(dag));
    info.task_id = TaskId(std::move(task));
    info.name = info.task_id.str();
    if (auto cron = CronExpr::parse(cron_str)) {
      info.cron_expr = *cron;
    }
    info.catchup = catchup;
    info.start_date = std::chrono::system_clock::now() - std::chrono::hours(1); // Ensure start date is old enough
    return info;
  }

  std::string db_path_;
  std::unique_ptr<Persistence> persistence_;
  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<Engine> engine_;
};

TEST_F(RecoveryTest, CatchupRunsScheduled) {
  engine_->start();
  std::jthread runner([this] { engine_->run_loop(); });

  auto dag_id = DAGId("test_dag");

  // Set watermark 10 minutes in the past
  auto now = std::chrono::system_clock::now();
  auto past_time = now - std::chrono::minutes(10);
  
  ASSERT_TRUE(persistence_->save_watermark(dag_id, past_time));

  // Count triggers
  std::atomic<int> trigger_count = 0;
  engine_->set_on_dag_trigger(
      [&](const DAGId& id, std::chrono::system_clock::time_point) {
        if (id == dag_id) {
          trigger_count++;
        }
      });

  // Add DAG with every minute schedule
  auto info = make_execution_info_with_cron("test_dag", "schedule_task", "* * * * *");
  engine_->add_task(info);

  // Wait for catchup runs to process
  int max_wait = 20; // 2 seconds
  while (trigger_count < 9 && max_wait-- > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  
  EXPECT_GE(trigger_count, 9);
  
  // Verify watermark updated
  auto watermark = persistence_->get_watermark(dag_id);
  ASSERT_TRUE(watermark.has_value());
  ASSERT_TRUE(watermark.value().has_value());
  
  auto stored_watermark = *watermark.value();
  
  auto diff = std::chrono::duration_cast<std::chrono::minutes>(now - stored_watermark);
  EXPECT_LT(diff.count(), 2);
  
  engine_->stop();
}

TEST_F(RecoveryTest, NoCatchupIfDisabled) {
  engine_->start();
  std::jthread runner([this] { engine_->run_loop(); });

  auto dag_id = DAGId("no_catchup_dag");

  // Set watermark 10 minutes in the past
  auto now = std::chrono::system_clock::now();
  auto past_time = now - std::chrono::minutes(10);
  
  ASSERT_TRUE(persistence_->save_watermark(dag_id, past_time));

  std::atomic<int> trigger_count = 0;
  engine_->set_on_dag_trigger(
      [&](const DAGId& id, std::chrono::system_clock::time_point) {
        if (id == dag_id) {
          trigger_count++;
        }
      });

  auto info = make_execution_info_with_cron("no_catchup_dag", "schedule_task", "* * * * *", false);
  engine_->add_task(info);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  
  EXPECT_EQ(trigger_count, 0);
  engine_->stop();
}
