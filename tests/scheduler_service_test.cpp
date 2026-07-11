#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/core/runtime.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

#include <atomic>

using namespace dagforge;
using namespace std::chrono_literals;

namespace {

auto make_cron_dag(std::string_view dag_id, std::string_view cron)
    -> DAGInfo {
  DAGInfo info;
  info.dag_id = DAGId{std::string(dag_id)};
  info.name = std::string(dag_id);
  info.cron = std::string(cron);
  return info;
}

} // namespace

class SchedulerServiceTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    ASSERT_TRUE(runtime_->start().has_value());
    scheduler_ = std::make_unique<SchedulerService>(*runtime_, 1);
  }

  void TearDown() override {
    scheduler_.reset();
    if (runtime_) {
      runtime_->stop();
      runtime_.reset();
    }
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<SchedulerService> scheduler_;
};

TEST_F(SchedulerServiceTest, RegisterDagWhileStoppedDoesNotQueueTask) {
  scheduler_->register_dag(DAGId{"stopped_dag"},
                           make_cron_dag("stopped_dag", "* * * * *"));
  EXPECT_EQ(scheduler_->queue_depth(), 0U);
}

TEST_F(SchedulerServiceTest, InvalidCronIncrementsParseErrorsWithoutScheduling) {
  scheduler_->start();

  scheduler_->register_dag(DAGId{"bad_cron_dag"},
                           make_cron_dag("bad_cron_dag", "not-a-cron"));

  EXPECT_EQ(scheduler_->cron_parse_errors_total(), 1U);
  EXPECT_EQ(scheduler_->queue_depth(), 0U);
}

TEST_F(SchedulerServiceTest, RegisterAndUnregisterDagUpdatesQueueDepth) {
  scheduler_->start();

  const DAGId dag_id{"scheduled_dag"};
  scheduler_->register_dag(dag_id, make_cron_dag(dag_id.value(), "* * * * *"));

  ASSERT_TRUE(test::poll_until(
      [&]() { return scheduler_->queue_depth() == 1U; },
      std::chrono::seconds(2), std::chrono::milliseconds(10)));

  scheduler_->unregister_dag(dag_id);

  ASSERT_TRUE(test::poll_until(
      [&]() { return scheduler_->queue_depth() == 0U; },
      std::chrono::seconds(2), std::chrono::milliseconds(10)));
}

TEST_F(SchedulerServiceTest, ZombieReaperCallbackRunsWhenEnabled) {
  std::atomic<int> calls{0};
  std::atomic<std::int64_t> last_timeout_ms{0};

  scheduler_->set_zombie_reaper_config(1, 2);
  scheduler_->set_zombie_reaper_callback(
      [&](std::int64_t timeout_ms) -> task<Result<std::size_t>> {
        last_timeout_ms.store(timeout_ms, std::memory_order_release);
        calls.fetch_add(1, std::memory_order_acq_rel);
        co_return ok<std::size_t>(1);
      });

  scheduler_->start();
  ASSERT_TRUE(test::poll_until(
      [&]() { return calls.load(std::memory_order_acquire) >= 1; }, 2500ms,
      20ms));

  ASSERT_TRUE(scheduler_->stop().has_value());

  EXPECT_EQ(last_timeout_ms.load(std::memory_order_acquire), 2000);
}

TEST_F(SchedulerServiceTest, StopPreventsAdditionalZombieReaperCallbacks) {
  std::atomic<int> calls{0};

  scheduler_->set_zombie_reaper_config(1, 1);
  scheduler_->set_zombie_reaper_callback(
      [&](std::int64_t) -> task<Result<std::size_t>> {
        calls.fetch_add(1, std::memory_order_acq_rel);
        co_return ok<std::size_t>(0);
      });

  scheduler_->start();
  ASSERT_TRUE(test::poll_until(
      [&]() { return calls.load(std::memory_order_acquire) >= 1; }, 2500ms,
      20ms));

  ASSERT_TRUE(scheduler_->stop().has_value());
  const auto after_stop = calls.load(std::memory_order_acquire);
  std::this_thread::sleep_for(1200ms);

  EXPECT_EQ(calls.load(std::memory_order_acquire), after_stop);
}
