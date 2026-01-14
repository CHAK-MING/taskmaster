#include "taskmaster/core/runtime.hpp"

#include <atomic>
#include <thread>
#include <chrono>

#include "gtest/gtest.h"

using namespace taskmaster;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(1);
constexpr auto kTaskTimeout = std::chrono::seconds(1);
constexpr auto kRunningCheckDelay = std::chrono::milliseconds(50);

auto increment_counter(std::atomic<int>* count_ptr) -> spawn_task {
  count_ptr->fetch_add(1);
  co_return;
}

}  // namespace

TEST(RuntimeTest, BasicStartStop) {
  Runtime rt(1);
  EXPECT_FALSE(rt.is_running());
  rt.start();
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, MultiShard) {
  Runtime rt(4);
  rt.start();
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, ShardCount) {
  Runtime rt(1);
  EXPECT_EQ(rt.shard_count(), 1);

  Runtime rt4(4);
  EXPECT_EQ(rt4.shard_count(), 4);
}

TEST(RuntimeTest, StopStopsRuntime) {
  Runtime rt(1);
  rt.start();
  EXPECT_TRUE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, CurrentShardReturnsInvalidOutsideContext) {
  Runtime rt(2);
  rt.start();

  auto shard_id = rt.current_shard();
  EXPECT_EQ(shard_id, kInvalidShard);

  rt.stop();
}

TEST(RuntimeTest, IsCurrentShardFalseOutsideContext) {
  Runtime rt(1);
  rt.start();

  EXPECT_FALSE(rt.is_current_shard());

  rt.stop();
}

TEST(RuntimeTest, ZeroShardsDefaultsToOne) {
  Runtime rt(0);
  rt.start();
  EXPECT_GE(rt.shard_count(), 1);
  rt.stop();
}

TEST(RuntimeTest, MultipleStartStops) {
  Runtime rt(1);

  rt.start();
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());

  rt.start();
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, GetShardIdValid) {
  Runtime rt(1);
  rt.start();

  // Outside of shard context, get_shard_id() returns 0.
  EXPECT_EQ(rt.get_shard_id(), 0u);

  rt.stop();
}

TEST(RuntimeTest, ScheduleExternalRunsTask) {
  Runtime rt(2);
  rt.start();

  std::atomic<int> count = 0;
  auto t = increment_counter(&count);
  rt.schedule_external(t.take());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (count.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  EXPECT_EQ(count.load(), 1);

  rt.stop();
}

TEST(RuntimeTest, RunningFlagSetCorrectly) {
  Runtime rt(2);

  EXPECT_FALSE(rt.is_running());

  rt.start();
  EXPECT_TRUE(rt.is_running());

  std::this_thread::sleep_for(kRunningCheckDelay);

  EXPECT_TRUE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, ScheduleAfterStop_IsNoOp) {
  Runtime rt(1);
  rt.start();
  rt.stop();

  std::atomic<int> count = 0;
  auto t = increment_counter(&count);
  rt.schedule_external(t.take());

  std::this_thread::sleep_for(kRunningCheckDelay);
  EXPECT_EQ(count.load(), 0);
}
