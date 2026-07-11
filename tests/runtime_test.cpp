#include "dagforge/core/runtime.hpp"

#include <atomic>
#include <chrono>
#include <thread>

#include "gtest/gtest.h"

using namespace dagforge;

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(1);
constexpr auto kTaskTimeout = std::chrono::seconds(1);
constexpr auto kRunningCheckDelay = std::chrono::milliseconds(50);

auto increment_counter(std::atomic<int> *count_ptr) -> spawn_task {
  count_ptr->fetch_add(1);
  co_return;
}

} // namespace

TEST(RuntimeTest, BasicStartStop) {
  Runtime rt(1);
  EXPECT_FALSE(rt.is_running());
  ASSERT_TRUE(rt.start().has_value());
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, MultiShard) {
  Runtime rt(4);
  ASSERT_TRUE(rt.start().has_value());
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
  ASSERT_TRUE(rt.start().has_value());
  EXPECT_TRUE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, CurrentShardReturnsInvalidOutsideContext) {
  Runtime rt(2);
  ASSERT_TRUE(rt.start().has_value());

  auto shard_id = rt.current_shard();
  EXPECT_EQ(shard_id, kInvalidShard);

  rt.stop();
}

TEST(RuntimeTest, IsCurrentShardFalseOutsideContext) {
  Runtime rt(1);
  ASSERT_TRUE(rt.start().has_value());

  EXPECT_FALSE(rt.is_current_shard());

  rt.stop();
}

TEST(RuntimeTest, ZeroShardsDefaultsToOne) {
  Runtime rt(0);
  ASSERT_TRUE(rt.start().has_value());
  EXPECT_GE(rt.shard_count(), 1);
  rt.stop();
}

TEST(RuntimeTest, MultipleStartStops) {
  Runtime rt(1);

  ASSERT_TRUE(rt.start().has_value());
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());

  ASSERT_TRUE(rt.start().has_value());
  EXPECT_TRUE(rt.is_running());
  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, GetShardIdValid) {
  Runtime rt(1);
  ASSERT_TRUE(rt.start().has_value());

  // Inside a shard coroutine, current_shard() returns the shard index.
  std::atomic<uint32_t> observed{kInvalidShard};
  auto check = [&observed, &rt]() -> spawn_task {
    observed.store(rt.current_shard());
    co_return;
  };
  rt.spawn_on(0, check());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (observed.load() == kInvalidShard &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  EXPECT_EQ(observed.load(), 0U);

  rt.stop();
}

TEST(RuntimeTest, ScheduleExternalRunsTask) {
  Runtime rt(2);
  ASSERT_TRUE(rt.start().has_value());

  std::atomic<int> count = 0;
  auto t = increment_counter(&count);
  rt.spawn_external(std::move(t));

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

  ASSERT_TRUE(rt.start().has_value());
  EXPECT_TRUE(rt.is_running());

  std::this_thread::sleep_for(kRunningCheckDelay);

  EXPECT_TRUE(rt.is_running());

  rt.stop();
  EXPECT_FALSE(rt.is_running());
}

TEST(RuntimeTest, ScheduleAfterStop_IsNoOp) {
  Runtime rt(1);
  ASSERT_TRUE(rt.start().has_value());
  rt.stop();

  std::atomic<int> count = 0;
  auto t = increment_counter(&count);
  rt.spawn_external(std::move(t));

  std::this_thread::sleep_for(kRunningCheckDelay);
  EXPECT_EQ(count.load(), 0);
}

TEST(RuntimeTest, SpawnOnTargetShard_FromExternalContext) {
  Runtime rt(2);
  ASSERT_TRUE(rt.start().has_value());

  std::atomic<uint32_t> target_observed{kInvalidShard};

  auto on_target = [&]() -> spawn_task {
    target_observed.store(rt.current_shard(), std::memory_order_relaxed);
    co_return;
  };

  rt.spawn_on(1, on_target());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (target_observed.load(std::memory_order_relaxed) == kInvalidShard &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_EQ(target_observed.load(std::memory_order_relaxed), 1U);

  rt.stop();
}

TEST(RuntimeTest, CrossShardQueueOverflowPreservesTargetOwnership) {
  constexpr int kPostedTasks = 5000;
  constexpr shard_id kSourceShard = 0;
  constexpr shard_id kTargetShard = 1;

  Runtime rt(2);
  ASSERT_TRUE(rt.start().has_value());

  std::atomic<bool> target_blocked{false};
  std::atomic<bool> release_target{false};
  std::atomic<bool> producer_done{false};
  std::atomic<int> completed{0};
  std::atomic<int> wrong_shard{0};

  rt.post_to(kTargetShard, [&] {
    target_blocked.store(true, std::memory_order_release);
    while (!release_target.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!target_blocked.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(target_blocked.load(std::memory_order_acquire));

  rt.post_to(kSourceShard, [&] {
    for (int i = 0; i < kPostedTasks; ++i) {
      rt.post_to(kTargetShard, [&] {
        if (!rt.is_current_shard() ||
            rt.current_shard() != kTargetShard) {
          wrong_shard.fetch_add(1, std::memory_order_relaxed);
        }
        completed.fetch_add(1, std::memory_order_release);
      });
    }
    producer_done.store(true, std::memory_order_release);
    release_target.store(true, std::memory_order_release);
  });

  deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((!producer_done.load(std::memory_order_acquire) ||
          completed.load(std::memory_order_acquire) != kPostedTasks) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_TRUE(producer_done.load(std::memory_order_acquire));
  EXPECT_EQ(completed.load(std::memory_order_acquire), kPostedTasks);
  EXPECT_GT(rt.cross_shard_queue_overflow_total(kSourceShard, kTargetShard),
            0U);
  EXPECT_EQ(wrong_shard.load(std::memory_order_relaxed), 0);

  release_target.store(true, std::memory_order_release);
  rt.stop();
}
