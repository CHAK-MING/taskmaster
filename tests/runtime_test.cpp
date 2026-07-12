#include "dagforge/core/runtime.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

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

TEST(ComputePoolTest, ExecutesWorkAndPublishesMetrics) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 4});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<int> value{0};
  auto submitted = pool.submit(
      {}, [&value](std::stop_token) { value.store(42); });
  ASSERT_TRUE(submitted.has_value());

  pool.stop(ComputeShutdownMode::Drain);

  EXPECT_EQ(value.load(), 42);
  const auto snapshot = pool.snapshot();
  EXPECT_EQ(snapshot.thread_count, 1U);
  EXPECT_EQ(snapshot.submitted_total, 1U);
  EXPECT_EQ(snapshot.completed_total, 1U);
  EXPECT_EQ(snapshot.queue_wait_time.count, 1U);
  EXPECT_EQ(snapshot.execution_time.count, 1U);
}

TEST(ComputePoolTest, RejectsWhenBoundedQueueIsFull) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 1});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  auto running = pool.submit({}, [&](std::stop_token) {
    started.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  ASSERT_TRUE(running.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(started.load(std::memory_order_acquire));

  auto queued = pool.submit({}, [](std::stop_token) {});
  ASSERT_TRUE(queued.has_value());
  auto rejected = pool.submit({}, [](std::stop_token) {});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error(), make_error_code(Error::ResourceExhausted));

  release.store(true, std::memory_order_release);
  pool.stop(ComputeShutdownMode::Drain);
  EXPECT_EQ(pool.snapshot().rejected_total, 1U);
}

TEST(ComputePoolTest, CancelsQueuedTaskBeforeExecution) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 2});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  auto running = pool.submit({}, [&](std::stop_token) {
    started.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  ASSERT_TRUE(running.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(started.load(std::memory_order_acquire));

  std::atomic<bool> executed{false};
  std::atomic<int> discarded_error{-1};
  auto queued = pool.submit(
      {}, [&executed](std::stop_token) { executed.store(true); },
      [&discarded_error](Error error) {
        discarded_error.store(static_cast<int>(error),
                              std::memory_order_release);
      });
  ASSERT_TRUE(queued.has_value());
  EXPECT_TRUE(queued->request_stop());

  release.store(true, std::memory_order_release);
  pool.stop(ComputeShutdownMode::Drain);

  EXPECT_FALSE(executed.load());
  EXPECT_EQ(discarded_error.load(std::memory_order_acquire),
            static_cast<int>(Error::Cancelled));
  EXPECT_EQ(pool.snapshot().cancelled_total, 1U);
}

TEST(ComputePoolTest, ExpiresQueuedTaskAtDeadline) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 2});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  auto running = pool.submit({}, [&](std::stop_token) {
    started.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  ASSERT_TRUE(running.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(started.load(std::memory_order_acquire));

  std::atomic<int> discarded_error{-1};
  ComputeOptions options{
      .priority = ComputePriority::Normal,
      .start_deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(10),
  };
  auto queued = pool.submit(
      options, [](std::stop_token) {},
      [&discarded_error](Error error) {
        discarded_error.store(static_cast<int>(error),
                              std::memory_order_release);
      });
  ASSERT_TRUE(queued.has_value());

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  release.store(true, std::memory_order_release);
  pool.stop(ComputeShutdownMode::Drain);

  EXPECT_EQ(discarded_error.load(std::memory_order_acquire),
            static_cast<int>(Error::Timeout));
  EXPECT_EQ(pool.snapshot().timed_out_total, 1U);
}

TEST(ComputePoolTest, CancelPendingStopsActiveAndDiscardsQueuedWork) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 2});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<bool> active_started{false};
  std::atomic<bool> active_observed_stop{false};
  auto active = pool.submit({}, [&](std::stop_token stop_token) {
    active_started.store(true, std::memory_order_release);
    while (!stop_token.stop_requested()) {
      std::this_thread::yield();
    }
    active_observed_stop.store(true, std::memory_order_release);
  });
  ASSERT_TRUE(active.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!active_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(active_started.load(std::memory_order_acquire));

  std::atomic<int> queued_discard{-1};
  auto queued = pool.submit(
      {}, [](std::stop_token) {},
      [&queued_discard](Error error) {
        queued_discard.store(static_cast<int>(error),
                             std::memory_order_release);
      });
  ASSERT_TRUE(queued.has_value());

  pool.stop(ComputeShutdownMode::CancelPending);

  EXPECT_TRUE(active_observed_stop.load(std::memory_order_acquire));
  EXPECT_EQ(queued_discard.load(std::memory_order_acquire),
            static_cast<int>(Error::Cancelled));
  EXPECT_EQ(pool.snapshot().cancelled_total, 1U);
}

TEST(ComputePoolTest, RunsHigherPriorityQueuedWorkFirst) {
  ComputePool pool(ComputePoolConfig{.thread_count = 1, .queue_capacity = 4});
  ASSERT_TRUE(pool.start().has_value());

  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  auto running = pool.submit({}, [&](std::stop_token) {
    started.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  });
  ASSERT_TRUE(running.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }
  ASSERT_TRUE(started.load(std::memory_order_acquire));

  std::mutex order_mutex;
  std::vector<int> order;
  auto low = pool.submit(
      ComputeOptions{.priority = ComputePriority::Low},
      [&](std::stop_token) {
        std::lock_guard lock(order_mutex);
        order.push_back(1);
      });
  auto critical = pool.submit(
      ComputeOptions{.priority = ComputePriority::Critical},
      [&](std::stop_token) {
        std::lock_guard lock(order_mutex);
        order.push_back(2);
      });
  ASSERT_TRUE(low.has_value());
  ASSERT_TRUE(critical.has_value());

  release.store(true, std::memory_order_release);
  pool.stop(ComputeShutdownMode::Drain);

  ASSERT_EQ(order.size(), 2U);
  EXPECT_EQ(order[0], 2);
  EXPECT_EQ(order[1], 1);
}

TEST(RuntimeTest, ComputeCompletionReturnsToOwnerShard) {
  Runtime rt(2, false, 0,
             ComputePoolConfig{.thread_count = 1, .queue_capacity = 8});
  ASSERT_TRUE(rt.start().has_value());

  struct State {
    std::atomic<bool> done{false};
    std::atomic<bool> work_ran_on_shard{true};
    std::atomic<shard_id> completion_shard{kInvalidShard};
    std::atomic<int> value{0};
    std::atomic<int> error{-1};
  };
  auto state = std::make_shared<State>();

  auto submitted = rt.submit_compute(
      1, {},
      [&rt, state](std::stop_token) -> Result<int> {
        state->work_ran_on_shard.store(rt.is_current_shard(),
                                      std::memory_order_release);
        return ok(42);
      },
      [&rt, state](Result<int> result) {
        state->completion_shard.store(rt.current_shard(),
                                      std::memory_order_release);
        if (result) {
          state->value.store(*result, std::memory_order_release);
        } else {
          state->error.store(result.error().value(),
                             std::memory_order_release);
        }
        state->done.store(true, std::memory_order_release);
      });
  ASSERT_TRUE(submitted.has_value());

  auto deadline = std::chrono::steady_clock::now() + kTaskTimeout;
  while (!state->done.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(kPollInterval);
  }

  EXPECT_TRUE(state->done.load(std::memory_order_acquire));
  EXPECT_FALSE(state->work_ran_on_shard.load(std::memory_order_acquire));
  EXPECT_EQ(state->completion_shard.load(std::memory_order_acquire), 1U);
  EXPECT_EQ(state->value.load(std::memory_order_acquire), 42);
  EXPECT_EQ(state->error.load(std::memory_order_acquire), -1);
  EXPECT_EQ(rt.compute_pool_snapshot().submitted_total, 1U);

  rt.stop();
}

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
