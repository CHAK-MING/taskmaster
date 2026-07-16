#include "dagforge/core/runtime.hpp"

#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>

#ifdef __linux__
#include <sched.h>
#endif

namespace dagforge::test {
namespace {

constexpr unsigned kShardCount = 16;
constexpr auto kTimeout = std::chrono::seconds(2);

} // namespace

TEST(ShardAffinityTest, CrossShardPostsStayOnTargetShard) {
  Runtime runtime(kShardCount);
  ASSERT_TRUE(runtime.start().has_value());

  std::array<std::atomic<unsigned>, kShardCount> hits{};
  std::array<std::atomic<bool>, kShardCount> mismatch{};

  for (auto &v : hits) {
    v.store(0, std::memory_order_relaxed);
  }
  for (auto &v : mismatch) {
    v.store(false, std::memory_order_relaxed);
  }

  for (unsigned source = 0; source < kShardCount; ++source) {
    runtime.post_to(source, [&runtime, &hits, &mismatch]() {
      for (unsigned target = 0; target < kShardCount; ++target) {
        runtime.post_to(target, [&runtime, &hits, &mismatch, target]() {
          if (runtime.current_shard() != target) {
            mismatch[target].store(true, std::memory_order_relaxed);
          }
          hits[target].fetch_add(1, std::memory_order_relaxed);
        });
      }
    });
  }

  EXPECT_TRUE(wait_until([&] {
    unsigned done = 0;
    for (const auto &h : hits) {
      done += h.load(std::memory_order_relaxed);
    }
    return done >= (kShardCount * kShardCount);
  }, kTimeout));

  for (unsigned i = 0; i < kShardCount; ++i) {
    EXPECT_GE(hits[i].load(std::memory_order_relaxed), kShardCount);
    EXPECT_FALSE(mismatch[i].load(std::memory_order_relaxed));
  }

  runtime.stop();
}

#ifdef __linux__
TEST(ShardAffinityTest, RuntimeCanPinShardThreadsToAllowedCpus) {
  constexpr unsigned kPinnedShardCount = 4;
  Runtime runtime(kPinnedShardCount, true);
  ASSERT_TRUE(runtime.start().has_value());

  std::array<std::atomic<int>, kPinnedShardCount> observed_cpu{};
  for (auto &cpu : observed_cpu) {
    cpu.store(-1, std::memory_order_relaxed);
  }

  for (unsigned target = 0; target < kPinnedShardCount; ++target) {
    runtime.post_to(target, [&observed_cpu, target]() {
      observed_cpu[target].store(sched_getcpu(), std::memory_order_relaxed);
    });
  }

  EXPECT_TRUE(wait_until([&] {
    for (const auto &cpu : observed_cpu) {
      if (cpu.load(std::memory_order_relaxed) < 0) {
        return false;
      }
    }
    return true;
  }, kTimeout));

  for (unsigned target = 0; target < kPinnedShardCount; ++target) {
    const auto expected_cpu = runtime.pinned_cpu_for_shard(target);
    ASSERT_GE(expected_cpu, 0);
    EXPECT_EQ(observed_cpu[target].load(std::memory_order_relaxed),
              expected_cpu);
  }

  runtime.stop();
}
#endif

} // namespace dagforge::test
