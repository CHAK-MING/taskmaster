#include "dagforge/core/shard.hpp"

#include "dagforge/util/hash.hpp"

#include <boost/asio/post.hpp>
#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <memory_resource>
#include <string_view>

using namespace dagforge;

TEST(ShardTest, BasicConstruction) {
  Shard shard(0);
  EXPECT_EQ(shard.id(), 0);
}

TEST(ShardTest, ShardId) {
  Shard shard1(0);
  Shard shard2(5);
  Shard shard3(10);

  EXPECT_EQ(shard1.id(), 0);
  EXPECT_EQ(shard2.id(), 5);
  EXPECT_EQ(shard3.id(), 10);
}

TEST(ShardTest, CtxNotStoppedInitially) {
  Shard shard(0);
  EXPECT_FALSE(shard.ctx().stopped());
}

TEST(ShardTest, MemoryResourceNotNull) {
  Shard shard(0);
  EXPECT_NE(shard.memory_resource(), nullptr);
}

TEST(ShardTest, MemoryResourceAllocFreeWorks) {
  Shard shard(0);
  auto *mr = shard.memory_resource();
  ASSERT_NE(mr, nullptr);
  void *p = mr->allocate(64, alignof(std::max_align_t));
  EXPECT_NE(p, nullptr);
  mr->deallocate(p, 64, alignof(std::max_align_t));
}

TEST(ShardTest, TracksArenaUsageAndUpstreamFallbacks) {
  Shard shard(0);
  auto *resource = shard.memory_resource();
  ASSERT_NE(resource, nullptr);

  EXPECT_EQ(shard.memory_capacity_bytes(), Shard::kArenaSize);
  const auto initial_used = shard.memory_used_bytes();
  const auto initial_allocations = shard.memory_allocations_total();
  const auto initial_fallbacks = shard.memory_oom_fallbacks_total();

  void *small = resource->allocate(64, alignof(std::max_align_t));
  ASSERT_NE(small, nullptr);
  EXPECT_GT(shard.memory_used_bytes(), initial_used);
  EXPECT_GT(shard.memory_allocations_total(), initial_allocations);
  resource->deallocate(small, 64, alignof(std::max_align_t));

  constexpr std::size_t kOversizedAllocation = Shard::kArenaSize * 2;
  void *oversized =
      resource->allocate(kOversizedAllocation, alignof(std::max_align_t));
  ASSERT_NE(oversized, nullptr);
  EXPECT_GT(shard.memory_oom_fallbacks_total(), initial_fallbacks);
  resource->deallocate(oversized, kOversizedAllocation,
                       alignof(std::max_align_t));
}

TEST(ShardTest, ContextExecutesPostedWork) {
  Shard shard(0);
  std::atomic<int> counter{0};
  boost::asio::post(shard.ctx(), [&] { counter.fetch_add(1); });
  (void)shard.ctx().run_one();
  EXPECT_EQ(counter.load(), 1);
}

TEST(ShardRoutingTest, UsesUnorderedDenseHashForStableTypeSemantics) {
  constexpr std::string_view kValue = "dag_run_alpha";
  const auto hash = static_cast<std::size_t>(
      ankerl::unordered_dense::hash<std::string_view>{}(kValue));

  for (unsigned shard_count : {1U, 2U, 3U, 8U, 16U}) {
    EXPECT_EQ(util::shard_of(kValue, shard_count), hash % shard_count);
  }
}
