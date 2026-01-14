#include "taskmaster/core/shard.hpp"

#include <chrono>
#include <thread>

#include "gtest/gtest.h"

using namespace taskmaster;

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

TEST(ShardTest, WakeFdValid) {
  Shard shard(0);
  EXPECT_GE(shard.wake_fd(), 0);
}

TEST(ShardTest, CtxValid) {
  Shard shard(0);
  EXPECT_TRUE(shard.ctx().valid());
}

TEST(ShardTest, MemoryResourceNotNull) {
  Shard shard(0);
  EXPECT_NE(shard.memory_resource(), nullptr);
}

TEST(ShardTest, HasWorkInitiallyFalse) {
  Shard shard(0);
  EXPECT_FALSE(shard.has_work());
}

TEST(ShardTest, InitiallyNotSleeping) {
  Shard shard(0);
  EXPECT_FALSE(shard.is_sleeping());
}

TEST(ShardTest, SetSleepingFlag) {
  Shard shard(0);

  shard.set_sleeping(true);
  EXPECT_TRUE(shard.is_sleeping());

  shard.set_sleeping(false);
  EXPECT_FALSE(shard.is_sleeping());
}

TEST(ShardTest, ProcessReadyReturnsFalseWhenEmpty) {
  Shard shard(0);
  auto result = shard.process_ready();
  EXPECT_FALSE(result);
}

TEST(ShardTest, ProcessIoDoesNotCrash) {
  Shard shard(0);
  shard.process_io();
}

TEST(ShardTest, DrainPendingDoesNotCrash) {
  Shard shard(0);
  shard.drain_pending();
}

TEST(ShardTest, TrackUntrackIoData) {
  Shard shard(0);

  CompletionData data1{};
  CompletionData data2{};

  shard.track_io_data(&data1);
  shard.track_io_data(&data2);

  shard.untrack_io_data(&data1);
  shard.untrack_io_data(&data2);
}
