#include "taskmaster/core/lockfree_queue.hpp"

#include <atomic>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "test_utils.hpp"

using namespace taskmaster;

class LockfreeQueueTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Setup test environment
  }

  void TearDown() override {
    // Cleanup test environment
  }
};

TEST_F(LockfreeQueueTest, SPSCBasicPushPop) {
  SPSCQueue<int> queue(64);

  EXPECT_TRUE(queue.push(42));
  std::optional<int> value = queue.try_pop();
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 42);
}

TEST_F(LockfreeQueueTest, SPSCEmptyQueue) {
  SPSCQueue<int> queue(64);

  std::optional<int> value = queue.try_pop();
  EXPECT_FALSE(value.has_value());
  EXPECT_TRUE(queue.empty());
}

TEST_F(LockfreeQueueTest, SPSCMultipleOperations) {
  SPSCQueue<int> queue(128);

  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(queue.push(i));
  }

  for (int i = 0; i < 100; ++i) {
    std::optional<int> value = queue.try_pop();
    EXPECT_TRUE(value.has_value());
    EXPECT_EQ(value.value(), i);
  }
}

TEST_F(LockfreeQueueTest, SPSCConcurrentOperations) {
  SPSCQueue<int> queue(1024);
  std::atomic<int> sum{0};
  std::atomic<bool> done{false};

  // Producer thread
  std::thread producer([&]() {
    for (int i = 1; i <= 1000; ++i) {
      while (!queue.push(i)) {
        std::this_thread::yield();
      }
    }
    done.store(true);
  });

  // Consumer thread
  std::thread consumer([&]() {
    int local_sum = 0;
    while (!done.load() || !queue.empty()) {
      std::optional<int> value = queue.try_pop();
      if (value.has_value()) {
        local_sum += value.value();
      } else {
        std::this_thread::yield();
      }
    }
    sum.store(local_sum);
  });

  producer.join();
  consumer.join();

  // Sum of numbers from 1 to 1000
  EXPECT_EQ(sum.load(), 1000 * 1001 / 2);
}

TEST_F(LockfreeQueueTest, BoundedMPSCBasicOperations) {
  BoundedMPSCQueue<int> queue(64);

  EXPECT_TRUE(queue.push(42));
  std::optional<int> value = queue.try_pop();
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 42);
}

TEST_F(LockfreeQueueTest, BoundedMPSCCapacity) {
  BoundedMPSCQueue<int> queue(8);

  // Fill to capacity
  for (int i = 0; i < 8; ++i) {
    EXPECT_TRUE(queue.push(i));
  }

  // Should fail when full
  EXPECT_FALSE(queue.push(8));

  // Pop one and push again
  std::optional<int> value = queue.try_pop();
  EXPECT_TRUE(value.has_value());
  EXPECT_EQ(value.value(), 0);

  EXPECT_TRUE(queue.push(8));
}