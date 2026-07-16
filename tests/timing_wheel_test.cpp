#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/core/runtime.hpp"

#include "test_utils.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

using namespace dagforge;
using namespace std::chrono_literals;

namespace {

constexpr auto kWaitTimeout = std::chrono::seconds(1);

auto race_timing_wheel_sleep(std::shared_ptr<std::atomic<bool>> completed)
    -> spawn_task {
  using namespace boost::asio::experimental::awaitable_operators;
  (void)co_await (async_sleep_on_timing_wheel(5s) ||
                  async_sleep(std::chrono::milliseconds(1)));
  completed->store(true, std::memory_order_release);
}

} // namespace

TEST(TimingWheelTest, FiresDelayedCallback) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  std::atomic<bool> fired{false};

  wheel.start();
  [[maybe_unused]] const auto fired_handle = wheel.schedule_after(25ms, [&] {
    fired.store(true, std::memory_order_release);
    wheel.stop();
  });

  std::jthread runner([&] { (void)io.run(); });

  EXPECT_TRUE(test::wait_until(
      [&] { return fired.load(std::memory_order_acquire); }, kWaitTimeout));
  io.stop();
}

TEST(TimingWheelTest, CancelPreventsDelayedCallback) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  std::atomic<bool> cancelled_fired{false};
  std::atomic<bool> stopper_fired{false};

  wheel.start();
  const auto cancelled = wheel.schedule_after(20ms, [&] {
    cancelled_fired.store(true, std::memory_order_release);
  });
  [[maybe_unused]] const auto stopper_handle = wheel.schedule_after(60ms, [&] {
    stopper_fired.store(true, std::memory_order_release);
    wheel.stop();
  });

  EXPECT_TRUE(wheel.cancel(cancelled));

  std::jthread runner([&] { (void)io.run(); });

  EXPECT_TRUE(test::wait_until(
      [&] { return stopper_fired.load(std::memory_order_acquire); },
      kWaitTimeout));
  EXPECT_FALSE(cancelled_fired.load(std::memory_order_acquire));
  io.stop();
}

TEST(TimingWheelTest, CancelsPendingSleepWhenRaceLoses) {
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  auto completed = std::make_shared<std::atomic<bool>>(false);

  runtime.spawn_on(shard_id{0}, race_timing_wheel_sleep(completed));

  ASSERT_TRUE(test::wait_until(
      [&] { return completed->load(std::memory_order_acquire); },
      kWaitTimeout));

  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(runtime.timing_wheel_pending_count(0), 0U);

  runtime.stop();
}

TEST(TimingWheelTest, ResumesAfterBecomingIdle) {
  io::IoContext io;
  io::TimingWheel wheel(io, 5ms, 64);
  auto guard = boost::asio::make_work_guard(io);
  std::atomic<int> fired_count{0};

  wheel.start();
  [[maybe_unused]] const auto first_handle = wheel.schedule_after(15ms, [&] {
    fired_count.fetch_add(1, std::memory_order_acq_rel);
  });

  std::jthread runner([&] { (void)io.run(); });

  ASSERT_TRUE(test::wait_until(
      [&] { return fired_count.load(std::memory_order_acquire) >= 1; },
      kWaitTimeout));

  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(wheel.pending_count(), 0U);

  std::atomic<bool> second_armed{false};
  boost::asio::post(io, [&] {
    [[maybe_unused]] const auto second_handle = wheel.schedule_after(15ms, [&] {
      fired_count.fetch_add(1, std::memory_order_acq_rel);
      wheel.stop();
      guard.reset();
    });
    second_armed.store(true, std::memory_order_release);
  });

  ASSERT_TRUE(test::wait_until(
      [&] { return second_armed.load(std::memory_order_acquire); },
      kWaitTimeout));
  EXPECT_TRUE(test::wait_until(
      [&] { return fired_count.load(std::memory_order_acquire) >= 2; },
      kWaitTimeout));

  io.stop();
}
