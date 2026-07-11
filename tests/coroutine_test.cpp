#include "dagforge/core/coroutine.hpp"
#include <boost/asio.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <gtest/gtest.h>

namespace asio = boost::asio;
using asio::awaitable;
using asio::co_spawn;
using asio::detached;
using asio::use_awaitable;
using namespace asio::experimental::awaitable_operators;

// Helper: run a coroutine on an io_context and return its result
template <typename T> T run(asio::io_context &io, awaitable<T> coro) {
  std::optional<T> result;
  std::exception_ptr ex;
  co_spawn(io, std::move(coro), [&](std::exception_ptr ep, T r) {
    ex = ep;
    if (!ep)
      result = std::move(r);
  });
  io.run();
  io.restart();
  if (ex)
    std::rethrow_exception(ex);
  return std::move(*result);
}

// Specialisation for void
void run_void(asio::io_context &io, awaitable<void> coro) {
  std::exception_ptr ex;
  co_spawn(io, std::move(coro), [&](std::exception_ptr ep) { ex = ep; });
  io.run();
  io.restart();
  if (ex)
    std::rethrow_exception(ex);
}

struct CoroutineTest : ::testing::Test {
  asio::io_context io;
};

// --- Basic return value ---
TEST_F(CoroutineTest, BasicReturnValue) {
  auto coro = []() -> awaitable<int> { co_return 42; };
  EXPECT_EQ(run(io, coro()), 42);
}

// --- Move-only type ---
TEST_F(CoroutineTest, MoveOnlyType) {
  auto coro = []() -> awaitable<std::unique_ptr<int>> {
    co_return std::make_unique<int>(99);
  };
  auto p = run(io, coro());
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(*p, 99);
}

// --- Awaiting a child coroutine ---
TEST_F(CoroutineTest, AwaitChildTask) {
  auto child = []() -> awaitable<int> { co_return 7; };
  auto parent = [&]() -> awaitable<int> {
    int v = co_await child();
    co_return v * 2;
  };
  EXPECT_EQ(run(io, parent()), 14);
}

// --- when_all (&&) ---
TEST_F(CoroutineTest, WhenAll_MixedTypes) {
  auto a = []() -> awaitable<int> { co_return 1; };
  auto b = []() -> awaitable<double> { co_return 2.5; };

  auto coro = [&]() -> awaitable<void> {
    auto [i, d] = co_await (a() && b());
    EXPECT_EQ(i, 1);
    EXPECT_DOUBLE_EQ(d, 2.5);
  };
  run_void(io, coro());
}

// --- when_any (||) ---
TEST_F(CoroutineTest, WhenAny_ImmediateCompletion) {
  auto fast = []() -> awaitable<int> { co_return 10; };
  auto slow = [](asio::io_context &event_loop) -> awaitable<int> {
    asio::steady_timer t(event_loop, std::chrono::seconds(10));
    co_await t.async_wait(use_awaitable);
    co_return 99;
  };

  auto coro = [&]() -> awaitable<void> {
    // || returns variant<tuple<int>, tuple<int>>; index 0 means fast() won
    auto result = co_await (fast() || slow(io));
    EXPECT_EQ(result.index(), 0u);
    EXPECT_EQ(std::get<0>(result), 10);
  };
  run_void(io, coro());
}

// --- Exception propagation from child ---
TEST_F(CoroutineTest, AwaitChildTask_PropagatesException) {
  auto child = []() -> awaitable<int> {
    throw std::runtime_error("child error");
    co_return 0;
  };
  auto parent = [&]() -> awaitable<void> {
    EXPECT_THROW(co_await child(), std::runtime_error);
  };
  run_void(io, parent());
}

// --- Exception stored and rethrown via co_spawn ---
TEST_F(CoroutineTest, Task_ExceptionStoredAndRethrown) {
  auto coro = []() -> awaitable<int> {
    throw std::runtime_error("boom");
    co_return 0;
  };
  EXPECT_THROW(run(io, coro()), std::runtime_error);
}

// --- when_all propagates exception ---
TEST_F(CoroutineTest, WhenAll_PropagatesException) {
  auto ok = []() -> awaitable<int> { co_return 1; };
  auto bad = []() -> awaitable<int> {
    throw std::runtime_error("fail");
    co_return 0;
  };
  auto coro = [&]() -> awaitable<void> {
    EXPECT_THROW(co_await (ok() && bad()), std::runtime_error);
  };
  run_void(io, coro());
}

// --- Asio async operation with use_awaitable ---
TEST_F(CoroutineTest, AsioTimer_AwaitNatively) {
  auto coro = [&]() -> awaitable<void> {
    asio::steady_timer t(io, std::chrono::milliseconds(1));
    co_await t.async_wait(use_awaitable);
  };
  run_void(io, coro());
}

// --- as_tuple (use_nothrow-style) returns error_code without throw ---
TEST_F(CoroutineTest, AsioTimer_ErrorCodeVariant) {
  constexpr auto use_nothrow = asio::as_tuple(use_awaitable);
  auto coro = [&]() -> awaitable<void> {
    asio::steady_timer t(io, std::chrono::milliseconds(1));
    auto [ec] = co_await t.async_wait(use_nothrow);
    EXPECT_FALSE(ec);
  };
  run_void(io, coro());
}

// --- Cancellation via asio::cancellation_signal ---
TEST_F(CoroutineTest, CancellationSignal_AbortsWait) {
  asio::cancellation_signal sig;
  asio::steady_timer cancel_after(io, std::chrono::milliseconds(1));

  auto coro = [&]() -> awaitable<void> {
    asio::steady_timer t(io, std::chrono::milliseconds(200));
    try {
      co_await t.async_wait(
          asio::bind_cancellation_slot(sig.slot(), use_awaitable));
      ADD_FAILURE() << "Should have been cancelled";
      co_return;
    } catch (const boost::system::system_error &e) {
      EXPECT_EQ(e.code(), asio::error::operation_aborted);
    }
  };

  // Emit cancellation after async_wait is armed to avoid scheduling race.
  cancel_after.async_wait([&](const boost::system::error_code &ec) {
    if (!ec) {
      sig.emit(asio::cancellation_type::all);
    }
  });
  run_void(io, coro());
}

// --- Symmetric transfer / deep recursion doesn't stack-overflow ---
TEST_F(CoroutineTest, SymmetricTransfer_StackDepth) {
  const int depth = 100'000;
  std::function<awaitable<int>(int)> recur;
  recur = [&](int n) -> awaitable<int> {
    if (n == 0)
      co_return 0;
    auto value = co_await recur(n - 1);
    co_return value + 1;
  };
  EXPECT_EQ(run(io, recur(depth)), depth);
}
