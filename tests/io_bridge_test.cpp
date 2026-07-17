#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/io/asio_error.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/io/result.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <exception>
#include <optional>

using namespace dagforge;
using namespace dagforge::io;

TEST(IoBridgeTest, ToResultSuccess) {
  auto res = ok<std::size_t>(10).and_then(
      [](std::size_t bytes) -> Result<std::size_t> { return ok(bytes * 2); });

  ASSERT_TRUE(res.has_value());
  EXPECT_EQ(*res, 20);
}

TEST(IoBridgeTest, ToResultFailure) {
  bool executed = false;
  Result<std::size_t> initial = fail(make_error_code(IoError::TimedOut));
  auto res = initial.and_then([&](std::size_t bytes) -> Result<std::size_t> {
    executed = true;
    return ok(bytes * 2);
  });

  ASSERT_FALSE(res.has_value());
  EXPECT_FALSE(executed);
  EXPECT_EQ(res.error(), make_error_code(IoError::TimedOut));
}

TEST(IoBridgeTest, DiscardBytes) {
  auto res = ok();

  ASSERT_TRUE(res.has_value());
  static_assert(std::is_same_v<decltype(res), Result<void>>);
}

TEST(IoBridgeTest, NormalizesCommonBoostFailures) {
  EXPECT_EQ(normalize_error_code(boost::asio::error::operation_aborted),
            make_error_code(IoError::Cancelled));
  EXPECT_EQ(normalize_error_code(boost::asio::error::timed_out),
            make_error_code(IoError::TimedOut));
  EXPECT_EQ(normalize_error_code(boost::asio::error::eof),
            make_error_code(IoError::EndOfFile));
  EXPECT_EQ(normalize_error_code(boost::asio::error::would_block),
            make_error_code(IoError::WouldBlock));

  const auto permission_denied = boost::system::errc::make_error_code(
      boost::system::errc::permission_denied);
  const auto normalized = normalize_error_code(permission_denied);
  EXPECT_EQ(normalized.value(), permission_denied.value());
  EXPECT_EQ(normalized.message(), permission_denied.message());
}

TEST(IoBridgeTest, AsyncSleepReturnsSuccess) {
  io::IoContext context;
  std::optional<Result<void>> result;
  std::exception_ptr exception;
  boost::asio::co_spawn(context.native_handle(),
                        async_sleep(context, std::chrono::milliseconds(1)),
                        [&](std::exception_ptr current, Result<void> value) {
                          exception = current;
                          result = std::move(value);
                        });

  (void)context.run();
  ASSERT_FALSE(exception);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->has_value());
}

TEST(IoBridgeTest, AsyncSleepReportsCallerCancellation) {
  io::IoContext context;
  boost::asio::cancellation_signal cancellation;
  std::optional<Result<void>> result;
  std::exception_ptr exception;
  boost::asio::co_spawn(context.native_handle(),
                        async_sleep(context, std::chrono::seconds(5)),
                        boost::asio::bind_cancellation_slot(
                            cancellation.slot(), [&](std::exception_ptr current,
                                                     Result<void> value) {
                              exception = current;
                              result = std::move(value);
                            }));
  boost::asio::steady_timer cancel_after(context.native_handle(),
                                         std::chrono::milliseconds(1));
  cancel_after.async_wait([&](const boost::system::error_code &error) {
    if (!error) {
      cancellation.emit(boost::asio::cancellation_type::all);
    }
  });

  (void)context.run();
  ASSERT_FALSE(exception);
  ASSERT_TRUE(result.has_value());
  ASSERT_FALSE(result->has_value());
  EXPECT_EQ(result->error(), make_error_code(IoError::Cancelled));
  EXPECT_TRUE(is_cancelled(result->error()));
}
