#include "dagforge/io/context.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/scope_exit.hpp"

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>

namespace dagforge::io {

template <typename Rep, typename Period>
auto async_sleep(IoContext &ctx, std::chrono::duration<Rep, Period> duration)
    -> task<Result<void>> {
  boost::asio::steady_timer timer(
      ctx.native_handle(),
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));

  Runtime *runtime = nullptr;
  shard_id shard = kInvalidShard;
  if (detail::current_runtime != nullptr &&
      detail::current_shard_id != kInvalidShard) {
    runtime = detail::current_runtime;
    shard = detail::current_shard_id;
    runtime->note_timer_started(shard);
  }
  const auto timer_depth_guard = dagforge::scope_exit([runtime, shard] {
    if (runtime && shard != kInvalidShard) {
      runtime->note_timer_finished(shard);
    }
  });

  const auto cancellation = co_await boost::asio::this_coro::cancellation_state;
  auto wait_res = co_await co_as_result(timer.async_wait(
      boost::asio::bind_cancellation_slot(cancellation.slot(), use_nothrow)));
  co_return wait_res;
}

template auto async_sleep(IoContext &, std::chrono::nanoseconds)
    -> task<Result<void>>;
template auto async_sleep(IoContext &, std::chrono::microseconds)
    -> task<Result<void>>;
template auto async_sleep(IoContext &, std::chrono::milliseconds)
    -> task<Result<void>>;
template auto async_sleep(IoContext &, std::chrono::seconds)
    -> task<Result<void>>;

} // namespace dagforge::io
