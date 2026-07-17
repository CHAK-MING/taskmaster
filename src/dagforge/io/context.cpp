#include "dagforge/io/context.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/scope_exit.hpp"

#include <boost/asio/steady_timer.hpp>
#include <boost/system/system_error.hpp>

#include <chrono>

namespace dagforge::io {

template <typename Rep, typename Period>
auto async_sleep(IoContext &ctx, std::chrono::duration<Rep, Period> duration)
    -> spawn_task {
  // Keep this wrapper instead of exposing steady_timer directly so callers get
  // one uniform "cancel is normal, everything else is exceptional" policy.
  boost::asio::steady_timer timer(
      ctx, std::chrono::duration_cast<std::chrono::nanoseconds>(duration));

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

  const auto operation_aborted =
      std::error_code{boost::asio::error::make_error_code(
          boost::asio::error::operation_aborted)};
  auto wait_res = co_await co_as_result(timer.async_wait(use_nothrow));
  if (!wait_res && wait_res.error() != operation_aborted) {
    throw boost::system::system_error(wait_res.error());
  }
}

template auto async_sleep(IoContext &, std::chrono::nanoseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::microseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::milliseconds) -> spawn_task;
template auto async_sleep(IoContext &, std::chrono::seconds) -> spawn_task;

} // namespace dagforge::io
