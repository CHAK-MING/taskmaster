#include "dagforge/sample/sample_service.hpp"

#include "dagforge/core/asio_awaitable.hpp"

#include <boost/asio/steady_timer.hpp>

#include <chrono>
#include <utility>

namespace dagforge {

SampleService::SampleService(Runtime &runtime) : runtime_(runtime) {}

auto SampleService::fetch(std::string_view key) -> task<Result<std::string>> {
  auto timer = boost::asio::steady_timer(runtime_.io().get_executor());
  timer.expires_after(std::chrono::milliseconds(1));

  auto wait_res = co_await co_as_result(timer.async_wait(dagforge::use_nothrow));
  if (!wait_res) {
    co_return fail(wait_res.error());
  }

  co_return ok(std::string(key));
}

auto SampleService::store(std::string key, std::string value)
    -> task<Result<void>> {
  auto timer = boost::asio::steady_timer(runtime_.io().get_executor());
  timer.expires_after(std::chrono::milliseconds(1));

  auto wait_res = co_await co_as_result(timer.async_wait(dagforge::use_nothrow));
  if (!wait_res) {
    co_return fail(wait_res.error());
  }

  (void)key;
  (void)value;
  co_return ok();
}

} // namespace dagforge

