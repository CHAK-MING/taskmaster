#include "dagforge/sample/sample_service.hpp"

#include "dagforge/io/timing_wheel.hpp"

#include <chrono>
#include <utility>

namespace dagforge {

SampleService::SampleService(Runtime &runtime) : runtime_(runtime) {}

auto SampleService::fetch(std::string key) -> task<Result<std::string>> {
  if (!runtime_.is_running()) {
    co_return fail(Error::SystemNotRunning);
  }

  auto sleep =
      co_await async_sleep_on_timing_wheel(std::chrono::milliseconds(1));
  if (!sleep) {
    co_return fail(sleep.error());
  }
  co_return ok(std::move(key));
}

auto SampleService::store(std::string key, std::string value)
    -> task<Result<void>> {
  if (!runtime_.is_running()) {
    co_return fail(Error::SystemNotRunning);
  }

  auto sleep =
      co_await async_sleep_on_timing_wheel(std::chrono::milliseconds(1));
  if (!sleep) {
    co_return fail(sleep.error());
  }
  (void)key;
  (void)value;
  co_return ok();
}

} // namespace dagforge

