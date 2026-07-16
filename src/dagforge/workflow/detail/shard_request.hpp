#pragma once

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"

#include <boost/asio/async_result.hpp>

#include <concepts>
#include <memory>
#include <utility>

namespace dagforge::workflow::detail {

template <std::default_initializable T, typename Function>
[[nodiscard]] auto request_value_on_shard(Runtime &runtime, shard_id target,
                                          std::weak_ptr<int> lifetime,
                                          Function function)
    -> task<Result<T>> {
  if (runtime.is_current_shard() && runtime.current_shard() == target) {
    co_return function();
  }

  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, T)>(
      [&runtime, target, lifetime, function = std::move(function)](
          auto handler) mutable {
        runtime.post_to(
            target,
            [lifetime, function = std::move(function),
             handler = std::move(handler)]() mutable {
              if (lifetime.expired()) {
                handler(boost::system::error_code{
                            make_error_code(Error::Cancelled)},
                        T{});
                return;
              }
              auto value = function();
              if (!value) {
                handler(boost::system::error_code{value.error()}, T{});
                return;
              }
              handler(boost::system::error_code{}, std::move(*value));
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok(std::move(*result));
}

template <typename Function>
[[nodiscard]] auto request_void_on_shard(Runtime &runtime, shard_id target,
                                         std::weak_ptr<int> lifetime,
                                         Function function)
    -> task<Result<void>> {
  if (runtime.is_current_shard() && runtime.current_shard() == target) {
    co_return function();
  }

  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [&runtime, target, lifetime, function = std::move(function)](
          auto handler) mutable {
        runtime.post_to(
            target,
            [lifetime, function = std::move(function),
             handler = std::move(handler)]() mutable {
              if (lifetime.expired()) {
                handler(boost::system::error_code{
                    make_error_code(Error::Cancelled)});
                return;
              }
              auto completed = function();
              handler(completed ? boost::system::error_code{}
                                : boost::system::error_code{
                                      completed.error()});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

} // namespace dagforge::workflow::detail
