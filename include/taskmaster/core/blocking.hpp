#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/runtime.hpp"

#include <optional>
#include <thread>
#include <type_traits>

namespace taskmaster {

template <typename F>
struct BlockingAwaiter {
  using Result = std::invoke_result_t<F>;
  
  F func;
  std::optional<Result> result;
  Runtime* runtime;

  explicit BlockingAwaiter(F&& f) 
      : func(std::move(f)), runtime(detail::current_runtime) {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    // Capture runtime pointer because thread execution will not have TLS set
    auto* rt = runtime;
    std::thread([this, handle, rt]() mutable {
      try {
        if constexpr (std::is_void_v<Result>) {
          func();
        } else {
          result.emplace(func());
        }
      } catch (...) {
        // TODO: Propagate exception
      }
      // Schedule resumption on the reactor
      rt->schedule_external(handle);
    }).detach();
  }

  auto await_resume() noexcept -> Result {
    if constexpr (!std::is_void_v<Result>) {
      return std::move(*result);
    }
  }
};

template <typename F>
[[nodiscard]] auto spawn_blocking(F&& func) -> task<std::invoke_result_t<F>> {
  co_return co_await BlockingAwaiter<std::decay_t<F>>{std::forward<F>(func)};
}

} // namespace taskmaster
