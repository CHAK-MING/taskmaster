#pragma once

#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/io/context.hpp"

#include "benchmark_compat.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <format>
#include <string>
#include <string_view>

namespace dagforge::bench {

// ---------------------------------------------------------------------------
// Size presets
// ---------------------------------------------------------------------------
constexpr int kSmallSize = 10;
constexpr int kMediumSize = 100;
constexpr int kLargeSize = 1000;
constexpr int kVeryLargeSize = 10000;

// ---------------------------------------------------------------------------
// run_on_io — synchronously drive one coroutine on a local IoContext.
// Suitable for benchmarks that need to measure a single async path without
// the overhead of spinning up a full Runtime.
// ---------------------------------------------------------------------------
template <typename T>
[[nodiscard]] auto run_on_io(io::IoContext &io, task<T> t) -> T {
  auto fut = boost::asio::co_spawn(io.native_handle(), std::move(t),
                                   boost::asio::use_future);
  (void)io.run();
  io.restart();
  return fut.get();
}

inline void run_on_io(io::IoContext &io, task<void> t) {
  auto fut = boost::asio::co_spawn(io.native_handle(), std::move(t),
                                   boost::asio::use_future);
  (void)io.run();
  io.restart();
  fut.get();
}

// ---------------------------------------------------------------------------
// RuntimeGuard — RAII wrapper that starts on construction and stops on
// destruction; avoids repetitive start/stop boilerplate in each benchmark.
// ---------------------------------------------------------------------------
struct RuntimeGuard {
  Runtime runtime;

  explicit RuntimeGuard(unsigned shards) : runtime(shards) {
    if (auto r = runtime.start(); !r) {
      throw std::runtime_error("runtime.start failed");
    }
  }

  ~RuntimeGuard() noexcept { runtime.stop(); }

  RuntimeGuard(const RuntimeGuard &) = delete;
  RuntimeGuard &operator=(const RuntimeGuard &) = delete;
};

// ---------------------------------------------------------------------------
// env_or_default — read an environment variable with a fallback.
// ---------------------------------------------------------------------------
[[nodiscard]] inline auto env_or_default(const char *key, std::string fallback)
    -> std::string {
  if (const char *v = std::getenv(key); v != nullptr && *v != '\0') {
    return v;
  }
  return fallback;
}

} // namespace dagforge::bench
