#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/io/context.hpp"
#endif

#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <unordered_map>
#include <vector>

namespace dagforge {

class Runtime;

namespace io {

class TimingWheel {
public:
  struct Handle {
    std::uint64_t value{0};

    [[nodiscard]] auto valid() const noexcept -> bool { return value != 0; }

    friend auto operator==(Handle lhs, Handle rhs) noexcept -> bool = default;
  };

  using Callback = std::move_only_function<void()>;

  explicit TimingWheel(IoContext &io,
                       std::chrono::nanoseconds tick =
                           std::chrono::milliseconds(10),
                       std::size_t slot_count = 512);
  ~TimingWheel() = default;

  TimingWheel(const TimingWheel &) = delete;
  auto operator=(const TimingWheel &) -> TimingWheel & = delete;

  auto start() -> void;
  auto stop() -> void;

  [[nodiscard]] auto schedule_after(std::chrono::nanoseconds delay,
                                    Callback callback) -> Handle;
  [[nodiscard]] auto cancel(Handle handle) -> bool;
  [[nodiscard]] auto pending_count() const noexcept -> std::size_t;

private:
  struct Entry {
    Handle handle;
    std::uint64_t rounds{0};
    Callback callback;
  };

  using Bucket = std::list<Entry>;

  struct EntryLocation {
    std::size_t bucket_index{0};
    Bucket::iterator iterator;
  };

  [[nodiscard]] static auto tick_count_for_delay(std::chrono::nanoseconds delay,
                                                 std::chrono::nanoseconds tick)
      -> std::uint64_t;
  auto arm_next_tick() -> void;
  auto handle_tick(const boost::system::error_code &ec) -> void;

  boost::asio::steady_timer timer_;
  std::chrono::nanoseconds tick_;
  std::vector<Bucket> buckets_;
  std::unordered_map<std::uint64_t, EntryLocation> locations_;
  std::atomic<std::size_t> pending_count_snapshot_{0};
  std::size_t cursor_{0};
  std::uint64_t next_handle_{1};
  bool running_{false};
  bool armed_{false};
};

} // namespace io

[[nodiscard]] auto async_sleep_on_timing_wheel(std::chrono::nanoseconds duration)
    -> spawn_task;

template <typename Rep, typename Period>
[[nodiscard]] inline auto
async_sleep_on_timing_wheel(std::chrono::duration<Rep, Period> duration)
    -> spawn_task {
  return async_sleep_on_timing_wheel(
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

} // namespace dagforge
