#pragma once

#include "taskmaster/io/result.hpp"
#include <chrono>
#include <cstdint>

namespace taskmaster::io {

class TimerFd {
public:
  [[nodiscard]] static auto create() -> IoExpected<TimerFd>;

  TimerFd() noexcept = default;
  ~TimerFd();

  TimerFd(TimerFd&& other) noexcept;
  auto operator=(TimerFd&& other) noexcept -> TimerFd&;

  TimerFd(const TimerFd&) = delete;
  auto operator=(const TimerFd&) -> TimerFd&;

  auto set_absolute(std::chrono::steady_clock::time_point deadline) -> bool;
  auto unset() -> bool;
  
  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }
  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_ >= 0; }
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  auto consume() noexcept -> std::uint64_t;

private:
  explicit TimerFd(int fd) noexcept : fd_(fd) {}
  auto close_sync() noexcept -> void;

  int fd_{-1};
};

} // namespace taskmaster::io
