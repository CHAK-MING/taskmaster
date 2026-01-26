#include "taskmaster/io/timer_fd.hpp"
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>
#include <cerrno>

namespace taskmaster::io {

auto TimerFd::create() -> IoExpected<TimerFd> {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (fd < 0) {
    return std::unexpected(from_errno(errno));
  }
  return TimerFd{fd};
}

TimerFd::~TimerFd() {
  close_sync();
}

TimerFd::TimerFd(TimerFd&& other) noexcept 
  : fd_(std::exchange(other.fd_, -1)) {}

auto TimerFd::operator=(TimerFd&& other) noexcept -> TimerFd& {
  if (this != &other) {
    close_sync();
    fd_ = std::exchange(other.fd_, -1);
  }
  return *this;
}

auto TimerFd::close_sync() noexcept -> void {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

auto TimerFd::set_absolute(std::chrono::steady_clock::time_point deadline) -> bool {
  if (fd_ < 0) return false;

  struct itimerspec new_value{};
  
  auto duration = deadline.time_since_epoch();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
  auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - seconds);

  new_value.it_value.tv_sec = static_cast<time_t>(seconds.count());
  new_value.it_value.tv_nsec = static_cast<long>(nanoseconds.count());
  new_value.it_interval.tv_sec = 0;
  new_value.it_interval.tv_nsec = 0;

  return ::timerfd_settime(fd_, TFD_TIMER_ABSTIME, &new_value, nullptr) == 0;
}

auto TimerFd::unset() -> bool {
  if (fd_ < 0) return false;
  struct itimerspec new_value{}; // zeroed out disarms timer
  return ::timerfd_settime(fd_, 0, &new_value, nullptr) == 0;
}

auto TimerFd::consume() noexcept -> std::uint64_t {
    if (fd_ < 0) return 0;
    std::uint64_t val;
    if (::read(fd_, &val, sizeof(val)) == sizeof(val)) {
        return val;
    }
    return 0;
}

} // namespace taskmaster::io
