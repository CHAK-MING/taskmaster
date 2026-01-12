#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/io/result.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <utility>

namespace taskmaster::io {

class EventFd {
public:
  /// Create eventfd with specified flags
  [[nodiscard]] static auto create(unsigned int initval = 0,
                                   int flags = EFD_NONBLOCK) -> IoExpected<EventFd> {
    int fd = ::eventfd(initval, flags);
    if (fd < 0) {
      return std::unexpected(from_errno(errno));
    }
    return EventFd{fd};
  }

  EventFd() noexcept = default;

  EventFd(EventFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

  auto operator=(EventFd&& other) noexcept -> EventFd& {
    if (this != &other) {
      close_sync();
      fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
  }

  EventFd(const EventFd&) = delete;
  auto operator=(const EventFd&) -> EventFd& = delete;

  ~EventFd() { close_sync(); }

  [[nodiscard]] auto native_handle() const noexcept -> int { return fd_; }
  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }

  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_ >= 0; }
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  auto signal(std::uint64_t value = 1) noexcept -> bool {
    while (true) {
      auto ret = ::write(fd_, &value, sizeof(value));
      if (ret == sizeof(value)) return true;
      if (ret < 0 && errno == EINTR) continue;
      if (ret < 0 && errno == EAGAIN) return true;  // Already signaled
      return false;
    }
  }

  auto consume() noexcept -> std::uint64_t {
    std::uint64_t total = 0;
    std::uint64_t val;
    while (::read(fd_, &val, sizeof(val)) > 0) {
      total += val;
    }
    return total;
  }

  [[nodiscard]] auto wait(IoContext& ctx) -> task<IoResult> {
    if (fd_ < 0) {
      co_return io_failure(IoError::BadDescriptor);
    }
    co_return co_await ctx.async_poll(fd_, POLLIN);
  }

private:
  explicit EventFd(int fd) noexcept : fd_(fd) {}

  auto close_sync() noexcept -> void {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  int fd_{-1};
};

}  // namespace taskmaster::io
