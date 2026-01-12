#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/io/buffer.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/io/result.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>
#include <utility>

namespace taskmaster::io {

enum class Ownership : std::uint8_t {
  Owned,
  Borrowed
};

/// RAII handle for async file descriptor operations.
/// Handle owns both the fd AND knows its IoContext.
class AsyncFd {
public:

  /// Create from raw file descriptor
  [[nodiscard]] static auto from_raw(IoContext& ctx, int fd,
                                     Ownership ownership = Ownership::Owned)
      -> AsyncFd {
    return AsyncFd{&ctx, fd, ownership};
  }

  [[nodiscard]] static auto borrow(IoContext& ctx, int fd) -> AsyncFd {
    return AsyncFd{&ctx, fd, Ownership::Borrowed};
  }

  AsyncFd() noexcept = default;

  AsyncFd(AsyncFd&& other) noexcept
      : ctx_(std::exchange(other.ctx_, nullptr)),
        fd_(std::exchange(other.fd_, -1)),
        ownership_(std::exchange(other.ownership_, Ownership::Borrowed)) {}

  auto operator=(AsyncFd&& other) noexcept -> AsyncFd& {
    if (this != &other) {
      close_sync();
      ctx_ = std::exchange(other.ctx_, nullptr);
      fd_ = std::exchange(other.fd_, -1);
      ownership_ = std::exchange(other.ownership_, Ownership::Borrowed);
    }
    return *this;
  }

  AsyncFd(const AsyncFd&) = delete;
  auto operator=(const AsyncFd&) -> AsyncFd& = delete;

  ~AsyncFd() { close_sync(); }

  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }
  [[nodiscard]] auto native_handle() const noexcept -> int { return fd_; }
  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_ >= 0; }
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  [[nodiscard]] auto context() noexcept -> IoContext& {
    assert(ctx_ != nullptr);
    return *ctx_;
  }

  [[nodiscard]] auto context() const noexcept -> const IoContext& {
    assert(ctx_ != nullptr);
    return *ctx_;
  }

  [[nodiscard]] auto has_context() const noexcept -> bool {
    return ctx_ != nullptr;
  }

  [[nodiscard]] auto release() noexcept -> int {
    ownership_ = Ownership::Borrowed;
    return std::exchange(fd_, -1);
  }

  auto reset(int fd = -1, Ownership ownership = Ownership::Owned) noexcept
      -> void {
    close_sync();
    fd_ = fd;
    ownership_ = ownership;
  }

  [[nodiscard]] auto detach() noexcept -> std::pair<int, Ownership> {
    auto result = std::make_pair(fd_, ownership_);
    fd_ = -1;
    ownership_ = Ownership::Borrowed;
    ctx_ = nullptr;
    return result;
  }

  auto rebind(IoContext& ctx) noexcept -> void { ctx_ = &ctx; }

  [[nodiscard]] auto async_read(MutableBuffer buffer, std::uint64_t offset = 0)
      -> IoAwaitable<ops::Read> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_read(fd_, buffer, offset);
  }

  [[nodiscard]] auto async_read_at(MutableBuffer buffer, std::uint64_t offset)
      -> IoAwaitable<ops::Read> {
    return async_read(buffer, offset);
  }

  [[nodiscard]] auto async_write(ConstBuffer buffer, std::uint64_t offset = 0)
      -> IoAwaitable<ops::Write> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_write(fd_, buffer, offset);
  }

  [[nodiscard]] auto async_write_at(ConstBuffer buffer, std::uint64_t offset)
      -> IoAwaitable<ops::Write> {
    return async_write(buffer, offset);
  }

  [[nodiscard]] auto async_close() -> IoAwaitable<ops::Close> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    auto result = ctx_->async_close(fd_);
    fd_ = -1;
    return result;
  }

  [[nodiscard]] auto async_poll(std::uint32_t events) -> IoAwaitable<ops::Poll> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_poll(fd_, events);
  }

  [[nodiscard]] auto async_poll_timeout(std::uint32_t events,
                                        std::chrono::milliseconds timeout)
      -> PollTimeoutAwaitable {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_poll_timeout(fd_, events, timeout);
  }

  [[nodiscard]] auto read_all(std::size_t max_size = 1024 * 1024)
      -> task<IoExpected<std::string>>;

  [[nodiscard]] auto write_all(ConstBuffer buffer) -> task<IoResult>;

private:
  AsyncFd(IoContext* ctx, int fd, Ownership ownership) noexcept
      : ctx_(ctx), fd_(fd), ownership_(ownership) {}

  auto close_sync() noexcept -> void {
    if (fd_ >= 0 && ownership_ == Ownership::Owned) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  IoContext* ctx_{nullptr};
  int fd_{-1};
  Ownership ownership_{Ownership::Borrowed};
};

struct AsyncPipe {
  AsyncFd read_end;
  AsyncFd write_end;

  [[nodiscard]] static auto create(IoContext& ctx) -> IoExpected<AsyncPipe>;
  [[nodiscard]] static auto create(IoContext& ctx, int flags)
      -> IoExpected<AsyncPipe>;
};

class AsyncEventFd {
public:
  [[nodiscard]] static auto create(IoContext& ctx, unsigned int initval = 0,
                                   int flags = EFD_NONBLOCK)
      -> IoExpected<AsyncEventFd>;

  AsyncEventFd() noexcept = default;

  AsyncEventFd(AsyncEventFd&& other) noexcept
      : fd_(std::move(other.fd_)) {}

  auto operator=(AsyncEventFd&& other) noexcept -> AsyncEventFd& {
    fd_ = std::move(other.fd_);
    return *this;
  }

  AsyncEventFd(const AsyncEventFd&) = delete;
  auto operator=(const AsyncEventFd&) -> AsyncEventFd& = delete;

  [[nodiscard]] auto native_handle() const noexcept -> int {
    return fd_.fd();
  }
  [[nodiscard]] auto fd() const noexcept -> int { return fd_.fd(); }

  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_.is_open(); }
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  auto signal(std::uint64_t value = 1) noexcept -> bool;
  auto consume() noexcept -> std::uint64_t;

  [[nodiscard]] auto async_wait() -> IoAwaitable<ops::Poll> {
    return fd_.async_poll(POLLIN);
  }

  [[nodiscard]] auto async_fd() noexcept -> AsyncFd& { return fd_; }
  [[nodiscard]] auto async_fd() const noexcept -> const AsyncFd& { return fd_; }

private:
  explicit AsyncEventFd(AsyncFd fd) noexcept : fd_(std::move(fd)) {}

  AsyncFd fd_;
};

[[nodiscard]] inline auto make_async_fd(IoContext& ctx, int fd) -> AsyncFd {
  return AsyncFd::from_raw(ctx, fd, Ownership::Owned);
}

[[nodiscard]] inline auto borrow_async_fd(IoContext& ctx, int fd) -> AsyncFd {
  return AsyncFd::borrow(ctx, fd);
}

}  // namespace taskmaster::io
