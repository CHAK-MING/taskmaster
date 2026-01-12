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

// ============================================================================
// Ownership Policy
// ============================================================================

/// Ownership policy for file descriptors
enum class Ownership : std::uint8_t {
  Owned,    // We own the fd and will close it
  Borrowed  // We don't own the fd
};

// ============================================================================
// AsyncFd - Async file descriptor handle bound to IoContext
// ============================================================================

/// RAII handle for async file descriptor operations.
/// Key design: Handle owns both the fd AND knows its IoContext.
/// This enables method calls directly on the handle without passing context.
class AsyncFd {
public:
  // ==========================================================================
  // Factory Methods
  // ==========================================================================

  /// Create from raw file descriptor
  [[nodiscard]] static auto from_raw(IoContext& ctx, int fd,
                                     Ownership ownership = Ownership::Owned)
      -> AsyncFd {
    return AsyncFd{&ctx, fd, ownership};
  }

  /// Create with borrowed ownership (won't close on destruction)
  [[nodiscard]] static auto borrow(IoContext& ctx, int fd) -> AsyncFd {
    return AsyncFd{&ctx, fd, Ownership::Borrowed};
  }

  // ==========================================================================
  // Constructors / Destructor
  // ==========================================================================

  /// Default constructor - invalid handle
  AsyncFd() noexcept = default;

  /// Move constructor
  AsyncFd(AsyncFd&& other) noexcept
      : ctx_(std::exchange(other.ctx_, nullptr)),
        fd_(std::exchange(other.fd_, -1)),
        ownership_(std::exchange(other.ownership_, Ownership::Borrowed)) {}

  /// Move assignment
  auto operator=(AsyncFd&& other) noexcept -> AsyncFd& {
    if (this != &other) {
      close_sync();
      ctx_ = std::exchange(other.ctx_, nullptr);
      fd_ = std::exchange(other.fd_, -1);
      ownership_ = std::exchange(other.ownership_, Ownership::Borrowed);
    }
    return *this;
  }

  // Non-copyable
  AsyncFd(const AsyncFd&) = delete;
  auto operator=(const AsyncFd&) -> AsyncFd& = delete;

  /// Destructor - closes fd if owned
  ~AsyncFd() { close_sync(); }

  // ==========================================================================
  // Accessors
  // ==========================================================================

  /// Get raw file descriptor
  [[nodiscard]] auto fd() const noexcept -> int { return fd_; }

  /// Get raw file descriptor (alias)
  [[nodiscard]] auto native_handle() const noexcept -> int { return fd_; }

  /// Check if handle is valid
  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_ >= 0; }

  /// Implicit conversion to bool
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  /// Get the bound IoContext
  [[nodiscard]] auto context() noexcept -> IoContext& {
    assert(ctx_ != nullptr);
    return *ctx_;
  }

  /// Get the bound IoContext (const)
  [[nodiscard]] auto context() const noexcept -> const IoContext& {
    assert(ctx_ != nullptr);
    return *ctx_;
  }

  /// Check if context is bound
  [[nodiscard]] auto has_context() const noexcept -> bool {
    return ctx_ != nullptr;
  }

  // ==========================================================================
  // Ownership Management
  // ==========================================================================

  /// Release ownership and return raw fd
  [[nodiscard]] auto release() noexcept -> int {
    ownership_ = Ownership::Borrowed;
    return std::exchange(fd_, -1);
  }

  /// Reset to new file descriptor
  auto reset(int fd = -1, Ownership ownership = Ownership::Owned) noexcept
      -> void {
    close_sync();
    fd_ = fd;
    ownership_ = ownership;
  }

  /// Detach from context (for moving to another context)
  [[nodiscard]] auto detach() noexcept -> std::pair<int, Ownership> {
    auto result = std::make_pair(fd_, ownership_);
    fd_ = -1;
    ownership_ = Ownership::Borrowed;
    ctx_ = nullptr;
    return result;
  }

  /// Rebind to a new context
  auto rebind(IoContext& ctx) noexcept -> void { ctx_ = &ctx; }

  // ==========================================================================
  // Async Operations - The key improvement over raw fd APIs
  // ==========================================================================

  /// Async read into buffer
  [[nodiscard]] auto async_read(MutableBuffer buffer, std::uint64_t offset = 0)
      -> IoAwaitable<ops::Read> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_read(fd_, buffer, offset);
  }

  /// Async read at offset (for regular files)
  [[nodiscard]] auto async_read_at(MutableBuffer buffer, std::uint64_t offset)
      -> IoAwaitable<ops::Read> {
    return async_read(buffer, offset);
  }

  /// Async write from buffer
  [[nodiscard]] auto async_write(ConstBuffer buffer, std::uint64_t offset = 0)
      -> IoAwaitable<ops::Write> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_write(fd_, buffer, offset);
  }

  /// Async write at offset (for regular files)
  [[nodiscard]] auto async_write_at(ConstBuffer buffer, std::uint64_t offset)
      -> IoAwaitable<ops::Write> {
    return async_write(buffer, offset);
  }

  /// Async close
  [[nodiscard]] auto async_close() -> IoAwaitable<ops::Close> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    auto result = ctx_->async_close(fd_);
    fd_ = -1;  // Mark as closed
    return result;
  }

  /// Async poll for events
  [[nodiscard]] auto async_poll(std::uint32_t events) -> IoAwaitable<ops::Poll> {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_poll(fd_, events);
  }

  /// Async poll for events with timeout
  [[nodiscard]] auto async_poll_timeout(std::uint32_t events,
                                        std::chrono::milliseconds timeout)
      -> PollTimeoutAwaitable {
    assert(ctx_ != nullptr && "AsyncFd not bound to IoContext");
    return ctx_->async_poll_timeout(fd_, events, timeout);
  }

  // ==========================================================================
  // Convenience Methods
  // ==========================================================================

  /// Read all available data into a string
  [[nodiscard]] auto read_all(std::size_t max_size = 1024 * 1024)
      -> task<IoExpected<std::string>>;

  /// Write all data from buffer
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

// ============================================================================
// AsyncPipe - Connected pair of async file descriptors
// ============================================================================

struct AsyncPipe {
  AsyncFd read_end;
  AsyncFd write_end;

  /// Create a new pipe bound to the given context
  [[nodiscard]] static auto create(IoContext& ctx) -> IoExpected<AsyncPipe>;

  /// Create a new pipe with flags (O_CLOEXEC, O_NONBLOCK, etc.)
  [[nodiscard]] static auto create(IoContext& ctx, int flags)
      -> IoExpected<AsyncPipe>;
};

// ============================================================================
// AsyncEventFd - Async eventfd for inter-thread signaling
// ============================================================================

class AsyncEventFd {
public:
  /// Create eventfd with specified flags
  [[nodiscard]] static auto create(IoContext& ctx, unsigned int initval = 0,
                                   int flags = EFD_NONBLOCK)
      -> IoExpected<AsyncEventFd>;

  /// Default constructor - invalid
  AsyncEventFd() noexcept = default;

  /// Move constructor
  AsyncEventFd(AsyncEventFd&& other) noexcept
      : fd_(std::move(other.fd_)) {}

  /// Move assignment
  auto operator=(AsyncEventFd&& other) noexcept -> AsyncEventFd& {
    fd_ = std::move(other.fd_);
    return *this;
  }

  // Non-copyable
  AsyncEventFd(const AsyncEventFd&) = delete;
  auto operator=(const AsyncEventFd&) -> AsyncEventFd& = delete;

  /// Get raw file descriptor
  [[nodiscard]] auto native_handle() const noexcept -> int {
    return fd_.fd();
  }
  [[nodiscard]] auto fd() const noexcept -> int { return fd_.fd(); }

  /// Check if valid
  [[nodiscard]] auto is_open() const noexcept -> bool { return fd_.is_open(); }
  [[nodiscard]] explicit operator bool() const noexcept { return is_open(); }

  /// Signal the eventfd (write) - synchronous
  auto signal(std::uint64_t value = 1) noexcept -> bool;

  /// Consume all pending signals (read and discard) - synchronous
  auto consume() noexcept -> std::uint64_t;

  /// Async wait for signal
  [[nodiscard]] auto async_wait() -> IoAwaitable<ops::Poll> {
    return fd_.async_poll(POLLIN);
  }

  /// Access underlying AsyncFd
  [[nodiscard]] auto async_fd() noexcept -> AsyncFd& { return fd_; }
  [[nodiscard]] auto async_fd() const noexcept -> const AsyncFd& { return fd_; }

private:
  explicit AsyncEventFd(AsyncFd fd) noexcept : fd_(std::move(fd)) {}

  AsyncFd fd_;
};

// ============================================================================
// Factory Functions
// ============================================================================

/// Create AsyncFd from raw fd (takes ownership)
[[nodiscard]] inline auto make_async_fd(IoContext& ctx, int fd) -> AsyncFd {
  return AsyncFd::from_raw(ctx, fd, Ownership::Owned);
}

/// Create AsyncFd from raw fd (borrows, no ownership)
[[nodiscard]] inline auto borrow_async_fd(IoContext& ctx, int fd) -> AsyncFd {
  return AsyncFd::borrow(ctx, fd);
}

}  // namespace taskmaster::io
