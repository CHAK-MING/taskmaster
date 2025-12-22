#pragma once

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <expected>
#include <functional>
#include <liburing.h>
#include <span>
#include <system_error>
#include <thread>

#include "taskmaster/lockfree_queue.hpp"

namespace taskmaster {

// io_uring submission queue size. Must be power of 2.
inline constexpr std::uint32_t RING_SIZE = 256;

struct io_data {
  void *coroutine = nullptr;
  std::int32_t result = 0;
  std::uint32_t flags = 0;
};

// IO request types for thread-safe submission
enum class IoOpType : std::uint8_t {
  Read,
  Write,
  Poll,
  PollTimeout,
  Timeout,
  Close,
  Nop
};

struct IoRequest {
  IoOpType op;
  io_data *data;
  int fd;
  void *buf;
  std::uint32_t len;
  std::uint64_t offset;
  std::uint32_t poll_mask;
  __kernel_timespec ts;
  __kernel_timespec *ts_ptr; // For poll_timeout linked operations
  bool has_link_timeout;
};

class Runtime {
public:
  static auto instance() -> Runtime &;

  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto schedule(std::coroutine_handle<> handle) -> void;

  // Thread-safe IO submission - awaiter calls this instead of accessing ring
  // directly
  auto submit_io(IoRequest req) -> bool;

private:
  Runtime();
  ~Runtime();

  auto run() -> void;
  auto process_ready_queue() -> void;
  auto process_io_requests() -> void;
  auto process_completions() -> void;
  auto wake() -> void;

  io_uring ring_{};
  bool ring_initialized_ = false;
  int wake_fd_ = -1;

  LockFreeQueue<std::coroutine_handle<>> ready_queue_{4096};

  LockFreeQueue<IoRequest> io_queue_{4096};

  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> sleeping_{false};
  std::thread thread_;
};

[[nodiscard]] inline auto decode_result(std::int32_t result) noexcept
    -> std::expected<std::uint32_t, std::errc> {
  if (result < 0) {
    return std::unexpected{static_cast<std::errc>(-result)};
  }
  return static_cast<std::uint32_t>(result);
}

[[nodiscard]] inline auto decode_void_result(std::int32_t result) noexcept
    -> std::expected<void, std::errc> {
  if (result < 0) {
    return std::unexpected{static_cast<std::errc>(-result)};
  }
  return {};
}

class read_awaiter {
public:
  read_awaiter(int fd, void *buf, std::uint32_t len,
               std::uint64_t offset = 0) noexcept
      : fd_{fd}, buf_{buf}, len_{len}, offset_{offset} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::Read;
    req.data = &data_;
    req.fd = fd_;
    req.buf = buf_;
    req.len = len_;
    req.offset = offset_;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<std::uint32_t, std::errc> {
    return decode_result(data_.result);
  }

private:
  io_data data_;
  int fd_;
  void *buf_;
  std::uint32_t len_;
  std::uint64_t offset_;
};

class write_awaiter {
public:
  write_awaiter(int fd, const void *buf, std::uint32_t len,
                std::uint64_t offset = 0) noexcept
      : fd_{fd}, buf_{buf}, len_{len}, offset_{offset} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::Write;
    req.data = &data_;
    req.fd = fd_;
    req.buf = const_cast<void *>(buf_);
    req.len = len_;
    req.offset = offset_;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<std::uint32_t, std::errc> {
    return decode_result(data_.result);
  }

private:
  io_data data_;
  int fd_;
  const void *buf_;
  std::uint32_t len_;
  std::uint64_t offset_;
};

class poll_awaiter {
public:
  poll_awaiter(int fd, std::uint32_t mask) noexcept : fd_{fd}, mask_{mask} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::Poll;
    req.data = &data_;
    req.fd = fd_;
    req.poll_mask = mask_;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<std::uint32_t, std::errc> {
    return decode_result(data_.result);
  }

private:
  io_data data_;
  int fd_;
  std::uint32_t mask_;
};

// Poll with timeout - wakes on either fd event or timeout
class poll_timeout_awaiter {
public:
  poll_timeout_awaiter(int fd, std::uint32_t mask,
                       std::chrono::milliseconds timeout) noexcept
      : fd_{fd}, mask_{mask} {
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    auto nsecs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(timeout - secs);
    ts_.tv_sec = secs.count();
    ts_.tv_nsec = nsecs.count();
  }

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    poll_data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::PollTimeout;
    req.data = &poll_data_;
    req.fd = fd_;
    req.poll_mask = mask_;
    req.ts = ts_;
    req.ts_ptr = &ts_;
    req.has_link_timeout = true;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept -> bool {
    // Returns true if poll succeeded (event arrived), false if
    // timeout/cancelled
    return poll_data_.result > 0;
  }

private:
  io_data poll_data_;
  int fd_;
  std::uint32_t mask_;
  __kernel_timespec ts_{};
};

class sleep_awaiter {
public:
  explicit sleep_awaiter(std::chrono::milliseconds duration) noexcept {
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto nsecs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
    ts_.tv_sec = secs.count();
    ts_.tv_nsec = nsecs.count();
  }

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::Timeout;
    req.data = &data_;
    req.ts = ts_;
    req.ts_ptr = &ts_;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<void, std::errc> {
    if (data_.result == -ETIME) {
      return {};
    }
    return decode_void_result(data_.result);
  }

private:
  io_data data_;
  __kernel_timespec ts_{};
};

class close_awaiter {
public:
  explicit close_awaiter(int fd) noexcept : fd_{fd} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    IoRequest req{};
    req.op = IoOpType::Close;
    req.data = &data_;
    req.fd = fd_;
    Runtime::instance().submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<void, std::errc> {
    return decode_void_result(data_.result);
  }

private:
  io_data data_;
  int fd_;
};

[[nodiscard]] inline auto async_read(int fd, void *buf,
                                     std::uint32_t len) noexcept
    -> read_awaiter {
  return read_awaiter{fd, buf, len, 0};
}

[[nodiscard]] inline auto async_read(int fd, std::span<std::byte> buf) noexcept
    -> read_awaiter {
  return read_awaiter{fd, buf.data(), static_cast<std::uint32_t>(buf.size()),
                      0};
}

[[nodiscard]] inline auto async_write(int fd, const void *buf,
                                      std::uint32_t len) noexcept
    -> write_awaiter {
  return write_awaiter{fd, buf, len, 0};
}

[[nodiscard]] inline auto async_write(int fd,
                                      std::span<const std::byte> buf) noexcept
    -> write_awaiter {
  return write_awaiter{fd, buf.data(), static_cast<std::uint32_t>(buf.size()),
                       0};
}

[[nodiscard]] inline auto async_poll(int fd, std::uint32_t mask) noexcept
    -> poll_awaiter {
  return poll_awaiter{fd, mask};
}

[[nodiscard]] inline auto
async_poll_timeout(int fd, std::uint32_t mask,
                   std::chrono::milliseconds timeout) noexcept
    -> poll_timeout_awaiter {
  return poll_timeout_awaiter{fd, mask, timeout};
}

[[nodiscard]] inline auto
async_sleep(std::chrono::milliseconds duration) noexcept -> sleep_awaiter {
  return sleep_awaiter{duration};
}

[[nodiscard]] inline auto async_close(int fd) noexcept -> close_awaiter {
  return close_awaiter{fd};
}

} // namespace taskmaster
