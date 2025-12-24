#pragma once

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <system_error>
#include <thread>
#include <variant>
#include <vector>

#include <liburing.h>

#include "taskmaster/cancellation.hpp"
#include "taskmaster/lockfree_queue.hpp"

namespace taskmaster {

inline constexpr std::uint32_t RING_SIZE = 256;

using shard_id = unsigned;
inline constexpr shard_id INVALID_SHARD = ~0u;

inline thread_local shard_id current_shard_id_ = INVALID_SHARD;
inline thread_local class Runtime *current_runtime_ = nullptr;

[[nodiscard]] inline auto this_shard_id() noexcept -> shard_id {
  return current_shard_id_;
}

[[nodiscard]] inline auto this_runtime() noexcept -> Runtime * {
  return current_runtime_;
}

struct io_data {
  void *coroutine = nullptr;
  std::int32_t result = 0;
  std::uint32_t flags = 0;
  __kernel_timespec ts{};
  shard_id owner_shard = INVALID_SHARD;
  std::uint64_t user_data = 0; // For cancel tracking
};

struct PollResult {
  bool ready = false;
  bool timed_out = false;
  std::errc error{};

  [[nodiscard]] explicit operator bool() const noexcept { return ready; }
  [[nodiscard]] auto has_error() const noexcept -> bool {
    return !ready && !timed_out && error != std::errc{};
  }
};

enum class IoOpType : std::uint8_t {
  Read,
  Write,
  Poll,
  PollTimeout,
  Timeout,
  Close,
  Cancel,
  Nop
};

struct IoRequest {
  IoOpType op{IoOpType::Nop};
  io_data *data{nullptr};
  int fd{-1};
  void *buf{nullptr};
  std::uint32_t len{0};
  std::uint64_t offset{0};
  std::uint32_t poll_mask{0};
  __kernel_timespec ts{};
  __kernel_timespec *ts_ptr{nullptr};
  bool has_link_timeout{false};
  std::uint64_t cancel_user_data{0}; // user_data of SQE to cancel
};

inline constexpr std::uintptr_t WAKE_EVENT_TOKEN = 0x1;

struct ShardLocal {
  shard_id id = INVALID_SHARD;
  io_uring ring{};
  bool ring_initialized = false;
  int wake_fd = -1;

  std::optional<std::coroutine_handle<>> run_next;
  std::deque<std::coroutine_handle<>> local_queue;
  BoundedMPSCQueue<std::coroutine_handle<>> remote_queue{4096};
  std::deque<IoRequest> io_queue;

  std::atomic<bool> sleeping{false};
  std::uint32_t pending_sqe_count{0};
  static constexpr std::uint32_t SUBMIT_BATCH_SIZE = 8;
};

struct SmpMessage {
  std::variant<std::coroutine_handle<>, std::move_only_function<void()>>
      payload;

  explicit SmpMessage(std::coroutine_handle<> h) : payload(h) {}
  explicit SmpMessage(std::move_only_function<void()> f)
      : payload(std::move(f)) {}

  auto execute() -> void {
    std::visit(
        [](auto &&arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
            if (arg && !arg.done())
              arg.resume();
          } else {
            arg();
          }
        },
        payload);
  }
};

class Runtime {
public:
  explicit Runtime(unsigned num_shards = 0);
  ~Runtime();

  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto schedule(std::coroutine_handle<> handle) -> void;
  auto schedule_on(shard_id target, std::coroutine_handle<> handle) -> void;

  // Schedule from external thread (non-shard context)
  auto schedule_external(std::coroutine_handle<> handle) -> void;

  auto submit_to(shard_id target, std::coroutine_handle<> handle) -> void {
    if (target == current_shard_id_) {
      if (handle && !handle.done())
        handle.resume();
    } else {
      (void)smp_queues_[current_shard_id_][target]->push(SmpMessage{handle});
      wake_shard(target);
    }
  }

  template <typename Func>
    requires std::invocable<Func>
  auto submit_to(shard_id target, Func &&func) -> void {
    if (target == current_shard_id_) {
      func();
    } else {
      (void)smp_queues_[current_shard_id_][target]->push(SmpMessage{
          std::move_only_function<void()>{std::forward<Func>(func)}});
      wake_shard(target);
    }
  }

  auto submit_io(IoRequest req) -> bool;

  [[nodiscard]] auto shard_count() const noexcept -> unsigned {
    return num_shards_;
  }

private:
  auto run_shard(shard_id id) -> void;
  auto process_ready_queue(ShardLocal &shard) -> bool;
  auto process_smp_messages(shard_id id) -> bool;
  auto process_io_requests(ShardLocal &shard) -> void;
  auto flush_submissions(ShardLocal &shard, bool force = false) -> bool;
  auto process_completions(ShardLocal &shard) -> bool;
  auto wait_for_work(ShardLocal &shard) -> void;
  auto wake_shard(shard_id id) -> void;
  auto setup_multishot_poll(ShardLocal &shard) -> void;

  unsigned num_shards_;
  std::vector<std::unique_ptr<ShardLocal>> shards_;
  std::vector<std::thread> threads_;
  std::vector<std::vector<
      std::unique_ptr<SPSCQueue<SmpMessage, std::allocator<SmpMessage>>>>>
      smp_queues_;

  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
};

[[nodiscard]] inline auto decode_result(std::int32_t result) noexcept
    -> std::expected<std::uint32_t, std::errc> {
  if (result < 0)
    return std::unexpected{static_cast<std::errc>(-result)};
  return static_cast<std::uint32_t>(result);
}

[[nodiscard]] inline auto decode_void_result(std::int32_t result) noexcept
    -> std::expected<void, std::errc> {
  if (result < 0)
    return std::unexpected{static_cast<std::errc>(-result)};
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
    data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::Read,
                  .data = &data_,
                  .fd = fd_,
                  .buf = buf_,
                  .len = len_,
                  .offset = offset_};
    this_runtime()->submit_io(req);
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
    data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::Write,
                  .data = &data_,
                  .fd = fd_,
                  .buf = const_cast<void *>(buf_),
                  .len = len_,
                  .offset = offset_};
    this_runtime()->submit_io(req);
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
    data_.owner_shard = this_shard_id();
    IoRequest req{
        .op = IoOpType::Poll, .data = &data_, .fd = fd_, .poll_mask = mask_};
    this_runtime()->submit_io(req);
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

class poll_timeout_awaiter {
public:
  poll_timeout_awaiter(int fd, std::uint32_t mask,
                       std::chrono::milliseconds timeout) noexcept
      : fd_{fd}, mask_{mask} {
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    auto nsecs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(timeout - secs);
    poll_data_.ts.tv_sec = secs.count();
    poll_data_.ts.tv_nsec = nsecs.count();
  }

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    poll_data_.coroutine = handle.address();
    poll_data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::PollTimeout,
                  .data = &poll_data_,
                  .fd = fd_,
                  .poll_mask = mask_,
                  .ts = poll_data_.ts,
                  .ts_ptr = &poll_data_.ts,
                  .has_link_timeout = true};
    this_runtime()->submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept -> PollResult {
    if (poll_data_.result > 0)
      return {.ready = true};
    if (poll_data_.result == -ECANCELED)
      return {.timed_out = true};
    if (poll_data_.result < 0) {
      return {.error = static_cast<std::errc>(-poll_data_.result)};
    }
    return {};
  }

private:
  io_data poll_data_;
  int fd_;
  std::uint32_t mask_;
};

class sleep_awaiter {
public:
  explicit sleep_awaiter(std::chrono::milliseconds duration) noexcept {
    auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto nsecs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
    data_.ts.tv_sec = secs.count();
    data_.ts.tv_nsec = nsecs.count();
  }

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::Timeout,
                  .data = &data_,
                  .ts = data_.ts,
                  .ts_ptr = &data_.ts};
    this_runtime()->submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept
      -> std::expected<void, std::errc> {
    if (data_.result == -ETIME)
      return {};
    return decode_void_result(data_.result);
  }

private:
  io_data data_;
};

class close_awaiter {
public:
  explicit close_awaiter(int fd) noexcept : fd_{fd} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::Close, .data = &data_, .fd = fd_};
    this_runtime()->submit_io(req);
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

class cancel_awaiter {
public:
  explicit cancel_awaiter(std::uint64_t user_data) noexcept
      : cancel_user_data_{user_data} {}

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    data_.coroutine = handle.address();
    data_.owner_shard = this_shard_id();
    IoRequest req{.op = IoOpType::Cancel,
                  .data = &data_,
                  .cancel_user_data = cancel_user_data_};
    this_runtime()->submit_io(req);
  }

  [[nodiscard]] auto await_resume() const noexcept -> bool {
    return data_.result >= 0;
  }

private:
  io_data data_;
  std::uint64_t cancel_user_data_;
};

[[nodiscard]] inline auto async_cancel(std::uint64_t user_data) noexcept
    -> cancel_awaiter {
  return cancel_awaiter{user_data};
}

[[nodiscard]] inline auto async_cancel(io_data *data) noexcept
    -> cancel_awaiter {
  return cancel_awaiter{reinterpret_cast<std::uint64_t>(data)};
}

// Awaiter that ensures coroutine runs in shard context
// If already in shard context, continues immediately
// If not, schedules to shard 0 via remote_queue
class ensure_shard_context {
public:
  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return this_shard_id() != INVALID_SHARD;
  }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> bool {
    auto *rt = this_runtime();
    if (rt) {
      rt->schedule(handle);
      return true;
    }
    // No runtime available - this is a programming error
    // Resume immediately and let the coroutine fail at IO submission
    return false;
  }

  auto await_resume() const noexcept -> void {}
};

} // namespace taskmaster
