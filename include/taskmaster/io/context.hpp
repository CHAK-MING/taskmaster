#pragma once

#include "taskmaster/io/buffer.hpp"
#include "taskmaster/io/result.hpp"

#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>
#include <memory_resource>
#include <span>
#include <unordered_set>
#include <mutex>

namespace taskmaster::io {

class IoContext;
class IoContextImpl;

constexpr unsigned kInvalidShard = ~0u;
constexpr std::uint32_t kDefaultQueueDepth = 256;
constexpr std::uintptr_t kWakeEventToken = 0x1;
constexpr std::uintptr_t kMsgRingWakeToken = 0x2;
constexpr std::uint32_t kCqeFMore = (1U << 1);

namespace ops {

struct Read {
  int fd;
  MutableBuffer buffer;
  std::uint64_t offset{0};
};

struct Write {
  int fd;
  ConstBuffer buffer;
  std::uint64_t offset{0};
};

struct ReadV {
  int fd;
  std::span<MutableBuffer> buffers;
  std::uint64_t offset{0};
};

struct WriteV {
  int fd;
  std::span<ConstBuffer> buffers;
  std::uint64_t offset{0};
};

struct Accept {
  int listen_fd;
};

struct Connect {
  int fd;
  const void* addr;
  std::uint32_t addrlen;
};

struct Close {
  int fd;
};

struct Timeout {
  std::chrono::nanoseconds duration;
};

struct Cancel {
  void* operation_data;
};

struct Poll {
  int fd;
  std::uint32_t events;
};

struct PollTimeout {
  int fd;
  std::uint32_t events;
  std::chrono::milliseconds timeout;
};

struct Nop {};

}  // namespace ops

enum class IoOpType : std::uint8_t {
  Read,
  Write,
  Connect,
  Poll,
  PollTimeout,
  Timeout,
  Close,
  Cancel,
  Nop,
  MsgRing
};

struct alignas(8) KernelTimespec {
  std::int64_t tv_sec;
  long long tv_nsec;
};

struct CompletionData {
  std::coroutine_handle<> continuation{};
  std::int32_t result{0};
  std::uint32_t flags{0};
  IoContext* context{nullptr};
  unsigned owner_shard{kInvalidShard};
  KernelTimespec ts{};  // For timeout storage
  bool completed{false};
  bool cancelled{false};
};

struct PollResult {
  bool ready = false;
  bool timed_out = false;
  std::error_code error{};

  [[nodiscard]] explicit operator bool() const noexcept { return ready; }
  [[nodiscard]] auto has_error() const noexcept -> bool {
    return !ready && !timed_out && static_cast<bool>(error);
  }
};

struct IoRequest {
  IoOpType op{IoOpType::Nop};
  CompletionData* data{nullptr};
  int fd{-1};
  void* buf{nullptr};
  std::uint32_t len{0};
  std::uint64_t offset{0};
  std::uint32_t poll_mask{0};
  KernelTimespec ts{};
  KernelTimespec* ts_ptr{nullptr};
  bool has_link_timeout{false};
  std::uint64_t cancel_user_data{0};
  std::uint64_t msg_ring_data{0};
};

template <typename Callback>
concept CompletionHandler =
    std::invocable<Callback, void*, std::int32_t, std::uint32_t>;

/// Base awaitable for all I/O operations
template <typename Operation>
class IoAwaitable {
public:
  IoAwaitable(IoContext& ctx, Operation op) noexcept
      : context_(&ctx), operation_(std::move(op)) {}

  IoAwaitable(IoAwaitable&& other) noexcept
      : context_(other.context_),
        operation_(std::move(other.operation_)),
        data_(other.data_) {
    other.context_ = nullptr;
    other.data_ = nullptr;
  }

  ~IoAwaitable();

  IoAwaitable& operator=(IoAwaitable&&) = delete;
  IoAwaitable(const IoAwaitable&) = delete;
  IoAwaitable& operator=(const IoAwaitable&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;  // Always suspend to submit to io_uring
  }

  auto await_suspend(std::coroutine_handle<> h) noexcept -> void;

  [[nodiscard]] auto await_resume() noexcept -> IoResult;

private:
  auto cleanup_on_destroy() noexcept -> void;

protected:
  IoContext* context_;
  Operation operation_;
  CompletionData* data_{nullptr};
};

/// Specialized awaitable for timeout operations (returns void)
class TimeoutAwaitable {
public:
  TimeoutAwaitable(IoContext& ctx, std::chrono::nanoseconds duration) noexcept;

  TimeoutAwaitable(TimeoutAwaitable&& other) noexcept;
  ~TimeoutAwaitable();

  TimeoutAwaitable& operator=(TimeoutAwaitable&&) = delete;
  TimeoutAwaitable(const TimeoutAwaitable&) = delete;
  TimeoutAwaitable& operator=(const TimeoutAwaitable&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }
  auto await_suspend(std::coroutine_handle<> h) noexcept -> void;
  auto await_resume() noexcept -> void;

private:
  IoContext* context_;
  std::chrono::nanoseconds duration_;
  CompletionData* data_{nullptr};
};

/// Specialized awaitable for poll with timeout (returns PollResult)
class PollTimeoutAwaitable {
public:
  PollTimeoutAwaitable(IoContext& ctx, int fd, std::uint32_t events,
                       std::chrono::milliseconds timeout) noexcept
      : context_(&ctx), fd_(fd), events_(events), timeout_(timeout) {}

  PollTimeoutAwaitable(PollTimeoutAwaitable&& other) noexcept
      : context_(other.context_),
        fd_(other.fd_),
        events_(other.events_),
        timeout_(other.timeout_),
        data_(other.data_) {
    other.context_ = nullptr;
    other.data_ = nullptr;
  }

  ~PollTimeoutAwaitable();

  PollTimeoutAwaitable& operator=(PollTimeoutAwaitable&&) = delete;
  PollTimeoutAwaitable(const PollTimeoutAwaitable&) = delete;
  PollTimeoutAwaitable& operator=(const PollTimeoutAwaitable&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool { return false; }
  auto await_suspend(std::coroutine_handle<> h) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept -> PollResult;

private:
  IoContext* context_;
  int fd_;
  std::uint32_t events_;
  std::chrono::milliseconds timeout_;
  CompletionData* data_{nullptr};
};

class IoContext {
public:
  /// Create IoContext with specified queue depth and optional memory resource
  explicit IoContext(std::uint32_t queue_depth = kDefaultQueueDepth,
                     std::pmr::memory_resource* mr = nullptr);
  ~IoContext();

  // Non-copyable, non-movable
  IoContext(const IoContext&) = delete;
  IoContext& operator=(const IoContext&) = delete;
  IoContext(IoContext&&) = delete;
  IoContext& operator=(IoContext&&) = delete;

  /// Check if context is valid
  [[nodiscard]] auto valid() const noexcept -> bool;

  /// Async read from file descriptor
  [[nodiscard]] auto async_read(int fd, MutableBuffer buffer,
                                std::uint64_t offset = 0)
      -> IoAwaitable<ops::Read>;

  /// Async write to file descriptor
  [[nodiscard]] auto async_write(int fd, ConstBuffer buffer,
                                 std::uint64_t offset = 0)
      -> IoAwaitable<ops::Write>;

  /// Async accept connection
  [[nodiscard]] auto async_accept(int listen_fd) -> IoAwaitable<ops::Accept>;

  /// Async connect to address
  [[nodiscard]] auto async_connect(int fd, const void* addr,
                                   std::uint32_t addrlen)
      -> IoAwaitable<ops::Connect>;

  /// Async close file descriptor
  [[nodiscard]] auto async_close(int fd) -> IoAwaitable<ops::Close>;

  /// Async timeout/sleep
  [[nodiscard]] auto async_timeout(std::chrono::nanoseconds duration)
      -> TimeoutAwaitable;

  /// Async poll for events
  [[nodiscard]] auto async_poll(int fd, std::uint32_t events)
      -> IoAwaitable<ops::Poll>;

  /// Async poll for events with timeout
  [[nodiscard]] auto async_poll_timeout(int fd, std::uint32_t events,
                                        std::chrono::milliseconds timeout)
      -> PollTimeoutAwaitable;

      /// Prepare an I/O request
  [[nodiscard]] auto prepare(const IoRequest& req) -> bool;

  /// Submit pending operations
  [[nodiscard]] auto submit(bool force = false) -> int;

  /// Wait for completions with timeout
  auto wait(std::chrono::milliseconds timeout) -> void;

  /// Setup multi-shot poll on fd (for wake eventfd)
  auto setup_wake_poll(int fd) -> void;

  /// Process completions with custom callback (type-erased)
  using CompletionCallback =
      std::move_only_function<void(void*, std::int32_t, std::uint32_t)>;
  auto process_completions(CompletionCallback cb) -> unsigned;

  /// Process completions with custom callback (template version)
  template <typename Callback>
    requires std::invocable<Callback&, void*, std::int32_t, std::uint32_t>
  auto process_completions(Callback&& cb) -> unsigned {
    return process_completions(CompletionCallback(std::forward<Callback>(cb)));
  }

  /// Run the event loop until all operations complete
  auto run() -> void;

  /// Run the event loop for one iteration
  auto run_one() -> std::size_t;

  /// Run ready completions without blocking
  auto poll() -> std::size_t;

  /// Stop the event loop
  auto stop() -> void;

  /// Check if event loop is stopped
  [[nodiscard]] auto stopped() const noexcept -> bool;

  /// Reset stopped state
  auto restart() -> void;

  /// Get implementation (for template methods)
  [[nodiscard]] auto impl() noexcept -> IoContextImpl*;

  /// Get io_uring ring fd (for msg_ring cross-shard wakeup)
  [[nodiscard]] auto ring_fd() const noexcept -> int;

  using TrackerCallback = std::move_only_function<void(CompletionData*)>;
  auto set_completion_tracker(TrackerCallback track,
                              TrackerCallback untrack) -> void;
  auto track_completion(CompletionData* data) noexcept -> void;
  auto untrack_completion(CompletionData* data) noexcept -> void;
  auto cleanup_completion_data(CompletionData* data) noexcept -> void;

  [[nodiscard]] inline auto allocate_completion() -> CompletionData* {
    void* mem = memory_resource_->allocate(sizeof(CompletionData), alignof(CompletionData));
    return new (mem) CompletionData{};
  }
  
  inline auto deallocate_completion(CompletionData* data) noexcept -> void {
    if (!data) return;
    data->~CompletionData();
    memory_resource_->deallocate(data, sizeof(CompletionData), alignof(CompletionData));
  }

  // Internal submission methods - used by IoAwaitable
  auto submit_read(CompletionData* data, int fd, MutableBuffer buffer,
                   std::uint64_t offset) -> void;
  auto submit_write(CompletionData* data, int fd, ConstBuffer buffer,
                     std::uint64_t offset) -> void;
  auto submit_accept(CompletionData* data, int fd) -> void;
  auto submit_connect(CompletionData* data, int fd, const void* addr,
                      std::uint32_t addrlen) -> void;
  auto submit_close(CompletionData* data, int fd) -> void;
  auto submit_timeout(CompletionData* data, std::chrono::nanoseconds duration)
      -> void;
  auto submit_poll(CompletionData* data, int fd, std::uint32_t events) -> void;
  auto submit_poll_timeout(CompletionData* data, int fd, std::uint32_t events,
                           std::chrono::milliseconds timeout) -> void;
  auto submit_cancel(void* operation_data) -> void;

private:
  std::unique_ptr<IoContextImpl> impl_;
  TrackerCallback track_cb_;
  TrackerCallback untrack_cb_;
  std::pmr::memory_resource* memory_resource_{nullptr};
};

/// Create async read awaitable
template <MutableBufferSequence Buffer>
[[nodiscard]] auto async_read(IoContext& ctx, int fd, Buffer& buffer,
                              std::uint64_t offset = 0) {
  return ctx.async_read(fd, io::buffer(buffer), offset);
}

/// Create async write awaitable
template <ConstBufferSequence Buffer>
[[nodiscard]] auto async_write(IoContext& ctx, int fd, const Buffer& buffer,
                               std::uint64_t offset = 0) {
  return ctx.async_write(fd, io::buffer(buffer), offset);
}

/// Async sleep for duration
[[nodiscard]] inline auto async_sleep(IoContext& ctx,
                                      std::chrono::nanoseconds duration) {
  return ctx.async_timeout(duration);
}

/// Async sleep with chrono duration
template <typename Rep, typename Period>
[[nodiscard]] auto async_sleep(IoContext& ctx,
                               std::chrono::duration<Rep, Period> duration) {
  return ctx.async_timeout(
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

}  // namespace taskmaster::io
