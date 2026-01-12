#include "taskmaster/io/context.hpp"
#include "taskmaster/io/result.hpp"
#include "taskmaster/util/log.hpp"

#include <liburing.h>
#include <poll.h>

#include <cerrno>
#include <cstring>
#include <memory_resource>
#include <unordered_set>
#include <mutex>

namespace taskmaster::io {

// ============================================================================
// Error Conversion
// ============================================================================

auto from_errno(int err) noexcept -> std::error_code {
  switch (err) {
    case 0: return {};
    case ECANCELED: return make_error_code(IoError::Cancelled);
    case ETIMEDOUT: return make_error_code(IoError::TimedOut);
    case ECONNRESET: return make_error_code(IoError::ConnectionReset);
    case ECONNREFUSED: return make_error_code(IoError::ConnectionRefused);
    case EPIPE: return make_error_code(IoError::BrokenPipe);
    case EAGAIN: return make_error_code(IoError::WouldBlock);
    // Note: EWOULDBLOCK == EAGAIN on Linux, so no separate case needed
    case EINVAL: return make_error_code(IoError::InvalidArgument);
    case EBADF: return make_error_code(IoError::BadDescriptor);
    case ENOBUFS: return make_error_code(IoError::NoBufferSpace);
    case EINPROGRESS: return make_error_code(IoError::OperationInProgress);
    case ENOTCONN: return make_error_code(IoError::NotConnected);
    case EISCONN: return make_error_code(IoError::AlreadyConnected);
    default: return {err, std::system_category()};
  }
}

class IoContextImpl {
public:
  static constexpr std::uint32_t BATCH_SIZE = 8;

  explicit IoContextImpl(std::uint32_t queue_depth) : queue_depth_(queue_depth) {
      unsigned flags = IORING_SETUP_SQPOLL;
      int ret = io_uring_queue_init(queue_depth_, &ring_, flags);
      
      if (ret < 0) {
        log::debug("SQPOLL not available ({}), falling back to normal mode", strerror(-ret));
        flags = 0;
        ret = io_uring_queue_init(queue_depth_, &ring_, flags);
      }
      
      if (ret < 0) {
        log::error("Failed to initialize io_uring: queue_depth={}, error={} ({})",
                   queue_depth, ret, strerror(-ret));
        initialized_ = false;
        return;
      }
      initialized_ = true;
      sqpoll_enabled_ = (flags & IORING_SETUP_SQPOLL) != 0;
      log::debug("io_uring initialized: queue_depth={}, sqpoll={}", queue_depth, sqpoll_enabled_);
    }

  ~IoContextImpl() {
    if (initialized_) {
      io_uring_queue_exit(&ring_);
    }
  }

  IoContextImpl(const IoContextImpl&) = delete;
  IoContextImpl& operator=(const IoContextImpl&) = delete;

  [[nodiscard]] auto valid() const noexcept -> bool { return initialized_; }
  [[nodiscard]] auto stopped() const noexcept -> bool { return stopped_; }

  auto stop() noexcept -> void { stopped_ = true; }
  auto restart() noexcept -> void { stopped_ = false; }

  // ==========================================================================
  // Direct preparation methods (for shard integration)
  // ==========================================================================

  auto prepare(const IoRequest& req) -> bool {
    auto* sqe = get_sqe();
    if (!sqe) {
      // Ring is full, submit and retry
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) return false;
    }

    switch (req.op) {
      case IoOpType::Read:
        io_uring_prep_read(sqe, req.fd, req.buf, req.len, req.offset);
        break;

      case IoOpType::Write:
        io_uring_prep_write(sqe, req.fd, req.buf, req.len, req.offset);
        break;

      case IoOpType::Poll:
        io_uring_prep_poll_add(sqe, req.fd, req.poll_mask);
        break;

      case IoOpType::PollTimeout:
        io_uring_prep_poll_add(sqe, req.fd, req.poll_mask);
        if (req.has_link_timeout && req.ts_ptr) {
          io_uring_sqe_set_data(sqe, req.data);
          sqe->flags |= IOSQE_IO_LINK;
          ++pending_count_;

          auto* timeout_sqe = get_sqe();
          if (timeout_sqe) {
            io_uring_prep_link_timeout(
                timeout_sqe,
                reinterpret_cast<__kernel_timespec*>(req.ts_ptr), 0);
            io_uring_sqe_set_data(timeout_sqe, nullptr);
            ++pending_count_;
            return true;
          }
          // Failed to get timeout sqe, prep nop to unlink
          io_uring_prep_nop(sqe);
          io_uring_sqe_set_data(sqe, nullptr);
        }
        break;

      case IoOpType::Timeout:
        io_uring_prep_timeout(sqe,
                              reinterpret_cast<__kernel_timespec*>(req.ts_ptr),
                              0, 0);
        break;

      case IoOpType::Close:
        io_uring_prep_close(sqe, req.fd);
        break;

      case IoOpType::Cancel:
        io_uring_prep_cancel(sqe, reinterpret_cast<void*>(req.cancel_user_data),
                             0);
        break;

      case IoOpType::Nop:
        io_uring_prep_nop(sqe);
        break;
    }

    io_uring_sqe_set_data(sqe, req.data);
    ++pending_count_;
    return true;
  }

  auto submit(bool force) -> int {
    if (!initialized_) return 0;
    if (!force && pending_count_ < BATCH_SIZE) return 0;
    return io_uring_submit(&ring_);
  }

  auto wait(std::chrono::milliseconds timeout) -> void {
    if (!initialized_) return;

    __kernel_timespec ts{};
    ts.tv_sec = timeout.count() / 1000;
    ts.tv_nsec = (timeout.count() % 1000) * 1000000;

    io_uring_cqe* cqe = nullptr;
    io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
  }

  auto setup_wake_poll(int fd) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_poll_multishot(sqe, fd, POLLIN);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(WAKE_EVENT_TOKEN));
    ++pending_count_;
  }

  template <std::invocable<void*, int, unsigned> Callback>
  auto process_completions_impl(Callback&& cb) -> unsigned {
    if (!initialized_) return 0;

    io_uring_cqe* cqe = nullptr;
    unsigned head, count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      cb(io_uring_cqe_get_data(cqe), cqe->res, cqe->flags);
      ++count;
    }
    io_uring_cq_advance(&ring_, count);
    return count;
  }

  // ==========================================================================
  // Awaitable submission methods
  // ==========================================================================

  auto submit_read(CompletionData* data, int fd, MutableBuffer buffer,
                   std::uint64_t offset) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_read(sqe, fd, buffer.data(),
                       static_cast<unsigned>(buffer.size()), offset);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_write(CompletionData* data, int fd, ConstBuffer buffer,
                    std::uint64_t offset) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_write(sqe, fd, buffer.data(),
                        static_cast<unsigned>(buffer.size()), offset);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_accept(CompletionData* data, int fd) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_close(CompletionData* data, int fd) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_timeout(CompletionData* data, std::chrono::nanoseconds duration)
      -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto nsecs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        duration - secs);

    // Store timespec in CompletionData to ensure lifetime
    data->ts.tv_sec = secs.count();
    data->ts.tv_nsec = nsecs.count();

    io_uring_prep_timeout(sqe, reinterpret_cast<__kernel_timespec*>(&data->ts),
                          0, 0);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_poll(CompletionData* data, int fd, std::uint32_t events) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_poll_add(sqe, fd, events);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_cancel(void* operation_data) -> void {
    auto* sqe = get_sqe();
    if (!sqe) return;

    io_uring_prep_cancel(sqe, operation_data, 0);
    io_uring_sqe_set_data(sqe, nullptr);  // No completion needed for cancel
    ++pending_count_;
  }

  // ==========================================================================
  // Event loop methods
  // ==========================================================================

  auto run() -> void {
    while (!stopped_ && pending_count_ > 0) {
      submit_pending();
      wait_and_process();
    }
  }

  auto run_one() -> std::size_t {
    if (stopped_ || pending_count_ == 0) return 0;

    submit_pending();
    return wait_and_process();
  }

  auto poll() -> std::size_t {
    submit_pending();
    return process_completions_internal();
  }

private:
  auto get_sqe() -> io_uring_sqe* {
    auto* sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      // Ring is full, submit and retry
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
    }
    return sqe;
  }

  auto submit_pending() -> void {
    io_uring_submit(&ring_);
  }

  auto wait_and_process() -> std::size_t {
    io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0) return 0;

    return process_completions_internal();
  }

  auto process_completions_internal() -> std::size_t {
    io_uring_cqe* cqe = nullptr;
    unsigned head;
    std::size_t count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      auto* data = static_cast<CompletionData*>(io_uring_cqe_get_data(cqe));
      if (data) {
        data->result = cqe->res;
        data->flags = cqe->flags;
        data->completed = true;

        if (data->cancelled) {
          // Coroutine was destroyed - clean up orphaned data
          if (data->context) {
            data->context->cleanup_completion_data(data);
          } else {
            delete data;
          }
        } else if (data->continuation) {
          data->continuation.resume();
        }
        --pending_count_;
      }
      ++count;
    }

    io_uring_cq_advance(&ring_, static_cast<unsigned>(count));
    return count;
  }

  io_uring ring_{};
  std::uint32_t queue_depth_;
  std::uint32_t pending_count_{0};
  bool initialized_{false};
  bool stopped_{false};
  bool sqpoll_enabled_{false};
};

// ============================================================================
// IoContext Implementation
// ============================================================================

IoContext::IoContext(std::uint32_t queue_depth, std::pmr::memory_resource* mr)
    : impl_(std::make_unique<IoContextImpl>(queue_depth)),
      memory_resource_(mr ? mr : std::pmr::get_default_resource()) {}

IoContext::~IoContext() = default;

auto IoContext::valid() const noexcept -> bool {
  return impl_ && impl_->valid();
}

auto IoContext::stopped() const noexcept -> bool {
  return impl_ && impl_->stopped();
}

auto IoContext::stop() -> void {
  if (impl_) impl_->stop();
}

auto IoContext::restart() -> void {
  if (impl_) impl_->restart();
}

auto IoContext::run() -> void {
  if (impl_) impl_->run();
}

auto IoContext::run_one() -> std::size_t {
  return impl_ ? impl_->run_one() : 0;
}

auto IoContext::poll() -> std::size_t {
  return impl_ ? impl_->poll() : 0;
}

// ==========================================================================
// Direct submission methods
// ==========================================================================

auto IoContext::prepare(const IoRequest& req) -> bool {
  return impl_ ? impl_->prepare(req) : false;
}

auto IoContext::submit(bool force) -> int {
  return impl_ ? impl_->submit(force) : 0;
}

auto IoContext::wait(std::chrono::milliseconds timeout) -> void {
  if (impl_) impl_->wait(timeout);
}

auto IoContext::setup_wake_poll(int fd) -> void {
  if (impl_) impl_->setup_wake_poll(fd);
}

auto IoContext::impl() noexcept -> IoContextImpl* {
  return impl_.get();
}

auto IoContext::set_completion_tracker(TrackerCallback track,
                                       TrackerCallback untrack) -> void {
  track_cb_ = std::move(track);
  untrack_cb_ = std::move(untrack);
}

auto IoContext::track_completion(CompletionData* data) noexcept -> void {
  if (track_cb_) {
    track_cb_(data);
  }
}

auto IoContext::untrack_completion(CompletionData* data) noexcept -> void {
  if (untrack_cb_) {
    untrack_cb_(data);
  }
}

auto IoContext::cleanup_completion_data(CompletionData* data) noexcept -> void {
  if (!data) return;
  
  untrack_completion(data);
  deallocate_completion(data);
}

auto IoContext::process_completions(CompletionCallback cb) -> unsigned {
  return impl_ ? impl_->process_completions_impl(std::move(cb)) : 0;
}

// ==========================================================================
// Awaitable submission methods
// ==========================================================================

auto IoContext::async_read(int fd, MutableBuffer buffer, std::uint64_t offset)
    -> IoAwaitable<ops::Read> {
  return IoAwaitable<ops::Read>{*this, {fd, buffer, offset}};
}

auto IoContext::async_write(int fd, ConstBuffer buffer, std::uint64_t offset)
    -> IoAwaitable<ops::Write> {
  return IoAwaitable<ops::Write>{*this, {fd, buffer, offset}};
}

auto IoContext::async_accept(int listen_fd) -> IoAwaitable<ops::Accept> {
  return IoAwaitable<ops::Accept>{*this, {listen_fd}};
}

auto IoContext::async_close(int fd) -> IoAwaitable<ops::Close> {
  return IoAwaitable<ops::Close>{*this, {fd}};
}

auto IoContext::async_timeout(std::chrono::nanoseconds duration)
    -> TimeoutAwaitable {
  return TimeoutAwaitable{*this, duration};
}

auto IoContext::async_poll(int fd, std::uint32_t events)
    -> IoAwaitable<ops::Poll> {
  return IoAwaitable<ops::Poll>{*this, {fd, events}};
}

auto IoContext::async_poll_timeout(int fd, std::uint32_t events,
                                   std::chrono::milliseconds timeout)
    -> PollTimeoutAwaitable {
  return PollTimeoutAwaitable{*this, fd, events, timeout};
}

auto IoContext::submit_read(CompletionData* data, int fd, MutableBuffer buffer,
                            std::uint64_t offset) -> void {
  if (impl_) impl_->submit_read(data, fd, buffer, offset);
}

auto IoContext::submit_write(CompletionData* data, int fd, ConstBuffer buffer,
                             std::uint64_t offset) -> void {
  if (impl_) impl_->submit_write(data, fd, buffer, offset);
}

auto IoContext::submit_accept(CompletionData* data, int fd) -> void {
  if (impl_) impl_->submit_accept(data, fd);
}

auto IoContext::submit_close(CompletionData* data, int fd) -> void {
  if (impl_) impl_->submit_close(data, fd);
}

auto IoContext::submit_timeout(CompletionData* data,
                               std::chrono::nanoseconds duration) -> void {
  if (impl_) impl_->submit_timeout(data, duration);
}

auto IoContext::submit_poll(CompletionData* data, int fd, std::uint32_t events)
    -> void {
  if (impl_) impl_->submit_poll(data, fd, events);
}

auto IoContext::submit_poll_timeout(CompletionData* data, int fd,
                                    std::uint32_t events,
                                    std::chrono::milliseconds timeout)
    -> void {
  if (!impl_) return;

  // Reuse the "prepare" path because it already implements poll + link-timeout
  // correctly and accounts for SQE availability.
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout);
  auto nsecs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(timeout - secs);

  data->ts.tv_sec = secs.count();
  data->ts.tv_nsec = nsecs.count();

  IoRequest req{
      .op = IoOpType::PollTimeout,
      .data = data,
      .fd = fd,
      .poll_mask = events,
      .ts_ptr = &data->ts,
      .has_link_timeout = true,
  };

  (void)impl_->prepare(req);
}

auto IoContext::submit_cancel(void* operation_data) -> void {
  if (impl_) impl_->submit_cancel(operation_data);
}

// ============================================================================
// IoAwaitable Implementation
// ============================================================================

template <typename Operation>
auto IoAwaitable<Operation>::await_suspend(std::coroutine_handle<> h) noexcept
    -> void {
  data_ = context_->allocate_completion();
  data_->continuation = h;
  data_->context = context_;
  data_->completed = false;
  context_->track_completion(data_);

  if constexpr (std::is_same_v<Operation, ops::Read>) {
    context_->submit_read(data_, operation_.fd, operation_.buffer,
                          operation_.offset);
  } else if constexpr (std::is_same_v<Operation, ops::Write>) {
    context_->submit_write(data_, operation_.fd, operation_.buffer,
                           operation_.offset);
  } else if constexpr (std::is_same_v<Operation, ops::Accept>) {
    context_->submit_accept(data_, operation_.listen_fd);
  } else if constexpr (std::is_same_v<Operation, ops::Close>) {
    context_->submit_close(data_, operation_.fd);
  } else if constexpr (std::is_same_v<Operation, ops::Poll>) {
    context_->submit_poll(data_, operation_.fd, operation_.events);
  }
}

template <typename Operation>
auto IoAwaitable<Operation>::await_resume() noexcept -> IoResult {
  if (data_ == nullptr) {
    return io_failure(IoError::Cancelled);
  }

  IoResult out;
  if (!data_->completed) {
    out = io_failure(IoError::Cancelled);
  } else if (data_->result < 0) {
    out = io_failure(from_errno(-data_->result));
  } else {
    out = io_success(static_cast<std::size_t>(data_->result));
  }

  context_->cleanup_completion_data(data_);
  data_ = nullptr;
  return out;
}

template <typename Operation>
IoAwaitable<Operation>::~IoAwaitable() {
  // If data_ is still set, the coroutine was destroyed before await_resume
  // Mark it as cancelled so io_uring completion handler knows to clean up
  if (data_ != nullptr) {
    data_->cancelled = true;
    data_->continuation = {};  // Clear handle - coroutine is already destroyed
    // Don't delete here - io_uring may still reference it
    // The completion handler will clean up when CQE arrives
  }
}

// Explicit instantiations
template class IoAwaitable<ops::Read>;
template class IoAwaitable<ops::Write>;
template class IoAwaitable<ops::Accept>;
template class IoAwaitable<ops::Close>;
template class IoAwaitable<ops::Poll>;

// ============================================================================
// TimeoutAwaitable Implementation
// ============================================================================

TimeoutAwaitable::TimeoutAwaitable(IoContext& ctx,
                                   std::chrono::nanoseconds duration) noexcept
    : context_(&ctx), duration_(duration) {}

TimeoutAwaitable::TimeoutAwaitable(TimeoutAwaitable&& other) noexcept
    : context_(other.context_),
      duration_(other.duration_),
      data_(other.data_) {
  other.context_ = nullptr;
  other.data_ = nullptr;
}

TimeoutAwaitable::~TimeoutAwaitable() {
  if (data_ != nullptr) {
    data_->cancelled = true;
    data_->continuation = {};
  }
}

auto TimeoutAwaitable::await_suspend(std::coroutine_handle<> h) noexcept
    -> void {
  data_ = context_->allocate_completion();
  data_->continuation = h;
  data_->context = context_;
  data_->completed = false;
  context_->track_completion(data_);
  context_->submit_timeout(data_, duration_);
}

auto TimeoutAwaitable::await_resume() noexcept -> void {
  if (data_ == nullptr) return;
  context_->cleanup_completion_data(data_);
  data_ = nullptr;
}

// ============================================================================
// PollTimeoutAwaitable Implementation
// ============================================================================

auto PollTimeoutAwaitable::await_suspend(std::coroutine_handle<> h) noexcept
    -> void {
  data_ = context_->allocate_completion();
  data_->continuation = h;
  data_->context = context_;
  data_->completed = false;
  context_->track_completion(data_);

  context_->submit_poll_timeout(data_, fd_, events_, timeout_);
}

PollTimeoutAwaitable::~PollTimeoutAwaitable() {
  if (data_ != nullptr) {
    data_->cancelled = true;
    data_->continuation = {};
  }
}

auto PollTimeoutAwaitable::await_resume() noexcept -> PollResult {
  PollResult out;
  if (data_ == nullptr) {
    out.error = make_error_code(IoError::Cancelled);
    return out;
  }

  if (!data_->completed) {
    out.error = make_error_code(IoError::Cancelled);
  } else if (data_->result > 0) {
    out.ready = true;
  } else if (data_->result == -ECANCELED) {
    out.timed_out = true;
  } else if (data_->result < 0) {
    out.error = from_errno(-data_->result);
  }

  context_->cleanup_completion_data(data_);
  data_ = nullptr;
  return out;
}

}  // namespace taskmaster::io
