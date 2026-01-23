#include "taskmaster/io/context.hpp"
#include "taskmaster/io/result.hpp"
#include "taskmaster/util/log.hpp"

#include <liburing.h>
#include <poll.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <list>
#include <memory_resource>
#include <unordered_map>
#include <vector>

namespace taskmaster::io {

// Error Conversion

auto from_errno(int err) noexcept -> std::error_code {
  switch (err) {
  case 0:
    return {};
  case ECANCELED:
    return make_error_code(IoError::Cancelled);
  case ETIMEDOUT:
    return make_error_code(IoError::TimedOut);
  case ECONNRESET:
    return make_error_code(IoError::ConnectionReset);
  case ECONNREFUSED:
    return make_error_code(IoError::ConnectionRefused);
  case EPIPE:
    return make_error_code(IoError::BrokenPipe);
  case EAGAIN:
    return make_error_code(IoError::WouldBlock);
  // Note: EWOULDBLOCK == EAGAIN on Linux, so no separate case needed
  case EINVAL:
    return make_error_code(IoError::InvalidArgument);
  case EBADF:
    return make_error_code(IoError::BadDescriptor);
  case ENOBUFS:
    return make_error_code(IoError::NoBufferSpace);
  case EINPROGRESS:
    return make_error_code(IoError::OperationInProgress);
  case ENOTCONN:
    return make_error_code(IoError::NotConnected);
  case EISCONN:
    return make_error_code(IoError::AlreadyConnected);
  default:
    return {err, std::system_category()};
  }
}

class IoContextImpl {
public:
  static constexpr std::uint32_t kBatchSize = 8;
  static constexpr std::uint32_t kMaxRegisteredFiles = 64;
  static constexpr std::uint32_t kMaxRegisteredBuffers = 32;
  static constexpr std::size_t kRegisteredBufferSize = 4096;
  static constexpr std::uint32_t kBufferRingSize = 64;
  static constexpr std::size_t kBufferRingBufSize = 4096;
  static constexpr std::uint16_t kBufferGroupId = 0;

  explicit IoContextImpl(std::uint32_t queue_depth)
      : queue_depth_(queue_depth) {
    // Use normal mode (no SQPOLL) to avoid idle CPU consumption
    unsigned flags = 0;
    int ret = io_uring_queue_init(queue_depth_, &ring_, flags);

    if (ret < 0) {
      log::error("Failed to initialize io_uring: queue_depth={}, error={} ({})",
                 queue_depth, ret, strerror(-ret));
      initialized_ = false;
      return;
    }
    initialized_ = true;
    sqpoll_enabled_ = false;

    // Initialize fixed file table
    ret = io_uring_register_files_sparse(&ring_, kMaxRegisteredFiles);
    if (ret < 0) {
      log::debug("Fixed file registration not available ({})", strerror(-ret));
      fixed_files_enabled_ = false;
    } else {
      fixed_files_enabled_ = true;
      idx_to_fd_.resize(kMaxRegisteredFiles, -1);
      free_indices_.reserve(kMaxRegisteredFiles);
      for (int i = static_cast<int>(kMaxRegisteredFiles) - 1; i >= 0; --i) {
        free_indices_.push_back(i);
      }
    }

    // Initialize registered buffers
    init_registered_buffers();

    // Initialize buffer ring
    init_buffer_ring();

    log::debug(
        "io_uring initialized: queue_depth={}, sqpoll={}, fixed_files={}, "
        "registered_buffers={}, buffer_ring={}",
        queue_depth, sqpoll_enabled_, fixed_files_enabled_,
        registered_buffers_enabled_, buffer_ring_enabled_);
  }

  ~IoContextImpl() {
    if (initialized_) {
      if (buffer_ring_enabled_) {
        io_uring_free_buf_ring(&ring_, buf_ring_, kBufferRingSize,
                               kBufferGroupId);
        std::free(buf_ring_mem_);
      }
      if (registered_buffers_enabled_) {
        io_uring_unregister_buffers(&ring_);
        std::free(buffer_pool_);
      }
      io_uring_queue_exit(&ring_);
    }
  }

  IoContextImpl(const IoContextImpl &) = delete;
  IoContextImpl &operator=(const IoContextImpl &) = delete;

  [[nodiscard]] auto valid() const noexcept -> bool { return initialized_; }
  [[nodiscard]] auto stopped() const noexcept -> bool { return stopped_; }
  [[nodiscard]] auto ring_fd() const noexcept -> int { return ring_.ring_fd; }
  [[nodiscard]] auto has_registered_buffers() const noexcept -> bool {
    return registered_buffers_enabled_;
  }

  auto stop() noexcept -> void { stopped_ = true; }
  auto restart() noexcept -> void { stopped_ = false; }

  [[nodiscard]] auto allocate_buffer() noexcept -> MutableBuffer {
    if (!registered_buffers_enabled_ || free_buffer_indices_.empty()) {
      return {nullptr, 0};
    }
    int idx = free_buffer_indices_.back();
    free_buffer_indices_.pop_back();
    return {buffer_pool_ +
                static_cast<std::size_t>(idx) * kRegisteredBufferSize,
            kRegisteredBufferSize};
  }

  auto deallocate_buffer(void *ptr) noexcept -> void {
    if (!registered_buffers_enabled_ || !ptr)
      return;
    int idx = get_buffer_index(ptr);
    if (idx >= 0) {
      free_buffer_indices_.push_back(idx);
    }
  }

  // Direct preparation methods (for shard integration)

  auto prepare(const IoRequest &req) -> bool {
    auto *sqe = get_sqe();
    if (!sqe) {
      // Ring is full, submit and retry
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe)
        return false;
    }

    switch (req.op) {
    case IoOpType::Read: {
      auto [fd_or_idx, is_fixed_file] = get_or_register_fd(req.fd);
      int buf_idx = get_buffer_index(req.buf);
      if (buf_idx >= 0) {
        io_uring_prep_read_fixed(sqe, fd_or_idx, req.buf, req.len, req.offset,
                                 buf_idx);
      } else {
        io_uring_prep_read(sqe, fd_or_idx, req.buf, req.len, req.offset);
      }
      if (is_fixed_file) {
        sqe->flags |= IOSQE_FIXED_FILE;
      }
      break;
    }

    case IoOpType::Write: {
      auto [fd_or_idx, is_fixed_file] = get_or_register_fd(req.fd);
      int buf_idx = get_buffer_index(req.buf);
      if (buf_idx >= 0) {
        io_uring_prep_write_fixed(sqe, fd_or_idx, req.buf, req.len, req.offset,
                                  buf_idx);
      } else {
        io_uring_prep_write(sqe, fd_or_idx, req.buf, req.len, req.offset);
      }
      if (is_fixed_file) {
        sqe->flags |= IOSQE_FIXED_FILE;
      }
      break;
    }

    case IoOpType::Connect:
      io_uring_prep_connect(sqe, req.fd, static_cast<const sockaddr *>(req.buf),
                            req.len);
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

        auto *timeout_sqe = get_sqe();
        if (timeout_sqe) {
          io_uring_prep_link_timeout(
              timeout_sqe, reinterpret_cast<__kernel_timespec *>(req.ts_ptr),
              0);
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
      io_uring_prep_timeout(
          sqe, reinterpret_cast<__kernel_timespec *>(req.ts_ptr), 0, 0);
      break;

    case IoOpType::Close:
      io_uring_prep_close(sqe, req.fd);
      break;

    case IoOpType::Cancel:
      io_uring_prep_cancel(sqe, reinterpret_cast<void *>(req.cancel_user_data),
                           0);
      break;

    case IoOpType::Nop:
      io_uring_prep_nop(sqe);
      break;

    case IoOpType::MsgRing:
      io_uring_prep_msg_ring(sqe, req.fd, 0, req.msg_ring_data, 0);
      sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
      io_uring_sqe_set_data(sqe, nullptr);
      return true;
    }

    io_uring_sqe_set_data(sqe, req.data);
    ++pending_count_;
    return true;
  }

  auto submit(bool force) -> int {
    if (!initialized_)
      return 0;
    if (!force && pending_count_ < kBatchSize)
      return 0;
    return io_uring_submit(&ring_);
  }

  auto wait(std::chrono::milliseconds timeout) -> void {
    if (!initialized_)
      return;

    io_uring_cqe *cqe = nullptr;
    int ret;

    if (timeout.count() < 0) {
      // Negative timeout: wait indefinitely until event or wakeup
      ret = io_uring_wait_cqe(&ring_, &cqe);
    } else {
      // Specific timeout
      __kernel_timespec ts{};
      ts.tv_sec = timeout.count() / 1000;
      ts.tv_nsec = (timeout.count() % 1000) * 1000000;
      ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
    }

    // -ETIME (timeout) is normal, but log other errors
    if (ret < 0 && ret != -ETIME) {
      log::debug("io_uring_wait_cqe failed: {} ({})", ret, strerror(-ret));
    }
  }

  auto setup_wake_poll(int fd) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_poll_multishot(sqe, fd, POLLIN);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void *>(kWakeEventToken));
    ++pending_count_;
  }

  template <std::invocable<void *, int, unsigned> Callback>
  auto process_completions_impl(Callback &&cb) -> unsigned {
    if (!initialized_)
      return 0;

    io_uring_cqe *cqe = nullptr;
    unsigned head, count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      cb(io_uring_cqe_get_data(cqe), cqe->res, cqe->flags);
      ++count;
    }
    io_uring_cq_advance(&ring_, count);
    return count;
  }

  // Awaitable submission methods

  auto submit_read(CompletionData *data, int fd, MutableBuffer buffer,
                   std::uint64_t offset) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    auto [fd_or_idx, is_fixed_file] = get_or_register_fd(fd);
    int buf_idx = get_buffer_index(buffer.data());

    if (buf_idx >= 0) {
      io_uring_prep_read_fixed(sqe, fd_or_idx, buffer.data(),
                               static_cast<unsigned>(buffer.size()), offset,
                               buf_idx);
    } else {
      io_uring_prep_read(sqe, fd_or_idx, buffer.data(),
                         static_cast<unsigned>(buffer.size()), offset);
    }
    if (is_fixed_file) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_write(CompletionData *data, int fd, ConstBuffer buffer,
                    std::uint64_t offset) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    auto [fd_or_idx, is_fixed_file] = get_or_register_fd(fd);
    int buf_idx = get_buffer_index(buffer.data());

    if (buf_idx >= 0) {
      io_uring_prep_write_fixed(sqe, fd_or_idx, buffer.data(),
                                static_cast<unsigned>(buffer.size()), offset,
                                buf_idx);
    } else {
      io_uring_prep_write(sqe, fd_or_idx, buffer.data(),
                          static_cast<unsigned>(buffer.size()), offset);
    }
    if (is_fixed_file) {
      sqe->flags |= IOSQE_FIXED_FILE;
    }
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_accept(CompletionData *data, int fd) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_accept(sqe, fd, nullptr, nullptr, 0);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_connect(CompletionData *data, int fd, const void *addr,
                      std::uint32_t addrlen) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_connect(sqe, fd, static_cast<const sockaddr *>(addr),
                          addrlen);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_close(CompletionData *data, int fd) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_close(sqe, fd);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_timeout(CompletionData *data, std::chrono::nanoseconds duration)
      -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto nsecs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);

    // Store timespec in CompletionData to ensure lifetime
    data->ts.tv_sec = secs.count();
    data->ts.tv_nsec = nsecs.count();

    io_uring_prep_timeout(sqe, reinterpret_cast<__kernel_timespec *>(&data->ts),
                          0, 0);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_poll(CompletionData *data, int fd, std::uint32_t events) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_poll_add(sqe, fd, events);
    io_uring_sqe_set_data(sqe, data);
    ++pending_count_;
  }

  auto submit_cancel(void *operation_data) -> void {
    auto *sqe = get_sqe();
    if (!sqe)
      return;

    io_uring_prep_cancel(sqe, operation_data, 0);
    io_uring_sqe_set_data(sqe, nullptr); // No completion needed for cancel
    ++pending_count_;
  }

  // Event loop methods

  auto run() -> void {
    while (!stopped_ && pending_count_ > 0) {
      submit_pending();
      wait_and_process();
    }
  }

  auto run_one() -> std::size_t {
    if (stopped_ || pending_count_ == 0)
      return 0;

    submit_pending();
    return wait_and_process();
  }

  auto poll() -> std::size_t {
    submit_pending();
    return process_completions_internal();
  }

private:
  auto get_sqe() -> io_uring_sqe * {
    auto *sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      // Ring is full, submit and retry
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
    }
    return sqe;
  }

  auto submit_pending() -> void { io_uring_submit(&ring_); }

  auto wait_and_process() -> std::size_t {
    io_uring_cqe *cqe = nullptr;
    int ret = io_uring_wait_cqe(&ring_, &cqe);
    if (ret < 0)
      return 0;

    return process_completions_internal();
  }

  auto process_completions_internal() -> std::size_t {
    io_uring_cqe *cqe = nullptr;
    unsigned head;
    std::size_t count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      auto *data = static_cast<CompletionData *>(io_uring_cqe_get_data(cqe));
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

  // Fixed File Registration helpers

  [[nodiscard]] static auto is_socket_fd(int fd) noexcept -> bool {
    int type = 0;
    socklen_t len = sizeof(type);
    return ::getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0;
  }

  struct LruEntry {
    int fd;
    int idx;
  };

  [[nodiscard]] auto get_or_register_fd(int fd) -> std::pair<int, bool> {
    if (!fixed_files_enabled_ || fd < 0) {
      return {fd, false};
    }

    // IMPORTANT: Do not use fixed-file registration for sockets.
    // Sockets are frequently short-lived (accept/close) and file descriptor
    // numbers are quickly reused. io_uring fixed-file slots reference the
    // underlying file, so reusing an fd number after close can lead to reads
    // being issued against a stale slot and connections getting reset.
    if (is_socket_fd(fd)) {
        log::debug("Socket FD {} detected, skipping fixed-file registration", fd);
      unregister_fd(fd); // Clean up any stale registration for this fd number.
      return {fd, false};
    }

    // Check if already registered
    if (auto it = fd_to_lru_iter_.find(fd); it != fd_to_lru_iter_.end()) {
      // Move to front (most recently used)
      lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
      return {it->second->idx, true};
    }

    // Need to register - get a free index or evict
    int idx = -1;
    if (!free_indices_.empty()) {
      idx = free_indices_.back();
      free_indices_.pop_back();
    } else {
      // Evict LRU entry
      idx = evict_lru_fd();
      if (idx < 0) {
        return {fd, false}; // Failed to evict
      }
    }

    // Register the fd at idx
      log::debug("Registered FD {} as fixed-file at index {}", fd, idx);
    if (io_uring_register_files_update(&ring_, static_cast<unsigned>(idx), &fd,
                                       1) < 0) {
      free_indices_.push_back(idx);
      return {fd, false};
    }

    // Add to LRU list at front
    lru_list_.push_front({fd, idx});
    fd_to_lru_iter_[fd] = lru_list_.begin();
    idx_to_fd_[static_cast<std::size_t>(idx)] = fd;

    return {idx, true};
  }

  auto evict_lru_fd() -> int {
    if (lru_list_.empty()) {
      return -1;
    }

    // Remove from back (least recently used)
    auto &entry = lru_list_.back();
    int idx = entry.idx;
    int old_fd = entry.fd;

    // Unregister by setting to -1
    int neg_one = -1;
    io_uring_register_files_update(&ring_, static_cast<unsigned>(idx), &neg_one,
                                   1);

    fd_to_lru_iter_.erase(old_fd);
    idx_to_fd_[static_cast<std::size_t>(idx)] = -1;
    lru_list_.pop_back();

    return idx;
  }

  auto unregister_fd(int fd) -> void {
    if (!fixed_files_enabled_)
      return;

    auto it = fd_to_lru_iter_.find(fd);
    if (it == fd_to_lru_iter_.end())
      return;

    int idx = it->second->idx;
    int neg_one = -1;
    io_uring_register_files_update(&ring_, static_cast<unsigned>(idx), &neg_one,
                                   1);

    idx_to_fd_[static_cast<std::size_t>(idx)] = -1;
    lru_list_.erase(it->second);
    fd_to_lru_iter_.erase(it);
    free_indices_.push_back(idx);
  }

  // Registered Buffers helpers

  auto init_registered_buffers() -> void {
    // Allocate aligned buffer pool
    constexpr std::size_t total_size =
        kMaxRegisteredBuffers * kRegisteredBufferSize;
    buffer_pool_ = static_cast<char *>(std::aligned_alloc(4096, total_size));
    if (!buffer_pool_) {
      log::debug("Failed to allocate registered buffer pool");
      registered_buffers_enabled_ = false;
      return;
    }

    // Setup iovecs for registration
    iovecs_.resize(kMaxRegisteredBuffers);
    for (std::uint32_t i = 0; i < kMaxRegisteredBuffers; ++i) {
      iovecs_[i].iov_base = buffer_pool_ + i * kRegisteredBufferSize;
      iovecs_[i].iov_len = kRegisteredBufferSize;
      free_buffer_indices_.push_back(
          static_cast<int>(kMaxRegisteredBuffers - 1 - i));
    }

    int ret = io_uring_register_buffers(&ring_, iovecs_.data(),
                                        kMaxRegisteredBuffers);
    if (ret < 0) {
      log::debug("Registered buffers not available ({})", strerror(-ret));
      std::free(buffer_pool_);
      buffer_pool_ = nullptr;
      iovecs_.clear();
      free_buffer_indices_.clear();
      registered_buffers_enabled_ = false;
      return;
    }

    registered_buffers_enabled_ = true;
  }

  [[nodiscard]] auto get_buffer_index(const void *ptr) const -> int {
    if (!registered_buffers_enabled_ || !buffer_pool_) {
      return -1;
    }
    auto *char_ptr = static_cast<const char *>(ptr);
    if (char_ptr < buffer_pool_ ||
        char_ptr >=
            buffer_pool_ + kMaxRegisteredBuffers * kRegisteredBufferSize) {
      return -1;
    }
    auto offset = static_cast<std::size_t>(char_ptr - buffer_pool_);
    if (offset % kRegisteredBufferSize != 0) {
      return -1; // Not aligned to buffer boundary
    }
    return static_cast<int>(offset / kRegisteredBufferSize);
  }

  // Buffer Ring helpers

  auto init_buffer_ring() -> void {
    // Allocate memory for buffer ring and buffers
    constexpr std::size_t ring_size = kBufferRingSize * sizeof(io_uring_buf);
    constexpr std::size_t bufs_size = kBufferRingSize * kBufferRingBufSize;
    buf_ring_mem_ =
        static_cast<char *>(std::aligned_alloc(4096, ring_size + bufs_size));
    if (!buf_ring_mem_) {
      log::debug("Failed to allocate buffer ring memory");
      buffer_ring_enabled_ = false;
      return;
    }

    // Setup buffer ring
    int ret = 0;
    buf_ring_ = io_uring_setup_buf_ring(&ring_, kBufferRingSize, kBufferGroupId,
                                        0, &ret);
    if (!buf_ring_ || ret < 0) {
      log::debug("Buffer ring not available ({})",
                 ret < 0 ? strerror(-ret) : "setup failed");
      std::free(buf_ring_mem_);
      buf_ring_mem_ = nullptr;
      buffer_ring_enabled_ = false;
      return;
    }

    // Add buffers to the ring
    char *buf_base = buf_ring_mem_ + ring_size;
    for (std::uint32_t i = 0; i < kBufferRingSize; ++i) {
      io_uring_buf_ring_add(buf_ring_, buf_base + i * kBufferRingBufSize,
                            kBufferRingBufSize, static_cast<std::uint16_t>(i),
                            io_uring_buf_ring_mask(kBufferRingSize),
                            static_cast<int>(i));
    }
    io_uring_buf_ring_advance(buf_ring_, kBufferRingSize);

    buffer_ring_enabled_ = true;
  }

  [[nodiscard]] auto has_buffer_ring() const noexcept -> bool {
    return buffer_ring_enabled_;
  }

  [[nodiscard]] auto buffer_ring_group_id() const noexcept -> std::uint16_t {
    return kBufferGroupId;
  }

  io_uring ring_{};
  std::uint32_t queue_depth_;
  std::uint32_t pending_count_{0};
  bool initialized_{false};
  bool stopped_{false};
  bool sqpoll_enabled_{false};

  // Fixed file registration state
  bool fixed_files_enabled_{false};
  std::vector<int> idx_to_fd_;    // index -> fd
  std::vector<int> free_indices_; // available indices
  std::list<LruEntry> lru_list_;  // front = MRU, back = LRU
  std::unordered_map<int, std::list<LruEntry>::iterator>
      fd_to_lru_iter_; // fd -> LRU iterator

  // Registered buffers state
  bool registered_buffers_enabled_{false};
  char *buffer_pool_{nullptr};
  std::vector<iovec> iovecs_;
  std::vector<int> free_buffer_indices_;

  // Buffer ring state
  bool buffer_ring_enabled_{false};
  io_uring_buf_ring *buf_ring_{nullptr};
  char *buf_ring_mem_{nullptr};
};

// IoContext Implementation

IoContext::IoContext(std::uint32_t queue_depth, std::pmr::memory_resource *mr)
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
  if (impl_)
    impl_->stop();
}

auto IoContext::restart() -> void {
  if (impl_)
    impl_->restart();
}

auto IoContext::run() -> void {
  if (impl_)
    impl_->run();
}

auto IoContext::run_one() -> std::size_t {
  return impl_ ? impl_->run_one() : 0;
}

auto IoContext::poll() -> std::size_t { return impl_ ? impl_->poll() : 0; }

// Direct submission methods

auto IoContext::prepare(const IoRequest &req) -> bool {
  return impl_ ? impl_->prepare(req) : false;
}

auto IoContext::submit(bool force) -> int {
  return impl_ ? impl_->submit(force) : 0;
}

auto IoContext::wait(std::chrono::milliseconds timeout) -> void {
  if (impl_)
    impl_->wait(timeout);
}

auto IoContext::setup_wake_poll(int fd) -> void {
  if (impl_)
    impl_->setup_wake_poll(fd);
}

auto IoContext::impl() noexcept -> IoContextImpl * { return impl_.get(); }

auto IoContext::ring_fd() const noexcept -> int {
  return impl_ ? impl_->ring_fd() : -1;
}

auto IoContext::set_completion_tracker(TrackerCallback track,
                                       TrackerCallback untrack) -> void {
  track_cb_ = std::move(track);
  untrack_cb_ = std::move(untrack);
}

auto IoContext::track_completion(CompletionData *data) noexcept -> void {
  if (track_cb_) {
    track_cb_(data);
  }
}

auto IoContext::untrack_completion(CompletionData *data) noexcept -> void {
  if (untrack_cb_) {
    untrack_cb_(data);
  }
}

auto IoContext::cleanup_completion_data(CompletionData *data) noexcept -> void {
  if (!data)
    return;

  untrack_completion(data);
  deallocate_completion(data);
}

auto IoContext::process_completions(CompletionCallback cb) -> unsigned {
  return impl_ ? impl_->process_completions_impl(std::move(cb)) : 0;
}

// Awaitable submission methods

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

auto IoContext::async_connect(int fd, const void *addr, std::uint32_t addrlen)
    -> IoAwaitable<ops::Connect> {
  return IoAwaitable<ops::Connect>{*this, {fd, addr, addrlen}};
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

auto IoContext::submit_read(CompletionData *data, int fd, MutableBuffer buffer,
                            std::uint64_t offset) -> void {
  if (impl_)
    impl_->submit_read(data, fd, buffer, offset);
}

auto IoContext::submit_write(CompletionData *data, int fd, ConstBuffer buffer,
                             std::uint64_t offset) -> void {
  if (impl_)
    impl_->submit_write(data, fd, buffer, offset);
}

auto IoContext::submit_accept(CompletionData *data, int fd) -> void {
  if (impl_)
    impl_->submit_accept(data, fd);
}

auto IoContext::submit_connect(CompletionData *data, int fd, const void *addr,
                               std::uint32_t addrlen) -> void {
  if (impl_)
    impl_->submit_connect(data, fd, addr, addrlen);
}

auto IoContext::submit_close(CompletionData *data, int fd) -> void {
  if (impl_)
    impl_->submit_close(data, fd);
}

auto IoContext::submit_timeout(CompletionData *data,
                               std::chrono::nanoseconds duration) -> void {
  if (impl_)
    impl_->submit_timeout(data, duration);
}

auto IoContext::submit_poll(CompletionData *data, int fd, std::uint32_t events)
    -> void {
  if (impl_)
    impl_->submit_poll(data, fd, events);
}

auto IoContext::submit_poll_timeout(CompletionData *data, int fd,
                                    std::uint32_t events,
                                    std::chrono::milliseconds timeout) -> void {
  if (!impl_)
    return;

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

auto IoContext::submit_cancel(void *operation_data) -> void {
  if (impl_)
    impl_->submit_cancel(operation_data);
}

// IoAwaitable Implementation

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
  } else if constexpr (std::is_same_v<Operation, ops::Connect>) {
    context_->submit_connect(data_, operation_.fd, operation_.addr,
                             operation_.addrlen);
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

template <typename Operation> IoAwaitable<Operation>::~IoAwaitable() {
  // If data_ is still set, the coroutine was destroyed before await_resume
  // Mark it as cancelled so io_uring completion handler knows to clean up
  if (data_ != nullptr) {
    data_->cancelled = true;
    data_->continuation = {}; // Clear handle - coroutine is already destroyed
    // Don't delete here - io_uring may still reference it
    // The completion handler will clean up when CQE arrives
  }
}

// Explicit instantiations
template class IoAwaitable<ops::Read>;
template class IoAwaitable<ops::Write>;
template class IoAwaitable<ops::Accept>;
template class IoAwaitable<ops::Connect>;
template class IoAwaitable<ops::Close>;
template class IoAwaitable<ops::Poll>;

// TimeoutAwaitable Implementation

TimeoutAwaitable::TimeoutAwaitable(IoContext &ctx,
                                   std::chrono::nanoseconds duration) noexcept
    : context_(&ctx), duration_(duration) {}

TimeoutAwaitable::TimeoutAwaitable(TimeoutAwaitable &&other) noexcept
    : context_(other.context_), duration_(other.duration_), data_(other.data_) {
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
  if (data_ == nullptr)
    return;
  context_->cleanup_completion_data(data_);
  data_ = nullptr;
}

// PollTimeoutAwaitable Implementation

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

// Registered Buffer API

auto IoContext::allocate_registered_buffer() noexcept -> MutableBuffer {
  return impl_ ? impl_->allocate_buffer() : MutableBuffer{nullptr, 0};
}

auto IoContext::deallocate_registered_buffer(void *ptr) noexcept -> void {
  if (impl_)
    impl_->deallocate_buffer(ptr);
}

auto IoContext::registered_buffers_enabled() const noexcept -> bool {
  return impl_ ? impl_->has_registered_buffers() : false;
}

} // namespace taskmaster::io
