#include "taskmaster/runtime.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include "taskmaster/log.hpp"

namespace taskmaster {

auto Runtime::instance() -> Runtime & {
  static Runtime runtime;
  return runtime;
}

Runtime::Runtime() {
  if (io_uring_queue_init(RING_SIZE, &ring_, 0) < 0) {
    log::error("Failed to initialize io_uring for Runtime");
    return;
  }
  ring_initialized_ = true;

  wake_fd_ = eventfd(0, EFD_NONBLOCK);
  if (wake_fd_ < 0) {
    log::error("Failed to create eventfd for Runtime");
  }
}

Runtime::~Runtime() {
  stop();
  if (ring_initialized_) {
    io_uring_queue_exit(&ring_);
  }
  if (wake_fd_ >= 0) {
    close(wake_fd_);
  }
}

auto Runtime::start() -> void {
  if (running_.load(std::memory_order_acquire)) {
    return;
  }
  if (!ring_initialized_) {
    log::error("Cannot start Runtime: io_uring not initialized");
    return;
  }

  stop_requested_.store(false, std::memory_order_release);
  running_.store(true, std::memory_order_release);
  thread_ = std::thread([this] { run(); });
}

auto Runtime::stop() -> void {
  if (!running_.load(std::memory_order_acquire)) {
    return;
  }

  stop_requested_.store(true, std::memory_order_release);
  wake();

  if (thread_.joinable()) {
    thread_.join();
  }

  running_.store(false, std::memory_order_release);
}

auto Runtime::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Runtime::schedule(std::coroutine_handle<> handle) -> void {
  ready_queue_.push(handle);
  if (sleeping_.load(std::memory_order_acquire)) {
    wake();
  }
}

auto Runtime::submit_io(IoRequest req) -> bool {
  if (!io_queue_.push(std::move(req))) {
    log::warn("IO queue full, request dropped");
    return false;
  }
  if (sleeping_.load(std::memory_order_acquire)) {
    wake();
  }
  return true;
}

auto Runtime::wake() -> void {
  if (wake_fd_ >= 0) {
    std::uint64_t val = 1;
    [[maybe_unused]] auto r = write(wake_fd_, &val, sizeof(val));
  }
}

auto Runtime::run() -> void {
  while (!stop_requested_.load(std::memory_order_acquire)) {
    process_ready_queue();
    process_io_requests();

    sleeping_.store(true, std::memory_order_release);
    process_completions();
    sleeping_.store(false, std::memory_order_release);
  }

  // Final drain
  process_ready_queue();
  process_io_requests();
  process_completions();
}

auto Runtime::process_ready_queue() -> void {
  std::coroutine_handle<> handle;
  while (auto h = ready_queue_.try_pop()) {
    handle = *h;
    if (handle && !handle.done()) {
      handle.resume();
    }
  }
}

auto Runtime::process_io_requests() -> void {
  std::optional<IoRequest> req;
  while ((req = io_queue_.try_pop())) {
    io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      io_uring_submit(&ring_);
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) {
        log::error("Failed to get SQE even after submit");
        // Put back and retry later
        io_queue_.push(std::move(*req));
        return;
      }
    }

    switch (req->op) {
    case IoOpType::Read:
      io_uring_prep_read(sqe, req->fd, req->buf, req->len, req->offset);
      io_uring_sqe_set_data(sqe, req->data);
      break;

    case IoOpType::Write:
      io_uring_prep_write(sqe, req->fd, req->buf, req->len, req->offset);
      io_uring_sqe_set_data(sqe, req->data);
      break;

    case IoOpType::Poll:
      io_uring_prep_poll_add(sqe, req->fd, req->poll_mask);
      io_uring_sqe_set_data(sqe, req->data);
      break;

    case IoOpType::PollTimeout: {
      io_uring_prep_poll_add(sqe, req->fd, req->poll_mask);
      io_uring_sqe_set_data(sqe, req->data);
      sqe->flags |= IOSQE_IO_LINK;

      io_uring_sqe *timeout_sqe = io_uring_get_sqe(&ring_);
      if (!timeout_sqe) {
        io_uring_submit(&ring_);
        timeout_sqe = io_uring_get_sqe(&ring_);
      }
      if (timeout_sqe) {
        io_uring_prep_link_timeout(timeout_sqe, req->ts_ptr, 0);
        io_uring_sqe_set_data(timeout_sqe, nullptr);
      }
      break;
    }

    case IoOpType::Timeout:
      io_uring_prep_timeout(sqe, req->ts_ptr, 0, 0);
      io_uring_sqe_set_data(sqe, req->data);
      break;

    case IoOpType::Close:
      io_uring_prep_close(sqe, req->fd);
      io_uring_sqe_set_data(sqe, req->data);
      break;

    case IoOpType::Nop:
      io_uring_prep_nop(sqe);
      io_uring_sqe_set_data(sqe, nullptr);
      break;
    }
  }
}

auto Runtime::process_completions() -> void {
  io_uring_submit(&ring_);

  // Drain wake eventfd
  if (wake_fd_ >= 0) {
    std::uint64_t val;
    while (read(wake_fd_, &val, sizeof(val)) > 0) {
    }
  }

  struct __kernel_timespec ts{.tv_sec = 0, .tv_nsec = 1'000'000};

  io_uring_cqe *cqe = nullptr;
  int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);

  if (ret == -ETIME || ret < 0 || !cqe) {
    return;
  }

  unsigned head;
  unsigned count = 0;
  io_uring_for_each_cqe(&ring_, head, cqe) {
    auto *data = static_cast<io_data *>(io_uring_cqe_get_data(cqe));
    if (data) {
      data->result = cqe->res;
      data->flags = cqe->flags;

      if (data->coroutine) {
        auto handle = std::coroutine_handle<>::from_address(data->coroutine);
        if (!handle.done()) {
          handle.resume();
        }
      }
    }
    ++count;
  }
  io_uring_cq_advance(&ring_, count);
}

} // namespace taskmaster
