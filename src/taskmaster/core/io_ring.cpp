#include "taskmaster/core/io_ring.hpp"

#include <poll.h>

namespace taskmaster {

IoRing::IoRing() {
  if (io_uring_queue_init(RING_SIZE, &ring_, 0) == 0) {
    initialized_ = true;
  }
}

IoRing::~IoRing() {
  if (initialized_) {
    io_uring_queue_exit(&ring_);
  }
}

auto IoRing::prepare(const IoRequest& req) -> bool {
  if (!initialized_)
    return false;

  auto* sqe = io_uring_get_sqe(&ring_);
  if (!sqe) {
    submit(true);
    sqe = io_uring_get_sqe(&ring_);
    if (!sqe)
      return false;
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
    case IoOpType::PollTimeout: {
      io_uring_prep_poll_add(sqe, req.fd, req.poll_mask);
      sqe->flags |= IOSQE_IO_LINK;
      io_uring_sqe_set_data(sqe, req.data);
      ++pending_count_;

      auto* timeout_sqe = io_uring_get_sqe(&ring_);
      if (timeout_sqe) {
        io_uring_prep_link_timeout(timeout_sqe, req.ts_ptr, 0);
        io_uring_sqe_set_data(timeout_sqe, nullptr);
      } else {
        sqe->flags = 0;
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        return false;
      }
      return true;
    }
    case IoOpType::Timeout:
      io_uring_prep_timeout(sqe, req.ts_ptr, 0, 0);
      break;
    case IoOpType::Close:
      io_uring_prep_close(sqe, req.fd);
      break;
    case IoOpType::Cancel:
      io_uring_prep_cancel64(sqe, req.cancel_user_data, 0);
      break;
    case IoOpType::Nop:
      io_uring_prep_nop(sqe);
      break;
  }

  io_uring_sqe_set_data(sqe, req.data);
  ++pending_count_;
  return true;
}

auto IoRing::submit(bool force) -> int {
  if (pending_count_ == 0)
    return 0;
  if (!force && pending_count_ < BATCH_SIZE)
    return 0;

  pending_count_ = 0;
  return io_uring_submit(&ring_);
}

auto IoRing::wait(std::chrono::milliseconds timeout) -> void {
  if (!initialized_)
    return;

  __kernel_timespec ts{.tv_sec = timeout.count() / 1000,
                       .tv_nsec = (timeout.count() % 1000) * 1000000};
  io_uring_cqe* cqe = nullptr;
  (void)io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);
}

auto IoRing::setup_wake_poll(int fd) -> void {
  if (!initialized_ || fd < 0)
    return;

  auto* sqe = io_uring_get_sqe(&ring_);
  if (!sqe)
    return;

  io_uring_prep_poll_multishot(sqe, fd, POLLIN);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(WAKE_EVENT_TOKEN));
  ++pending_count_;
  submit(true);
}

}  // namespace taskmaster
