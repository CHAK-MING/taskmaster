#include "taskmaster/core/shard.hpp"

#include "taskmaster/util/log.hpp"

#include <sys/eventfd.h>

#include <unistd.h>

namespace taskmaster {

Shard::Shard(shard_id id) : id_(id) {
  wake_fd_ = eventfd(0, EFD_NONBLOCK);
  if (wake_fd_ < 0) {
    log::error("Failed to create eventfd for shard {}", id);
  }
}

Shard::~Shard() {
  if (wake_fd_ >= 0) {
    close(wake_fd_);
  }
}

auto Shard::schedule_local(std::coroutine_handle<> h) -> void {
  if (h && !h.done()) {
    local_queue_.push_back(h);
  }
}

auto Shard::schedule_next(std::coroutine_handle<> h) -> void {
  if (!h || h.done())
    return;

  if (!run_next_.has_value()) {
    run_next_ = h;
  } else {
    local_queue_.push_back(h);
  }
}

auto Shard::schedule_remote(std::coroutine_handle<> h) -> bool {
  return remote_queue_.push(h);
}

auto Shard::submit_io(IoRequest req) -> void {
  if (req.data) {
    pending_io_.insert(req.data);
  }
  io_queue_.push_back(std::move(req));
}

auto Shard::process_ready() -> bool {
  bool did_work = false;

  if (run_next_.has_value()) {
    auto handle = run_next_.value();
    run_next_.reset();
    if (handle) {
      if (!handle.done()) {
        handle.resume();
        did_work = true;
      }
      if (handle.done()) {
        handle.destroy();
      }
    }
  }

  std::deque<std::coroutine_handle<>> batch;
  batch.swap(local_queue_);

  for (auto& handle : batch) {
    if (handle) {
      if (!handle.done()) {
        handle.resume();
        did_work = true;
      }
      if (handle.done()) {
        handle.destroy();
      }
    }
  }

  while (auto h = remote_queue_.try_pop()) {
    if (*h) {
      if (!(*h).done()) {
        (*h).resume();
        did_work = true;
      }
      if ((*h).done()) {
        (*h).destroy();
      }
    }
  }

  return did_work;
}

auto Shard::process_io() -> void {
  while (!io_queue_.empty()) {
    auto req = std::move(io_queue_.front());
    io_queue_.pop_front();

    if (!ring_.prepare(req)) {
      io_queue_.push_front(std::move(req));
      return;
    }
  }
}

auto Shard::has_work() const noexcept -> bool {
  return run_next_.has_value() || !local_queue_.empty() ||
         !remote_queue_.empty() || !io_queue_.empty();
}

auto Shard::is_sleeping() const noexcept -> bool {
  return sleeping_.load(std::memory_order_acquire);
}

auto Shard::set_sleeping(bool v) noexcept -> void {
  sleeping_.store(v, std::memory_order_release);
}

auto Shard::drain_pending() -> void {
  for (auto& req : io_queue_) {
    if (req.data && req.data->coroutine) {
      auto h = std::coroutine_handle<>::from_address(req.data->coroutine);
      if (h) {
        h.destroy();
      }
      req.data->coroutine = nullptr;
    }
  }
  io_queue_.clear();

  if (run_next_.has_value()) {
    auto h = run_next_.value();
    run_next_.reset();
    if (h) {
      h.destroy();
    }
  }

  for (auto& h : local_queue_) {
    if (h) {
      h.destroy();
    }
  }
  local_queue_.clear();

  while (auto h = remote_queue_.try_pop()) {
    if (*h) {
      (*h).destroy();
    }
  }

  for (auto* data : pending_io_) {
    if (data && data->coroutine) {
      auto h = std::coroutine_handle<>::from_address(data->coroutine);
      if (h) {
        h.destroy();
      }
      data->coroutine = nullptr;
    }
  }
  pending_io_.clear();
}

auto Shard::track_io_data(io_data* data) -> void {
  if (data) {
    pending_io_.insert(data);
  }
}

auto Shard::untrack_io_data(io_data* data) -> void {
  if (data) {
    pending_io_.erase(data);
  }
}

}  // namespace taskmaster
