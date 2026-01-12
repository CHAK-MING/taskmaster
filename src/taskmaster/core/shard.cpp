#include "taskmaster/core/shard.hpp"

#include "taskmaster/util/log.hpp"

namespace taskmaster {

Shard::Shard(shard_id id) : id_(id), ctx_(io::kDefaultQueueDepth, &pool_) {
  io::AsyncEventFd::create(ctx_, 0, EFD_NONBLOCK)
      .transform([this](auto&& fd) { wake_fd_ = std::move(fd); })
      .or_else([id](auto err) -> std::expected<void, std::error_code> {
        log::error("Failed to create eventfd for shard {}: {}", id,
                   err.message());
        return {};
      });

  ctx_.set_completion_tracker(
      [this](CompletionData* data) { track_io_data(data); },
      [this](CompletionData* data) { untrack_io_data(data); });
}

Shard::~Shard() {
  drain_pending();
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

  // Swap to reusable buffer instead of allocating new deque
  if (!local_queue_.empty()) {
    batch_buffer_.swap(local_queue_);
    
    for (auto& handle : batch_buffer_) {
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
    batch_buffer_.clear();
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

    if (!ctx_.prepare(req)) {
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
  // Collect all pending CompletionData pointers (from both the local io_queue_
  // and the tracked pending_io_ set) and delete them after destroying their
  // continuations. This is important because awaitables allocate CompletionData
  // dynamically and normally free it in await_resume(); during shutdown, many
  // awaiters will never resume, so we must clean them up here.
  std::unordered_set<CompletionData*> to_delete;
  to_delete.reserve(io_queue_.size() + pending_io_.size());

  for (auto& req : io_queue_) {
    if (req.data) {
      to_delete.insert(req.data);
    }
  }
  io_queue_.clear();

  for (auto* data : pending_io_) {
    if (data) {
      to_delete.insert(data);
    }
  }
  pending_io_.clear();

  for (auto* data : to_delete) {
    if (!data) continue;
    if (data->continuation) {
      auto h = data->continuation;
      if (h) {
        h.destroy();
      }
      data->continuation = {};
    }
    if (data->context) {
      data->context->cleanup_completion_data(data);
    } else {
      delete data;
    }
  }

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
}

auto Shard::track_io_data(CompletionData* data) -> void {
  if (data) {
    pending_io_.insert(data);
  }
}

auto Shard::untrack_io_data(CompletionData* data) -> void {
  if (data) {
    pending_io_.erase(data);
  }
}

}  // namespace taskmaster
