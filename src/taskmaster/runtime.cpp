#include "taskmaster/runtime.hpp"
#include "taskmaster/log.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

namespace taskmaster {

Runtime::Runtime(unsigned num_shards) {
  if (num_shards == 0) {
    num_shards = std::thread::hardware_concurrency();
    if (num_shards == 0)
      num_shards = 1;
  }
  num_shards_ = num_shards;

  shards_.reserve(num_shards);
  for (unsigned i = 0; i < num_shards; ++i) {
    auto shard = std::make_unique<ShardLocal>();
    shard->id = i;

    if (io_uring_queue_init(RING_SIZE, &shard->ring, 0) < 0) {
      log::error("Failed to initialize io_uring for shard {}", i);
    } else {
      shard->ring_initialized = true;
    }

    shard->wake_fd = eventfd(0, EFD_NONBLOCK);
    if (shard->wake_fd < 0) {
      log::error("Failed to create eventfd for shard {}", i);
    }

    shards_.push_back(std::move(shard));
  }

  smp_queues_.resize(num_shards);
  for (unsigned from = 0; from < num_shards; ++from) {
    smp_queues_[from].reserve(num_shards);
    for (unsigned to = 0; to < num_shards; ++to) {
      smp_queues_[from].push_back(std::make_unique<SPSCQueue<SmpMessage>>(128));
    }
  }
}

Runtime::~Runtime() {
  stop();
  for (auto &shard : shards_) {
    if (shard->ring_initialized) {
      io_uring_queue_exit(&shard->ring);
    }
    if (shard->wake_fd >= 0) {
      close(shard->wake_fd);
    }
  }
}

auto Runtime::start() -> void {
  if (running_.exchange(true))
    return;
  stop_requested_.store(false);

  threads_.reserve(num_shards_);
  for (unsigned i = 0; i < num_shards_; ++i) {
    threads_.emplace_back([this, i] { run_shard(i); });
  }

  log::info("Runtime started with {} shards", num_shards_);
}

auto Runtime::stop() -> void {
  if (!running_.exchange(false))
    return;
  stop_requested_.store(true);

  for (unsigned i = 0; i < num_shards_; ++i) {
    wake_shard(i);
  }

  for (auto &t : threads_) {
    if (t.joinable())
      t.join();
  }
  threads_.clear();
}

auto Runtime::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Runtime::run_shard(shard_id id) -> void {
  current_shard_id_ = id;
  current_runtime_ = this;
  auto &shard = *shards_[id];

  setup_multishot_poll(shard);

  while (!stop_requested_.load(std::memory_order_acquire)) {
    bool has_work = false;

    has_work |= process_ready_queue(shard);
    has_work |= process_smp_messages(id);
    process_io_requests(shard);
    has_work |= flush_submissions(shard, false);
    has_work |= process_completions(shard);

    if (!has_work) {
      shard.sleeping.store(true, std::memory_order_release);

      if (shard.run_next.has_value() || !shard.local_queue.empty() ||
          !shard.remote_queue.empty() || !shard.io_queue.empty()) {
        shard.sleeping.store(false, std::memory_order_release);
        continue;
      }

      wait_for_work(shard);
      shard.sleeping.store(false, std::memory_order_release);
      process_completions(shard);
    }
  }

  process_ready_queue(shard);
  process_smp_messages(id);

  current_shard_id_ = INVALID_SHARD;
  current_runtime_ = nullptr;
}

auto Runtime::process_ready_queue(ShardLocal &shard) -> bool {
  bool did_work = false;

  if (shard.run_next.has_value()) {
    auto handle = shard.run_next.value();
    shard.run_next.reset();
    if (handle && !handle.done()) {
      handle.resume();
      did_work = true;
    }
  }

  std::deque<std::coroutine_handle<>> batch;
  batch.swap(shard.local_queue);

  for (auto &handle : batch) {
    if (handle && !handle.done()) {
      handle.resume();
      did_work = true;
    }
  }

  while (auto h = shard.remote_queue.try_pop()) {
    if (*h && !(*h).done()) {
      (*h).resume();
      did_work = true;
    }
  }

  return did_work;
}

auto Runtime::process_smp_messages(shard_id id) -> bool {
  bool did_work = false;

  for (unsigned from = 0; from < num_shards_; ++from) {
    if (from == id)
      continue;

    while (auto msg = smp_queues_[from][id]->try_pop()) {
      msg->execute();
      did_work = true;
    }
  }

  return did_work;
}

auto Runtime::process_io_requests(ShardLocal &shard) -> void {
  while (!shard.io_queue.empty()) {
    auto req = std::move(shard.io_queue.front());
    shard.io_queue.pop_front();

    auto *sqe = io_uring_get_sqe(&shard.ring);
    if (!sqe) {
      flush_submissions(shard);
      process_completions(shard);
      sqe = io_uring_get_sqe(&shard.ring);
      if (!sqe) {
        log::error("Failed to get SQE on shard {}", shard.id);
        shard.io_queue.push_front(std::move(req));
        return;
      }
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
      shard.pending_sqe_count++;

      auto *timeout_sqe = io_uring_get_sqe(&shard.ring);
      if (timeout_sqe) {
        io_uring_prep_link_timeout(timeout_sqe, req.ts_ptr, 0);
        io_uring_sqe_set_data(timeout_sqe, nullptr);
      } else {
        sqe->flags = 0;
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, nullptr);
        shard.io_queue.push_front(std::move(req));
        return;
      }
      continue;
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
    shard.pending_sqe_count++;
  }
}

auto Runtime::flush_submissions(ShardLocal &shard, bool force) -> bool {
  if (shard.pending_sqe_count == 0) {
    return false;
  }
  if (!force && shard.pending_sqe_count < ShardLocal::SUBMIT_BATCH_SIZE) {
    return false;
  }
  shard.pending_sqe_count = 0;
  return io_uring_submit(&shard.ring) > 0;
}

auto Runtime::process_completions(ShardLocal &shard) -> bool {
  if (!shard.ring_initialized)
    return false;

  io_uring_cqe *cqe = nullptr;
  unsigned head, count = 0;

  io_uring_for_each_cqe(&shard.ring, head, cqe) {
    void *raw_data = io_uring_cqe_get_data(cqe);

    if (raw_data == reinterpret_cast<void *>(WAKE_EVENT_TOKEN)) {
      std::uint64_t val;
      while (read(shard.wake_fd, &val, sizeof(val)) > 0) {
      }
      if (!(cqe->flags & IORING_CQE_F_MORE)) {
        setup_multishot_poll(shard);
      }
      ++count;
      continue;
    }

    if (raw_data != nullptr) {
      auto *data = static_cast<io_data *>(raw_data);
      data->result = cqe->res;
      data->flags = cqe->flags;

      if (data->coroutine != nullptr) {
        auto handle = std::coroutine_handle<>::from_address(data->coroutine);

        if (data->owner_shard == shard.id ||
            data->owner_shard == INVALID_SHARD) {
          if (!handle.done())
            handle.resume();
        } else {
          submit_to(data->owner_shard, handle);
        }
      }
    }
    ++count;
  }
  io_uring_cq_advance(&shard.ring, count);

  return count > 0;
}

auto Runtime::wait_for_work(ShardLocal &shard) -> void {
  if (!shard.ring_initialized)
    return;

  if (shard.run_next.has_value() || !shard.local_queue.empty() ||
      !shard.remote_queue.empty() || !shard.io_queue.empty()) {
    return;
  }

  flush_submissions(shard, true);

  io_uring_cqe *cqe = nullptr;
  (void)io_uring_wait_cqe(&shard.ring, &cqe);
}

auto Runtime::schedule(std::coroutine_handle<> handle) -> void {
  if (!handle || handle.done())
    return;

  if (current_shard_id_ != INVALID_SHARD) {
    auto &shard = *shards_[current_shard_id_];
    if (!shard.run_next.has_value()) {
      shard.run_next = handle;
    } else {
      shard.local_queue.push_back(handle);
    }
  } else {
    auto target = static_cast<shard_id>(std::hash<void *>{}(handle.address()) %
                                        num_shards_);
    while (!shards_[target]->remote_queue.push(handle)) {
      std::this_thread::yield();
    }
    wake_shard(target);
  }
}

auto Runtime::schedule_on(shard_id target, std::coroutine_handle<> handle)
    -> void {
  if (!handle || handle.done())
    return;

  if (target == current_shard_id_) {
    shards_[target]->local_queue.push_back(handle);
  } else {
    while (!shards_[target]->remote_queue.push(handle)) {
      std::this_thread::yield();
    }
    wake_shard(target);
  }
}

auto Runtime::schedule_external(std::coroutine_handle<> handle) -> void {
  auto target = static_cast<shard_id>(std::hash<void *>{}(handle.address()) %
                                      num_shards_);
  schedule_on(target, handle);
}

auto Runtime::submit_io(IoRequest req) -> bool {
  if (current_shard_id_ == INVALID_SHARD) {
    log::error("Cannot submit IO: not in a shard context");
    return false;
  }

  auto &shard = *shards_[current_shard_id_];
  if (!shard.ring_initialized) {
    log::error("Cannot submit IO: io_uring not initialized");
    return false;
  }

  shard.io_queue.push_back(std::move(req));
  return true;
}

auto Runtime::wake_shard(shard_id id) -> void {
  if (id >= num_shards_)
    return;

  auto &shard = *shards_[id];
  if (shard.wake_fd >= 0) {
    std::uint64_t val = 1;
    while (true) {
      auto ret = write(shard.wake_fd, &val, sizeof(val));
      if (ret == sizeof(val))
        break;
      if (ret < 0 && errno == EINTR)
        continue;
      if (ret < 0 && errno == EAGAIN)
        break; // Counter overflow, already woken
      break;
    }
  }
}

auto Runtime::setup_multishot_poll(ShardLocal &shard) -> void {
  if (shard.wake_fd < 0 || !shard.ring_initialized) {
    return;
  }

  auto *sqe = io_uring_get_sqe(&shard.ring);
  if (!sqe) {
    return;
  }

  io_uring_prep_poll_multishot(sqe, shard.wake_fd, POLLIN);
  io_uring_sqe_set_data(sqe, reinterpret_cast<void *>(WAKE_EVENT_TOKEN));
  shard.pending_sqe_count++;
  flush_submissions(shard, true);
}

} // namespace taskmaster
