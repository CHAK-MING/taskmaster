#include "taskmaster/core/runtime.hpp"

#include "taskmaster/util/log.hpp"

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
  for (auto i : std::views::iota(0u, num_shards)) {
    shards_.emplace_back(std::make_unique<Shard>(i));
  }

  smp_queues_.resize(num_shards);
  for (auto from : std::views::iota(0u, num_shards)) {
    smp_queues_[from].reserve(num_shards);
    for (auto _ : std::views::iota(0u, num_shards)) {
      smp_queues_[from].emplace_back(
          std::make_unique<SPSCQueue<SmpMessage>>(128));
    }
  }
}

Runtime::~Runtime() {
  stop();
}

auto Runtime::start() -> void {
  if (running_.exchange(true))
    return;
  stop_requested_.store(false);

  threads_.reserve(num_shards_);
  for (auto i : std::views::iota(0u, num_shards_)) {
    threads_.emplace_back([this, i] { run_shard(i); });
  }
}

auto Runtime::stop() -> void {
  if (!running_.exchange(false))
    return;
  stop_requested_.store(true);

  for (auto i : std::views::iota(0u, num_shards_)) {
    wake_shard(i);
  }

  for (auto& t : threads_) {
    if (t.joinable())
      t.join();
  }
  threads_.clear();

  for (auto from : std::views::iota(0u, num_shards_)) {
    for (auto to : std::views::iota(0u, num_shards_)) {
      while (auto msg = smp_queues_[from][to]->try_pop()) {
        if (std::holds_alternative<std::coroutine_handle<>>(msg->payload)) {
          auto h = std::get<std::coroutine_handle<>>(msg->payload);
          if (h) {
            h.destroy();
          }
        }
      }
    }
  }

  for (auto& shard : shards_) {
    shard->drain_pending();
  }
}

auto Runtime::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Runtime::current_shard() const noexcept -> shard_id {
  return detail::current_shard_id;
}

auto Runtime::run_shard(shard_id id) -> void {
  detail::current_shard_id = id;
  detail::current_runtime = this;
  current_scheduler_ptr = this;

  auto& shard = *shards_[id];
  shard.ring().setup_wake_poll(shard.wake_fd());

  while (!stop_requested_.load(std::memory_order_acquire)) {
    bool has_work = false;

    has_work |= shard.process_ready();
    has_work |= process_smp_messages(id);
    shard.process_io();
    has_work |= shard.ring().submit(false) > 0;
    has_work |= process_completions(shard);

    if (!has_work) {
      shard.set_sleeping(true);

      if (shard.has_work()) {
        shard.set_sleeping(false);
        continue;
      }

      wait_for_work(shard);
      shard.set_sleeping(false);
      process_completions(shard);
    }
  }

  shard.process_ready();
  process_smp_messages(id);

  current_scheduler_ptr = nullptr;
  detail::current_shard_id = INVALID_SHARD;
  detail::current_runtime = nullptr;
}

auto Runtime::process_smp_messages(shard_id id) -> bool {
  bool did_work = false;

  for (auto from : std::views::iota(0u, num_shards_)) {
    if (from == id)
      continue;

    while (auto msg = smp_queues_[from][id]->try_pop()) {
      msg->execute();
      did_work = true;
    }
  }

  return did_work;
}

auto Runtime::process_completions(Shard& shard) -> bool {
  unsigned count = shard.ring().process_completions([&](void* raw_data, int res,
                                                        unsigned flags) {
    if (raw_data == reinterpret_cast<void*>(WAKE_EVENT_TOKEN)) {
      std::uint64_t val;
      while (read(shard.wake_fd(), &val, sizeof(val)) > 0) {
      }
      if (!(flags & IORING_CQE_F_MORE)) {
        shard.ring().setup_wake_poll(shard.wake_fd());
      }
      return;
    }

    if (raw_data != nullptr) {
      auto* data = static_cast<io_data*>(raw_data);
      shard.untrack_io_data(data);
      data->result = res;
      data->flags = flags;

      if (data->coroutine != nullptr) {
        auto handle = std::coroutine_handle<>::from_address(data->coroutine);
        if (handle && !handle.done()) {
          if (data->owner_shard == shard.id() ||
              data->owner_shard == INVALID_SHARD) {
            handle.resume();
            if (handle.done()) {
              handle.destroy();
            }
          } else {
            submit_to(data->owner_shard, handle);
          }
        } else if (handle && handle.done()) {
          handle.destroy();
        }
      }
    }
  });

  return count > 0;
}

auto Runtime::wait_for_work(Shard& shard) -> void {
  if (shard.has_work())
    return;

  if (!shard.ring().valid()) {
    // Fallback when io_uring is not available
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return;
  }

  shard.ring().submit(true);
  shard.ring().wait(std::chrono::milliseconds(1000));
}

auto Runtime::schedule(std::coroutine_handle<> handle) noexcept -> void {
  if (!handle || handle.done())
    return;

  if (detail::current_shard_id != INVALID_SHARD) {
    shards_[detail::current_shard_id]->schedule_next(handle);
  } else {
    schedule_external(handle);
  }
}

auto Runtime::get_shard_id() const noexcept -> unsigned {
  return detail::current_shard_id != INVALID_SHARD ? detail::current_shard_id
                                                   : 0;
}

auto Runtime::is_current_shard() const noexcept -> bool {
  return detail::current_shard_id != INVALID_SHARD &&
         this == detail::current_runtime;
}

auto Runtime::schedule_on(shard_id target, std::coroutine_handle<> handle)
    -> void {
  if (!handle || handle.done())
    return;

  if (target == detail::current_shard_id) {
    shards_[target]->schedule_local(handle);
  } else {
    while (!shards_[target]->schedule_remote(handle)) {
      std::this_thread::yield();
    }
    wake_shard(target);
  }
}

auto Runtime::schedule_external(std::coroutine_handle<> handle) -> void {
  auto target =
      static_cast<shard_id>(std::hash<void*>{}(handle.address()) % num_shards_);
  schedule_on(target, handle);
}

auto Runtime::submit_to(shard_id target, std::coroutine_handle<> handle)
    -> void {
  if (target == detail::current_shard_id) {
    if (handle && !handle.done())
      handle.resume();
  } else {
    (void)smp_queues_[detail::current_shard_id][target]->push(
        SmpMessage{handle});
    wake_shard(target);
  }
}

auto Runtime::submit_io(IoRequest req) -> bool {
  if (detail::current_shard_id == INVALID_SHARD) {
    log::error("Cannot submit IO: not in a shard context");
    return false;
  }

  auto& shard = *shards_[detail::current_shard_id];
  if (!shard.ring().valid()) {
    log::error("Cannot submit IO: io_uring not initialized");
    return false;
  }

  shard.submit_io(std::move(req));
  return true;
}

auto Runtime::wake_shard(shard_id id) -> void {
  if (id >= num_shards_)
    return;

  int fd = shards_[id]->wake_fd();
  if (fd >= 0) {
    std::uint64_t val = 1;
    while (true) {
      auto ret = write(fd, &val, sizeof(val));
      if (ret == sizeof(val))
        break;
      if (ret < 0 && errno == EINTR)
        continue;
      if (ret < 0 && errno == EAGAIN)
        break;
      break;
    }
  }
}

auto Runtime::alloc_io_data() -> io_data* {
  auto sid = detail::current_shard_id;
  if (sid != INVALID_SHARD && sid < num_shards_) {
    auto* mr = shards_[sid]->memory_resource();
    auto* p =
        static_cast<io_data*>(mr->allocate(sizeof(io_data), alignof(io_data)));
    auto* data = std::construct_at(p);
    data->owner_shard = sid;
    return data;
  }
  auto* data = new io_data{};
  data->owner_shard = INVALID_SHARD;
  return data;
}

auto Runtime::free_io_data(io_data* data) -> void {
  if (data == nullptr)
    return;
  auto sid = data->owner_shard;
  if (sid != INVALID_SHARD && sid < num_shards_) {
    auto* mr = shards_[sid]->memory_resource();
    std::destroy_at(data);
    mr->deallocate(data, sizeof(io_data), alignof(io_data));
  } else {
    delete data;
  }
}

auto read_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  IoRequest req{.op = IoOpType::Read,
                .data = data_,
                .fd = fd_,
                .buf = buf_,
                .len = len_,
                .offset = offset_};
  rt->submit_io(std::move(req));
}

auto write_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  IoRequest req{.op = IoOpType::Write,
                .data = data_,
                .fd = fd_,
                .buf = const_cast<void*>(buf_),
                .len = len_,
                .offset = offset_};
  rt->submit_io(std::move(req));
}

auto poll_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  IoRequest req{
      .op = IoOpType::Poll, .data = data_, .fd = fd_, .poll_mask = mask_};
  rt->submit_io(std::move(req));
}

auto poll_timeout_awaiter::await_suspend(
    std::coroutine_handle<> handle) noexcept -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout_);
  auto nsecs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(timeout_ - secs);
  data_->ts.tv_sec = secs.count();
  data_->ts.tv_nsec = nsecs.count();

  IoRequest req{.op = IoOpType::PollTimeout,
                .data = data_,
                .fd = fd_,
                .poll_mask = mask_,
                .ts_ptr = &data_->ts};
  rt->submit_io(std::move(req));
}

auto sleep_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  auto secs = std::chrono::duration_cast<std::chrono::seconds>(duration_);
  auto nsecs =
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration_ - secs);
  data_->ts.tv_sec = secs.count();
  data_->ts.tv_nsec = nsecs.count();

  IoRequest req{.op = IoOpType::Timeout, .data = data_, .ts_ptr = &data_->ts};
  rt->submit_io(std::move(req));
}

auto close_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  IoRequest req{.op = IoOpType::Close, .data = data_, .fd = fd_};
  rt->submit_io(std::move(req));
}

auto cancel_awaiter::await_suspend(std::coroutine_handle<> handle) noexcept
    -> void {
  auto* rt = detail::current_runtime;
  data_ = rt->alloc_io_data();
  data_->coroutine = handle.address();

  IoRequest req{.op = IoOpType::Cancel,
                .data = data_,
                .cancel_user_data = cancel_user_data_};
  rt->submit_io(std::move(req));
}

}  // namespace taskmaster
