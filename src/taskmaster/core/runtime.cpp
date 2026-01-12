#include "taskmaster/core/runtime.hpp"

namespace taskmaster {

namespace {
  template <typename F>
  concept ShardVisitor = std::invocable<F, unsigned>;

  template <typename F>
  concept ShardPairVisitor = std::invocable<F, unsigned, unsigned>;

  template <ShardVisitor Fn>
  auto for_each_shard(unsigned num_shards, Fn&& fn) -> void {
    for (auto i : std::views::iota(0u, num_shards)) {
      fn(i);
    }
  }

  template <ShardPairVisitor Fn>
  auto for_each_shard_pair(unsigned num_shards, Fn&& fn) -> void {
    for (auto from : std::views::iota(0u, num_shards)) {
      for (auto to : std::views::iota(0u, num_shards)) {
        fn(from, to);
      }
    }
  }
}

Runtime::Runtime(unsigned num_shards) {
  if (num_shards == 0) {
    num_shards = std::thread::hardware_concurrency();
    if (num_shards == 0)
      num_shards = 1;
  }
  num_shards_ = num_shards;

  shards_.reserve(num_shards);
  for_each_shard(num_shards, [this](unsigned i) {
    shards_.emplace_back(std::make_unique<Shard>(i));
  });

  smp_queues_.resize(num_shards);
  for_each_shard(num_shards, [this, num_shards](unsigned from) {
    smp_queues_[from].reserve(num_shards);
    for_each_shard(num_shards, [this, from](unsigned) {
      smp_queues_[from].emplace_back(
          std::make_unique<SPSCQueue<SmpMessage>>(128));
    });
  });
}

Runtime::~Runtime() {
  stop();
}

auto Runtime::start() -> void {
  if (running_.exchange(true))
    return;
  stop_requested_.store(false);

  threads_.reserve(num_shards_);
  for_each_shard(num_shards_, [this](unsigned i) {
    threads_.emplace_back([this, i] { run_shard(i); });
  });
}

auto Runtime::stop() -> void {
  if (!running_.exchange(false))
    return;
  stop_requested_.store(true);

  for (auto i : std::views::iota(0u, num_shards_)) {
    wake_shard(i);
  }

  // std::jthread auto-joins on destruction, just clear the vector
  threads_.clear();

  for_each_shard_pair(num_shards_, [this](unsigned from, unsigned to) {
    while (auto msg = smp_queues_[from][to]->try_pop()) {
      if (std::holds_alternative<std::coroutine_handle<>>(msg->payload)) {
        auto h = std::get<std::coroutine_handle<>>(msg->payload);
        if (h) {
          h.destroy();
        }
      }
    }
  });

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

auto Runtime::current_context() noexcept -> io::IoContext& {
  return shards_[current_shard()]->ctx();
}

auto Runtime::run_shard(shard_id id) -> void {
  detail::current_shard_id = id;
  detail::current_runtime = this;

  auto& shard = *shards_[id];
  shard.ctx().setup_wake_poll(shard.wake_fd());

  while (!stop_requested_.load(std::memory_order_acquire)) {
    bool has_work = false;

    has_work |= shard.process_ready();
    has_work |= process_smp_messages(id);
    shard.process_io();
    has_work |= shard.ctx().submit(false) > 0;
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
  unsigned count = shard.ctx().process_completions([&](void* raw_data, int res,
                                                        unsigned flags) {
    if (raw_data == reinterpret_cast<void*>(WAKE_EVENT_TOKEN)) {
      shard.wake_event().consume();
      if (!(flags & CQE_F_MORE)) {
        shard.ctx().setup_wake_poll(shard.wake_fd());
      }
      return;
    }

    if (raw_data != nullptr) {
      auto* data = static_cast<CompletionData*>(raw_data);
      shard.untrack_io_data(data);
      data->result = res;
      data->flags = flags;
      data->completed = true;

      if (!data->continuation) {
        delete data;
        return;
      }

      auto handle = data->continuation;
      if (!handle) {
        return;
      }

      if (handle.done()) {
        handle.destroy();
        return;
      }

      // Avoid resuming inline from the completion callback to prevent re-entrancy.
      if (data->owner_shard == shard.id() || data->owner_shard == INVALID_SHARD) {
        shard.schedule_next(handle);
      } else {
        submit_to(data->owner_shard, handle);
      }
    }
  });

  return count > 0;
}

auto Runtime::wait_for_work(Shard& shard) -> void {
  if (shard.has_work())
    return;

  if (!shard.ctx().valid()) {
    // Fallback when io_uring is not available
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    return;
  }

  (void)shard.ctx().submit(true);
  shard.ctx().wait(std::chrono::milliseconds(1000));
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
  auto target = shard_id{0};
  if (num_shards_ <= 1) {
    target = 0;
  } else {
    // Reserve shard0 for control-plane actors (e.g. Engine).
    target = static_cast<shard_id>(
                 std::hash<void*>{}(handle.address()) % (num_shards_ - 1)) +
             1;
  }
  schedule_on(target, handle);
}

auto Runtime::submit_to(shard_id target, std::coroutine_handle<> handle)
    -> void {
  if (target == detail::current_shard_id) {
    if (!handle) return;
    if (handle.done()) {
      handle.destroy();
      return;
    }
    // Avoid resuming inline to prevent re-entrancy.
    shards_[target]->schedule_next(handle);
  } else {
    (void)smp_queues_[detail::current_shard_id][target]->push(
        SmpMessage{handle});
    wake_shard(target);
  }
}

auto Runtime::wake_shard(shard_id id) -> void {
  if (id >= num_shards_)
    return;

  // Only signal if the shard is sleeping to avoid excessive syscalls
  if (shards_[id]->is_sleeping()) {
    shards_[id]->wake_event().signal();
  }
}

}  // namespace taskmaster
