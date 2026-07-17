namespace dagforge {

inline constexpr shard_id kInvalidShard = std::numeric_limits<shard_id>::max();

class Runtime {
public:
  explicit Runtime(unsigned num_shards = 0, bool pin_shards_to_cores = false,
                   unsigned cpu_affinity_offset = 0);
  ~Runtime() noexcept;

  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;

  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  /// Launch an awaitable coroutine on the specified shard. This is the
  /// preferred way to spawn work.
  template <typename T> auto spawn_on(shard_id target, task<T> coro) -> void {
    assert(target < num_shards_);
    submit_to_shard(
        target,
        [&]() {
          return std::make_unique<CrossShardThunk>(
              [coro = std::move(coro)](
                  Runtime &rt, shard_id queued_target) mutable {
                Allocator alloc{rt.shards_[queued_target]->memory_resource()};
                co_spawn(rt.shards_[queued_target]->ctx().get_executor(),
                         std::move(coro),
                         boost::asio::bind_allocator(alloc, detached));
              });
        },
        [&]() {
          Allocator alloc{shards_[target]->memory_resource()};
          co_spawn(shards_[target]->ctx().get_executor(), std::move(coro),
                   boost::asio::bind_allocator(alloc, detached));
        });
  }

  /// Launch an awaitable coroutine on the current shard's executor.
  template <typename T> auto spawn(task<T> coro) -> void {
    auto sid = current_shard();
    if (sid == kInvalidShard) {
      sid = 0;
    }
    spawn_on(sid, std::move(coro));
  }

  /// Launch on a shard selected for external callers.
  template <typename T> auto spawn_external(task<T> coro) -> void {
    const auto target = static_cast<shard_id>(
        external_rr_.fetch_add(1, std::memory_order_relaxed) %
        std::max(1U, num_shards_));
    spawn_on(target, std::move(coro));
  }

  [[nodiscard]] auto shard_count() const noexcept -> unsigned {
    return num_shards_;
  }
  [[nodiscard]] auto pending_cross_shard_queue_length() const -> std::size_t;
  [[nodiscard]] auto pending_cross_shard_queue_length(shard_id target) const
      -> std::size_t;
  [[nodiscard]] auto cross_shard_messages_total(shard_id source,
                                                shard_id target) const
      -> std::uint64_t;
  [[nodiscard]] auto cross_shard_queue_overflow_total(shard_id source,
                                                      shard_id target) const
      -> std::uint64_t;
  [[nodiscard]] auto cross_shard_latency_snapshot(shard_id source,
                                                  shard_id target) const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto io_context_poll_duration_snapshot(shard_id id) const
      -> metrics::Histogram::Snapshot;
  [[nodiscard]] auto io_context_timer_depth(shard_id id) const -> std::uint64_t;
  [[nodiscard]] auto timing_wheel_pending_count(shard_id id) const
      -> std::size_t;
  auto note_timer_started(shard_id id) -> void;
  auto note_timer_finished(shard_id id) -> void;
  [[nodiscard]] auto stall_age_ms(shard_id id) const -> std::uint64_t;
  [[nodiscard]] auto pinned_cpu_for_shard(shard_id id) const noexcept -> int;
  template <typename Rep, typename Period, typename F>
  auto schedule_after_on(shard_id target, std::chrono::duration<Rep, Period> delay,
                         F &&fn) -> io::TimingWheel::Handle {
    if (target >= num_shards_ || !running_.load(std::memory_order_acquire)) {
      return {};
    }

    const auto delay_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(delay);
    if (delay_ns <= std::chrono::nanoseconds::zero()) {
      post_to(target, std::forward<F>(fn));
      return {};
    }

    if (is_current_shard() && current_shard() == target) {
      return timing_wheels_[target]->schedule_after(delay_ns,
                                                    std::forward<F>(fn));
    }

    post_to(target, [this, target, delay_ns,
                     fn = std::forward<F>(fn)]() mutable {
      if (!running_.load(std::memory_order_acquire) ||
          target >= timing_wheels_.size() || !timing_wheels_[target]) {
        return;
      }
      (void)timing_wheels_[target]->schedule_after(delay_ns, std::move(fn));
    });
    return {};
  }
  auto cancel_after_on(shard_id target, io::TimingWheel::Handle handle) -> void;

  [[nodiscard]] auto current_shard() const noexcept -> shard_id;
  [[nodiscard]] auto is_current_shard() const noexcept -> bool;
  [[nodiscard]] auto current_context() noexcept -> io::IoContext &;
  [[nodiscard]] auto shard(shard_id id) noexcept -> Shard & {
    assert(id < num_shards_);
    return *shards_[id];
  }
  [[nodiscard]] auto shard(shard_id id) const noexcept -> const Shard & {
    assert(id < num_shards_);
    return *shards_[id];
  }

  /// Get the executor for a specific shard.
  [[nodiscard]] auto executor_for(shard_id id)
      -> io::IoContext::executor_type {
    assert(id < num_shards_);
    return shards_[id]->ctx().get_executor();
  }

  /// Post a callable to a specific shard's executor (fire-and-forget).
  template <typename F> auto post_to(shard_id target, F &&fn) -> void {
    assert(target < num_shards_);
    submit_to_shard(
        target,
        [&]() {
          return std::make_unique<CrossShardThunk>(
              [fn = std::forward<F>(fn)](Runtime &, shard_id) mutable { fn(); });
        },
        [&]() {
          boost::asio::post(shards_[target]->ctx().get_executor(),
                            std::forward<F>(fn));
        });
  }

  /// Post a copy of a callable to every shard's executor.
  template <typename F> auto broadcast_to_all_shards(F fn) -> void {
    for (unsigned i = 0; i < num_shards_; ++i) {
      boost::asio::post(shards_[i]->ctx().get_executor(), fn);
    }
  }

private:
  template <typename T> struct alignas(64) CacheAlignedAtomic {
    std::atomic<T> value;

    CacheAlignedAtomic() noexcept : value{} {}
    explicit CacheAlignedAtomic(T initial) noexcept : value(initial) {}

    CacheAlignedAtomic(const CacheAlignedAtomic &other) noexcept
        : value(other.value.load(std::memory_order_relaxed)) {}

    auto operator=(const CacheAlignedAtomic &other) noexcept
        -> CacheAlignedAtomic & {
      value.store(other.value.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
      return *this;
    }

    CacheAlignedAtomic(CacheAlignedAtomic &&other) noexcept
        : value(other.value.load(std::memory_order_relaxed)) {}

    auto operator=(CacheAlignedAtomic &&other) noexcept
        -> CacheAlignedAtomic & {
      value.store(other.value.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
      return *this;
    }
  };

  template <typename ThunkFactory, typename DirectFn>
  auto submit_to_shard(shard_id target, ThunkFactory &&make_thunk,
                       DirectFn &&direct_submit) -> void {
    if (is_current_shard()) {
      const auto source = current_shard();
      if (source != kInvalidShard && source != target) {
        auto thunk = make_thunk();
        if (enqueue_cross_shard(source, target, thunk)) {
          return;
        }
        // The bounded mailbox is full. Preserve target-shard ownership by
        // handing the already materialized thunk to Asio's target queue. Never
        // execute it inline on the source shard.
        boost::asio::post(
            shards_[target]->ctx().get_executor(),
            [this, target, thunk = std::move(thunk)]() mutable {
              thunk->run(*this, target);
            });
        return;
      }
    }
    direct_submit();
  }

  struct CrossShardThunk {
    explicit CrossShardThunk(
        std::move_only_function<void(Runtime &, shard_id)> fn_)
        : fn(std::move(fn_)) {}

    auto run(Runtime &rt, shard_id target) -> void { fn(rt, target); }

    std::move_only_function<void(Runtime &, shard_id)> fn;
    std::chrono::steady_clock::time_point enqueued_at{};
  };

  using QueueItem = CrossShardThunk *;
  // Cross-shard dispatch uses one SPSC mailbox per source/target pair.
  using PairQueue =
      boost::lockfree::spsc_queue<QueueItem, boost::lockfree::capacity<4096>>;

  [[nodiscard]] auto enqueue_cross_shard(shard_id source, shard_id target,
                                         std::unique_ptr<CrossShardThunk> &item)
      -> bool;
  auto schedule_drain(shard_id target) -> void;
  auto drain_inbound(shard_id target) -> void;
  [[nodiscard]] auto has_pending_for_target(shard_id target) const -> bool;
  auto run_shard(shard_id id) -> void;
  auto bind_shard_thread_to_cpu(shard_id id) -> void;
  auto start_stall_detection() -> void;
  auto stop_stall_detection() -> void;
  auto start_timing_wheel_on_shard(shard_id id) -> void;

  alignas(64) std::atomic<bool> running_{false};
  bool pin_shards_to_cores_{false};
  unsigned cpu_affinity_offset_{0};
  unsigned num_shards_;
  std::vector<CacheAlignedAtomic<std::uint64_t>> shard_last_tick_ms_;
  std::vector<CacheAlignedAtomic<std::uint64_t>> io_context_timer_depth_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::vector<std::unique_ptr<io::TimingWheel>> timing_wheels_;
  std::vector<std::optional<
      boost::asio::executor_work_guard<io::IoContext::executor_type>>>
      work_guards_;
  std::vector<std::jthread> threads_;
  std::jthread stall_watchdog_thread_;
  std::vector<std::vector<std::unique_ptr<PairQueue>>> inbound_queues_;
  alignas(64) std::vector<CacheAlignedAtomic<bool>> drain_scheduled_;
  alignas(64) std::vector<CacheAlignedAtomic<std::uint32_t>> pending_inbound_;
  std::vector<std::vector<CacheAlignedAtomic<std::uint64_t>>>
      cross_shard_messages_total_;
  std::vector<std::vector<CacheAlignedAtomic<std::uint64_t>>>
      cross_shard_queue_overflow_total_;
  std::vector<std::vector<std::unique_ptr<metrics::Histogram>>>
      cross_shard_latency_histograms_;
  std::vector<std::unique_ptr<metrics::Histogram>>
      io_context_poll_duration_histograms_;
  std::vector<int> pinned_cpus_;
  alignas(64) std::atomic<std::uint64_t> external_rr_{0};
};

// Thread-local for internal use only.
namespace detail {
inline thread_local shard_id current_shard_id = kInvalidShard;
inline thread_local Runtime *current_runtime = nullptr;
} // namespace detail

// ---------------------------------------------------------------------------
// Free-function coroutine helpers
// ---------------------------------------------------------------------------

/// Yield execution back to the current shard's event loop (one cycle).
[[nodiscard]] inline auto async_yield() -> task<Result<void>> {
  auto executor = co_await boost::asio::this_coro::executor;
  auto post_res =
      co_await co_as_result(boost::asio::post(executor, dagforge::use_nothrow));
  if (!post_res) {
    co_return fail(post_res.error());
  }
  co_return ok();
}

[[nodiscard]] inline auto current_io_context() noexcept -> io::IoContext & {
  assert(detail::current_runtime != nullptr);
  assert(detail::current_shard_id != kInvalidShard);
  return detail::current_runtime->current_context();
}

[[nodiscard]] inline auto current_memory_resource() noexcept
    -> pmr::memory_resource * {
  if (detail::override_memory_resource != nullptr) {
    return detail::override_memory_resource;
  }
  assert(detail::current_runtime != nullptr);
  assert(detail::current_shard_id != kInvalidShard);
  return detail::current_runtime->shard(detail::current_shard_id)
      .memory_resource();
}

[[nodiscard]] inline auto current_memory_resource_or_default() noexcept
    -> pmr::memory_resource * {
  if (detail::override_memory_resource != nullptr) {
    return detail::override_memory_resource;
  }
  if (detail::current_runtime == nullptr ||
      detail::current_shard_id == kInvalidShard) {
    return pmr::get_default_resource();
  }
  return detail::current_runtime->shard(detail::current_shard_id)
      .memory_resource();
}

template <typename Rep, typename Period>
[[nodiscard]] inline auto async_sleep(std::chrono::duration<Rep, Period> duration) {
  return io::async_sleep(
      current_io_context(),
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

} // namespace dagforge
