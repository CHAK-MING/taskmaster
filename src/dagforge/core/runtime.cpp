#include "dagforge/core/runtime.hpp"

#include "dagforge/core/scope_exit.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <ranges>
#include <span>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

namespace dagforge {

namespace {
[[nodiscard]] auto now_monotonic_ms() -> std::uint64_t {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

constexpr std::array<std::uint64_t, 12> kCrossShardLatencyBucketsNs{
    1'000ULL,     5'000ULL,     10'000ULL,    25'000ULL,
    50'000ULL,    100'000ULL,   250'000ULL,   500'000ULL,
    1'000'000ULL, 2'500'000ULL, 5'000'000ULL, 10'000'000ULL};
constexpr std::array<std::uint64_t, 15> kIoPollDurationBucketsNs{
    100'000ULL,   250'000ULL,   500'000ULL,   1'000'000ULL,
    2'500'000ULL, 5'000'000ULL, 10'000'000ULL, 25'000'000ULL,
    50'000'000ULL, 100'000'000ULL, 250'000'000ULL, 500'000'000ULL,
    1'000'000'000ULL, 2'500'000'000ULL, 10'000'000'000ULL};
constexpr auto kShardProgressInterval = std::chrono::milliseconds(50);
constexpr auto kWatchdogPollInterval = std::chrono::milliseconds(100);
constexpr std::uint64_t kStallThresholdMs = 200;

template <typename F>
concept ShardVisitor = std::invocable<F, unsigned>;

template <ShardVisitor Fn>
auto for_each_shard(unsigned num_shards, Fn &&fn) -> void {
  for (auto i : std::views::iota(0U, num_shards)) {
    fn(i);
  }
}

#ifdef __linux__
[[nodiscard]] auto allowed_cpus_for_current_thread() -> std::vector<int> {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  if (pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    return {};
  }

  std::vector<int> cpus;
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(cpu, &cpuset)) {
      cpus.push_back(cpu);
    }
  }
  return cpus;
}
#endif
} // namespace

Runtime::Runtime(unsigned num_shards, bool pin_shards_to_cores,
                 unsigned cpu_affinity_offset)
    : pin_shards_to_cores_(pin_shards_to_cores),
      cpu_affinity_offset_(cpu_affinity_offset) {
  if (num_shards == 0) {
    num_shards = 4;
  }
  num_shards_ = num_shards;

  shards_.reserve(num_shards);
  timing_wheels_.reserve(num_shards);
  work_guards_.resize(num_shards);
  inbound_queues_.resize(num_shards);
  drain_scheduled_.reserve(num_shards);
  pending_inbound_.reserve(num_shards);
  shard_last_tick_ms_.reserve(num_shards);
  io_context_timer_depth_.reserve(num_shards);
  pinned_cpus_.assign(num_shards, -1);
  cross_shard_messages_total_.resize(num_shards);
  cross_shard_queue_overflow_total_.resize(num_shards);
  cross_shard_latency_histograms_.resize(num_shards);
  io_context_poll_duration_histograms_.resize(num_shards);
  for (unsigned i = 0; i < num_shards; ++i) {
    drain_scheduled_.emplace_back(false);
    pending_inbound_.emplace_back(0);
    shard_last_tick_ms_.emplace_back(now_monotonic_ms());
    io_context_timer_depth_.emplace_back(0);
    cross_shard_messages_total_[i].resize(num_shards);
    cross_shard_queue_overflow_total_[i].resize(num_shards);
    cross_shard_latency_histograms_[i].resize(num_shards);
    io_context_poll_duration_histograms_[i] =
        std::make_unique<metrics::Histogram>(
            std::span<const std::uint64_t>(kIoPollDurationBucketsNs));
    for (unsigned target = 0; target < num_shards; ++target) {
      if (i == target) {
        continue;
      }
      cross_shard_latency_histograms_[i][target] =
          std::make_unique<metrics::Histogram>(
              std::span<const std::uint64_t>(kCrossShardLatencyBucketsNs));
    }
  }

  for (unsigned source = 0; source < num_shards; ++source) {
    inbound_queues_[source].resize(num_shards);
    for (unsigned target = 0; target < num_shards; ++target) {
      if (source == target) {
        continue;
      }
      inbound_queues_[source][target] = std::make_unique<PairQueue>();
    }
  }

  for_each_shard(num_shards, [this](unsigned i) {
    shards_.emplace_back(std::make_unique<Shard>(i));
  });
  for_each_shard(num_shards, [this](unsigned i) {
    timing_wheels_.emplace_back(
        std::make_unique<io::TimingWheel>(shards_[i]->ctx()));
  });
}

Runtime::~Runtime() noexcept { stop(); }

auto Runtime::start() -> Result<void> {
  if (running_.exchange(true))
    return ok();

  log::debug("Starting runtime with {} shards", shards_.size());

  threads_.reserve(num_shards_);
  for_each_shard(num_shards_, [this](unsigned i) {
    auto &ctx = shards_[i]->ctx();
    ctx.restart();
    work_guards_[i].emplace(boost::asio::make_work_guard(ctx));
    threads_.emplace_back([this, i] { run_shard(i); });
  });

  start_stall_detection();

  return ok();
}

auto Runtime::stop() noexcept -> void {
  if (!running_.exchange(false))
    return;

  stop_stall_detection();

  for (auto i : std::views::iota(0U, num_shards_)) {
    auto work_guard = std::exchange(work_guards_[i], std::nullopt);
    if (work_guard.has_value()) {
      work_guard.value().reset();
    }
    shards_[i]->ctx().stop();
  }

  // std::jthread auto-joins on destruction, just clear the vector
  threads_.clear();

  // Cross-shard queue items are raw pointers; release any remaining thunks
  // after all shard threads have stopped.
  for (unsigned source = 0; source < num_shards_; ++source) {
    for (unsigned target = 0; target < num_shards_; ++target) {
      if (source == target) {
        continue;
      }
      auto &q_ptr = inbound_queues_[source][target];
      if (!q_ptr) {
        continue;
      }
      QueueItem item = nullptr;
      while (q_ptr->pop(item)) {
        std::unique_ptr<CrossShardThunk> owned(item);
        item = nullptr;
      }
    }
  }
  for (unsigned i = 0; i < num_shards_; ++i) {
    if (i < timing_wheels_.size() && timing_wheels_[i]) {
      timing_wheels_[i]->stop();
    }
    pending_inbound_[i].value.store(0, std::memory_order_release);
    drain_scheduled_[i].value.store(false, std::memory_order_release);
  }
}

auto Runtime::is_running() const noexcept -> bool {
  return running_.load(std::memory_order_acquire);
}

auto Runtime::current_shard() const noexcept -> shard_id {
  return detail::current_shard_id;
}

auto Runtime::is_current_shard() const noexcept -> bool {
  return detail::current_runtime == this &&
         detail::current_shard_id != kInvalidShard;
}

auto Runtime::current_context() noexcept -> io::IoContext & {
  return shards_[current_shard()]->ctx();
}

auto Runtime::run_shard(shard_id id) -> void {
  bind_shard_thread_to_cpu(id);
  detail::current_shard_id = id;
  detail::current_runtime = this;
  dagforge::scope_exit reset_thread_locals{
      [] {
        detail::current_shard_id = kInvalidShard;
        detail::current_runtime = nullptr;
      }};

  auto &ctx = shards_[id]->ctx();
  start_timing_wheel_on_shard(id);
  // Drain any cross-shard work that arrived before this shard entered run().
  drain_inbound(id);
  while (running_.load(std::memory_order_acquire) && !ctx.stopped()) {
    const auto poll_started = std::chrono::steady_clock::now();
    (void)ctx.run_one_for(kShardProgressInterval);
    const auto poll_finished = std::chrono::steady_clock::now();
    const auto poll_elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(poll_finished -
                                                            poll_started)
            .count();
    shard_last_tick_ms_[id].value.store(
        static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                poll_finished.time_since_epoch())
                .count()),
        std::memory_order_release);
    if (id < io_context_poll_duration_histograms_.size() &&
        io_context_poll_duration_histograms_[id]) {
      io_context_poll_duration_histograms_[id]->observe_ns(
          static_cast<std::uint64_t>(poll_elapsed_ns > 0 ? poll_elapsed_ns : 0));
    }
  }
}

auto Runtime::bind_shard_thread_to_cpu(shard_id id) -> void {
#ifdef __linux__
  if (!pin_shards_to_cores_) {
    pinned_cpus_[id] = -1;
    return;
  }

  const auto allowed = allowed_cpus_for_current_thread();
  if (allowed.empty()) {
    log::warn("Shard {} failed to query allowed CPUs for affinity", id);
    pinned_cpus_[id] = -1;
    return;
  }

  const auto cpu =
      allowed[(cpu_affinity_offset_ + static_cast<unsigned>(id)) % allowed.size()];
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);

  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    log::warn("Shard {} failed to bind to CPU {}: {}", id, cpu,
              std::strerror(errno));
    pinned_cpus_[id] = -1;
    return;
  }

  pinned_cpus_[id] = cpu;
  log::debug("Shard {} bound to CPU {}", id, cpu);
#else
  (void)id;
#endif
}

auto Runtime::enqueue_cross_shard(shard_id source, shard_id target,
                                  std::unique_ptr<CrossShardThunk> &item)
    -> bool {
  if (source == target || target >= num_shards_ || source >= num_shards_) {
    return false;
  }
  auto &q_ptr = inbound_queues_[source][target];
  if (!q_ptr) {
    return false;
  }
  auto *raw = item.get();
  raw->enqueued_at = std::chrono::steady_clock::now();
  if (!q_ptr->push(raw)) {
    if (source < cross_shard_queue_overflow_total_.size() &&
        target < cross_shard_queue_overflow_total_[source].size()) {
      cross_shard_queue_overflow_total_[source][target].value.fetch_add(
          1, std::memory_order_relaxed);
    }
    return false;
  }
  [[maybe_unused]] auto *released = item.release();
  if (source < cross_shard_messages_total_.size() &&
      target < cross_shard_messages_total_[source].size()) {
    cross_shard_messages_total_[source][target].value.fetch_add(
        1, std::memory_order_relaxed);
  }
  pending_inbound_[target].value.fetch_add(1, std::memory_order_release);
  schedule_drain(target);
  return true;
}

auto Runtime::schedule_drain(shard_id target) -> void {
  if (target >= num_shards_) {
    return;
  }
  bool expected = false;
  if (!drain_scheduled_[target].value.compare_exchange_strong(
          expected, true, std::memory_order_acq_rel,
          std::memory_order_acquire)) {
    return;
  }
  boost::asio::post(shards_[target]->ctx().get_executor(),
                    [this, target]() { drain_inbound(target); });
}

auto Runtime::drain_inbound(shard_id target) -> void {
  if (target >= num_shards_) {
    return;
  }

  for (;;) {
    // Each source->target mailbox is SPSC. We drain them all on the target
    // shard thread, then clear the scheduled bit. If a sender races and marks
    // new work after that point, the compare-exchange below re-arms exactly one
    // follow-up drain without needing a strand or a global lock.
    for (unsigned source = 0; source < num_shards_; ++source) {
      if (source == target) {
        continue;
      }
      auto &q_ptr = inbound_queues_[source][target];
      if (!q_ptr) {
        continue;
      }
      QueueItem item = nullptr;
      while (q_ptr->pop(item)) {
        if (item) {
          pending_inbound_[target].value.fetch_sub(1, std::memory_order_acquire);
          std::unique_ptr<CrossShardThunk> owned(item);
          const auto elapsed_ns =
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - owned->enqueued_at)
                  .count();
          if (source < cross_shard_latency_histograms_.size() &&
              target < cross_shard_latency_histograms_[source].size() &&
              cross_shard_latency_histograms_[source][target]) {
            cross_shard_latency_histograms_[source][target]->observe_ns(
                static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
          }
          owned->run(*this, target);
        }
        item = nullptr;
      }
    }

    drain_scheduled_[target].value.store(false, std::memory_order_release);
    if (!has_pending_for_target(target)) {
      break;
    }
    bool expected = false;
    if (!drain_scheduled_[target].value.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      break;
    }
  }
}

auto Runtime::has_pending_for_target(shard_id target) const -> bool {
  return pending_inbound_[target].value.load(std::memory_order_acquire) > 0;
}

auto Runtime::pending_cross_shard_queue_length() const -> std::size_t {
  std::size_t total = 0;
  for (unsigned source = 0; source < num_shards_; ++source) {
    for (unsigned target = 0; target < num_shards_; ++target) {
      if (source == target) {
        continue;
      }
      const auto &q_ptr = inbound_queues_[source][target];
      if (q_ptr) {
        total += q_ptr->read_available();
      }
    }
  }
  return total;
}

auto Runtime::pending_cross_shard_queue_length(shard_id target) const
    -> std::size_t {
  if (target >= num_shards_) {
    return 0;
  }
  std::size_t total = 0;
  for (unsigned source = 0; source < num_shards_; ++source) {
    if (source == target) {
      continue;
    }
    const auto &q_ptr = inbound_queues_[source][target];
    if (q_ptr) {
      total += q_ptr->read_available();
    }
  }
  return total;
}

auto Runtime::cross_shard_messages_total(shard_id source, shard_id target) const
    -> std::uint64_t {
  if (source >= cross_shard_messages_total_.size() ||
      target >= cross_shard_messages_total_[source].size()) {
    return 0;
  }
  return cross_shard_messages_total_[source][target].value.load(
      std::memory_order_relaxed);
}

auto Runtime::cross_shard_queue_overflow_total(shard_id source,
                                               shard_id target) const
    -> std::uint64_t {
  if (source >= cross_shard_queue_overflow_total_.size() ||
      target >= cross_shard_queue_overflow_total_[source].size()) {
    return 0;
  }
  return cross_shard_queue_overflow_total_[source][target].value.load(
      std::memory_order_relaxed);
}

auto Runtime::cross_shard_latency_snapshot(shard_id source,
                                           shard_id target) const
    -> metrics::Histogram::Snapshot {
  if (source >= cross_shard_latency_histograms_.size() ||
      target >= cross_shard_latency_histograms_[source].size() ||
      !cross_shard_latency_histograms_[source][target]) {
    return {};
  }
  return cross_shard_latency_histograms_[source][target]->snapshot();
}

auto Runtime::io_context_poll_duration_snapshot(shard_id id) const
    -> metrics::Histogram::Snapshot {
  if (id >= io_context_poll_duration_histograms_.size() ||
      !io_context_poll_duration_histograms_[id]) {
    return {};
  }
  return io_context_poll_duration_histograms_[id]->snapshot();
}

auto Runtime::io_context_timer_depth(shard_id id) const -> std::uint64_t {
  if (id >= io_context_timer_depth_.size()) {
    return 0;
  }
  return io_context_timer_depth_[id].value.load(std::memory_order_relaxed);
}

auto Runtime::timing_wheel_pending_count(shard_id id) const -> std::size_t {
  if (id >= timing_wheels_.size() || !timing_wheels_[id]) {
    return 0;
  }
  return timing_wheels_[id]->pending_count();
}

auto Runtime::cancel_after_on(shard_id target,
                              io::TimingWheel::Handle handle) -> void {
  if (!handle.valid() || target >= num_shards_ ||
      !running_.load(std::memory_order_acquire)) {
    return;
  }

  if (is_current_shard() && current_shard() == target) {
    if (target < timing_wheels_.size() && timing_wheels_[target]) {
      (void)timing_wheels_[target]->cancel(handle);
    }
    return;
  }

  post_to(target, [this, target, handle]() mutable {
    if (!running_.load(std::memory_order_acquire) ||
        target >= timing_wheels_.size() || !timing_wheels_[target]) {
      return;
    }
    (void)timing_wheels_[target]->cancel(handle);
  });
}

auto Runtime::note_timer_started(shard_id id) -> void {
  if (id >= io_context_timer_depth_.size()) {
    return;
  }
  io_context_timer_depth_[id].value.fetch_add(1, std::memory_order_relaxed);
}

auto Runtime::note_timer_finished(shard_id id) -> void {
  if (id >= io_context_timer_depth_.size()) {
    return;
  }
  io_context_timer_depth_[id].value.fetch_sub(1, std::memory_order_relaxed);
}

auto Runtime::stall_age_ms(shard_id id) const -> std::uint64_t {
  if (id >= num_shards_) {
    return 0;
  }
  const auto last_tick =
      shard_last_tick_ms_[id].value.load(std::memory_order_acquire);
  const auto now_ms = now_monotonic_ms();
  return now_ms >= last_tick ? (now_ms - last_tick) : 0;
}

auto Runtime::pinned_cpu_for_shard(shard_id id) const noexcept -> int {
  if (id >= num_shards_) {
    return -1;
  }
  return pinned_cpus_[id];
}

auto Runtime::start_timing_wheel_on_shard(shard_id id) -> void {
  if (id >= timing_wheels_.size() || !timing_wheels_[id]) {
    return;
  }
  timing_wheels_[id]->start();
}

auto Runtime::start_stall_detection() -> void {
  for (unsigned i = 0; i < num_shards_; ++i) {
    shard_last_tick_ms_[i].value.store(now_monotonic_ms(),
                                       std::memory_order_release);
  }

  stall_watchdog_thread_ = std::jthread([this](const std::stop_token &st) {
    std::condition_variable_any wakeup;
    std::mutex wakeup_mutex;
    std::vector<bool> warned(num_shards_, false);
    while (!st.stop_requested() && running_.load(std::memory_order_acquire)) {
      {
        std::unique_lock lock(wakeup_mutex);
        (void)wakeup.wait_for(lock, st, kWatchdogPollInterval, [this] {
          return !running_.load(std::memory_order_acquire);
        });
      }
      if (st.stop_requested() || !running_.load(std::memory_order_acquire)) {
        break;
      }

      for (unsigned i = 0; i < num_shards_; ++i) {
        const auto age_ms = stall_age_ms(i);
        if (age_ms > kStallThresholdMs) {
          if (!warned[i]) {
            log::error("Shard {} stalled ({} ms without heartbeat). Possible "
                       "blocking call in coroutine.",
                       i, age_ms);
            warned[i] = true;
          }
        } else {
          warned[i] = false;
        }
      }
    }
  });
}

auto Runtime::stop_stall_detection() -> void {
  if (stall_watchdog_thread_.joinable()) {
    stall_watchdog_thread_.request_stop();
    stall_watchdog_thread_.join();
  }
}

} // namespace dagforge
