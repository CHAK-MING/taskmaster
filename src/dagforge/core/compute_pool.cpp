#include "dagforge/core/compute_pool.hpp"

#include "dagforge/util/log.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#ifdef __linux__
#include <cerrno>
#include <cstring>
#include <pthread.h>
#include <sched.h>
#endif

namespace dagforge {
namespace {

constexpr std::array<std::uint64_t, 12> kQueueWaitBucketsNs{
    1'000ULL,     5'000ULL,     10'000ULL,    25'000ULL,
    50'000ULL,    100'000ULL,   250'000ULL,   500'000ULL,
    1'000'000ULL, 5'000'000ULL, 25'000'000ULL, 100'000'000ULL};
constexpr std::array<std::uint64_t, 14> kExecutionTimeBucketsNs{
    10'000ULL,      50'000ULL,      100'000ULL,     250'000ULL,
    500'000ULL,     1'000'000ULL,   5'000'000ULL,   10'000'000ULL,
    25'000'000ULL,  50'000'000ULL,  100'000'000ULL, 250'000'000ULL,
    1'000'000'000ULL, 10'000'000'000ULL};

[[nodiscard]] auto normalized_thread_count(std::size_t configured)
    -> std::size_t {
  if (configured > 0) {
    return configured;
  }
  const auto hardware_threads = std::max(1U, std::thread::hardware_concurrency());
  return std::max<std::size_t>(1, hardware_threads / 2U);
}

[[nodiscard]] constexpr auto priority_index(ComputePriority priority) noexcept
    -> std::size_t {
  return static_cast<std::size_t>(priority);
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

class ComputePool::Impl {
public:
  explicit Impl(ComputePoolConfig config)
      : config_(std::move(config)),
        queue_wait_time_(std::span<const std::uint64_t>(kQueueWaitBucketsNs)),
        execution_time_(
            std::span<const std::uint64_t>(kExecutionTimeBucketsNs)) {
    config_.thread_count = normalized_thread_count(config_.thread_count);
    config_.queue_capacity = std::max<std::size_t>(1, config_.queue_capacity);
    active_stop_sources_.resize(config_.thread_count);
  }

  ~Impl() noexcept { stop(ComputeShutdownMode::CancelPending); }

  [[nodiscard]] auto start() -> Result<void> {
    {
      std::lock_guard lock(mutex_);
      if (running_.load(std::memory_order_acquire)) {
        return ok();
      }

      accepting_ = true;
      stopping_ = false;
      cancel_pending_ = false;
      running_.store(true, std::memory_order_release);
    }

    try {
      workers_.reserve(config_.thread_count);
      for (std::size_t index = 0; index < config_.thread_count; ++index) {
        workers_.emplace_back(
            [this, index](std::stop_token token) { worker_loop(token, index); });
      }
    } catch (...) {
      {
        std::lock_guard lock(mutex_);
        accepting_ = false;
        stopping_ = true;
        running_.store(false, std::memory_order_release);
      }
      for (auto &worker : workers_) {
        worker.request_stop();
      }
      work_available_.notify_all();
      workers_.clear();
      return fail(Error::ResourceExhausted);
    }

    return ok();
  }

  auto stop(ComputeShutdownMode mode) noexcept -> void {
    {
      std::lock_guard lock(mutex_);
      if (!running_.load(std::memory_order_acquire)) {
        return;
      }
      accepting_ = false;
      stopping_ = true;
      cancel_pending_ = mode == ComputeShutdownMode::CancelPending;

      if (cancel_pending_) {
        for (auto &queue : queues_) {
          for (auto &item : queue) {
            (void)item.stop_source->request_stop();
          }
        }
        for (const auto &stop_source : active_stop_sources_) {
          if (stop_source) {
            (void)stop_source->request_stop();
          }
        }
      }
    }
    work_available_.notify_all();

    {
      std::unique_lock lock(mutex_);
      idle_.wait(lock, [this] { return queued_tasks_ == 0 && active_tasks_ == 0; });
    }

    for (auto &worker : workers_) {
      worker.request_stop();
    }
    work_available_.notify_all();
    workers_.clear();

    {
      std::lock_guard lock(mutex_);
      stopping_ = false;
      cancel_pending_ = false;
      running_.store(false, std::memory_order_release);
    }
  }

  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto submit(ComputeOptions options, Work work,
                            OnDiscard on_discard)
      -> Result<ComputeTaskHandle> {
    if (!work) {
      return fail(Error::InvalidArgument);
    }

    try {
      auto stop_source = std::make_shared<std::stop_source>();
      WorkItem item{
          .options = std::move(options),
          .enqueued_at = std::chrono::steady_clock::now(),
          .stop_source = stop_source,
          .work = std::move(work),
          .on_discard = std::move(on_discard),
      };

      {
        std::lock_guard lock(mutex_);
        if (!accepting_ || !running_.load(std::memory_order_acquire)) {
          rejected_total_.fetch_add(1, std::memory_order_relaxed);
          return fail(Error::SystemNotRunning);
        }
        if (queued_tasks_ >= config_.queue_capacity) {
          rejected_total_.fetch_add(1, std::memory_order_relaxed);
          return fail(Error::ResourceExhausted);
        }

        queues_[priority_index(item.options.priority)].push_back(std::move(item));
        ++queued_tasks_;
        submitted_total_.fetch_add(1, std::memory_order_relaxed);
      }

      work_available_.notify_one();
      return ok(ComputeTaskHandle{std::move(stop_source)});
    } catch (...) {
      rejected_total_.fetch_add(1, std::memory_order_relaxed);
      return fail(Error::ResourceExhausted);
    }
  }

  [[nodiscard]] auto snapshot() const -> ComputePoolSnapshot {
    ComputePoolSnapshot out;
    out.thread_count = config_.thread_count;
    out.queue_capacity = config_.queue_capacity;
    {
      std::lock_guard lock(mutex_);
      out.queued_tasks = queued_tasks_;
      out.active_tasks = active_tasks_;
    }
    out.submitted_total = submitted_total_.load(std::memory_order_relaxed);
    out.completed_total = completed_total_.load(std::memory_order_relaxed);
    out.rejected_total = rejected_total_.load(std::memory_order_relaxed);
    out.cancelled_total = cancelled_total_.load(std::memory_order_relaxed);
    out.timed_out_total = timed_out_total_.load(std::memory_order_relaxed);
    out.failed_total = failed_total_.load(std::memory_order_relaxed);
    out.queue_wait_time = queue_wait_time_.snapshot();
    out.execution_time = execution_time_.snapshot();
    return out;
  }

private:
  struct WorkItem {
    ComputeOptions options;
    std::chrono::steady_clock::time_point enqueued_at;
    std::shared_ptr<std::stop_source> stop_source;
    Work work;
    OnDiscard on_discard;
  };

  [[nodiscard]] auto pop_next_locked() -> WorkItem {
    for (std::size_t index = queues_.size(); index > 0; --index) {
      auto &queue = queues_[index - 1];
      if (!queue.empty()) {
        auto item = std::move(queue.front());
        queue.pop_front();
        --queued_tasks_;
        return item;
      }
    }
    std::unreachable();
  }

  auto invoke_discard(WorkItem &item, Error reason) noexcept -> void {
    if (reason == Error::Timeout) {
      timed_out_total_.fetch_add(1, std::memory_order_relaxed);
    } else {
      cancelled_total_.fetch_add(1, std::memory_order_relaxed);
    }

    if (!item.on_discard) {
      return;
    }
    try {
      item.on_discard(reason);
    } catch (...) {
      failed_total_.fetch_add(1, std::memory_order_relaxed);
    }
  }

  auto worker_loop(std::stop_token worker_stop, std::size_t worker_index)
      -> void {
    bind_worker_thread(worker_index);

    for (;;) {
      std::optional<WorkItem> item;
      bool force_cancel = false;
      {
        std::unique_lock lock(mutex_);
        work_available_.wait(lock, [this, worker_stop] {
          return queued_tasks_ > 0 || stopping_ || worker_stop.stop_requested();
        });

        if (queued_tasks_ == 0 &&
            (stopping_ || worker_stop.stop_requested())) {
          return;
        }
        if (queued_tasks_ == 0) {
          continue;
        }

        item.emplace(pop_next_locked());
        force_cancel = cancel_pending_;
        ++active_tasks_;
        active_stop_sources_[worker_index] = item->stop_source;
      }

      const auto started_at = std::chrono::steady_clock::now();
      const auto queue_wait_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              started_at - item->enqueued_at)
              .count();
      queue_wait_time_.observe_ns(
          static_cast<std::uint64_t>(std::max<std::int64_t>(0, queue_wait_ns)));

      if (force_cancel || item->stop_source->stop_requested()) {
        invoke_discard(*item, Error::Cancelled);
      } else if (item->options.start_deadline &&
                 started_at >= *item->options.start_deadline) {
        (void)item->stop_source->request_stop();
        invoke_discard(*item, Error::Timeout);
      } else {
        try {
          item->work(item->stop_source->get_token());
          completed_total_.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
          failed_total_.fetch_add(1, std::memory_order_relaxed);
          if (item->on_discard) {
            try {
              item->on_discard(Error::Unknown);
            } catch (...) {
              failed_total_.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }

        const auto finished_at = std::chrono::steady_clock::now();
        const auto execution_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(finished_at -
                                                                started_at)
                .count();
        execution_time_.observe_ns(static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, execution_ns)));
      }

      {
        std::lock_guard lock(mutex_);
        active_stop_sources_[worker_index].reset();
        --active_tasks_;
        if (queued_tasks_ == 0 && active_tasks_ == 0) {
          idle_.notify_all();
        }
      }
    }
  }

  auto bind_worker_thread(std::size_t worker_index) -> void {
#ifdef __linux__
    if (!config_.pin_threads_to_cores) {
      return;
    }

    const auto allowed = allowed_cpus_for_current_thread();
    if (allowed.empty()) {
      log::warn("Compute worker {} failed to query allowed CPUs", worker_index);
      return;
    }

    const auto cpu = allowed[(config_.cpu_affinity_offset + worker_index) %
                             allowed.size()];
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
      log::warn("Compute worker {} failed to bind to CPU {}: {}", worker_index,
                cpu, std::strerror(errno));
    }
#else
    (void)worker_index;
#endif
  }

  ComputePoolConfig config_;
  mutable std::mutex mutex_;
  std::condition_variable work_available_;
  std::condition_variable idle_;
  std::array<std::deque<WorkItem>, 4> queues_{};
  std::vector<std::jthread> workers_{};
  std::vector<std::shared_ptr<std::stop_source>> active_stop_sources_{};
  std::size_t queued_tasks_{0};
  std::size_t active_tasks_{0};
  bool accepting_{false};
  bool stopping_{false};
  bool cancel_pending_{false};
  std::atomic<bool> running_{false};

  std::atomic<std::uint64_t> submitted_total_{0};
  std::atomic<std::uint64_t> completed_total_{0};
  std::atomic<std::uint64_t> rejected_total_{0};
  std::atomic<std::uint64_t> cancelled_total_{0};
  std::atomic<std::uint64_t> timed_out_total_{0};
  std::atomic<std::uint64_t> failed_total_{0};
  metrics::Histogram queue_wait_time_;
  metrics::Histogram execution_time_;
};

ComputePool::ComputePool(ComputePoolConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

ComputePool::~ComputePool() noexcept = default;

auto ComputePool::start() -> Result<void> { return impl_->start(); }

auto ComputePool::stop(ComputeShutdownMode mode) noexcept -> void {
  impl_->stop(mode);
}

auto ComputePool::is_running() const noexcept -> bool {
  return impl_->is_running();
}

auto ComputePool::submit(ComputeOptions options, Work work,
                         OnDiscard on_discard)
    -> Result<ComputeTaskHandle> {
  return impl_->submit(std::move(options), std::move(work),
                       std::move(on_discard));
}

auto ComputePool::snapshot() const -> ComputePoolSnapshot {
  return impl_->snapshot();
}

} // namespace dagforge
