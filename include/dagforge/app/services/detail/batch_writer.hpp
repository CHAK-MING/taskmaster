#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/metrics.hpp"
#endif

#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace dagforge::detail {

template <typename Request> class BatchWriter {
public:
  using RequestPtr = std::shared_ptr<Request>;
  using Queue = boost::asio::experimental::concurrent_channel<
      boost::asio::any_io_executor,
      void(boost::system::error_code, RequestPtr)>;

  struct Config {
    std::size_t capacity{0};
    std::size_t max_batch_size{64};
    std::chrono::milliseconds max_linger{50};
  };

  struct Batch {
    std::vector<RequestPtr> requests;
    std::chrono::steady_clock::time_point first_enqueued_at{};
  };

  BatchWriter(boost::asio::any_io_executor executor, Config config)
      : queue_(executor, config.capacity), config_(config) {}

  auto start() -> bool {
    accepting_.store(true, std::memory_order_release);
    return !running_.exchange(true, std::memory_order_acq_rel);
  }

  auto stop(bool drain = true) -> void {
    accepting_.store(false, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    if (drain) {
      queue_.close();
    } else {
      queue_.cancel();
    }
  }

  [[nodiscard]] auto running() const noexcept -> bool {
    return running_.load(std::memory_order_acquire);
  }

  auto try_enqueue(const RequestPtr &request) -> bool {
    if (!accepting_.load(std::memory_order_acquire)) {
      rejected_total_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    if (!queue_.try_send(boost::system::error_code{}, request)) {
      rejected_total_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }

    requests_total_.fetch_add(1, std::memory_order_relaxed);
    queue_depth_.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  auto enqueue(const RequestPtr &request) -> task<Result<void>> {
    if (!accepting_.load(std::memory_order_acquire)) {
      co_return fail(Error::DatabaseQueryFailed,
                     "Batch writer is not accepting new requests");
    }
    if (try_enqueue(request)) {
      co_return ok();
    }

    auto send_res = co_await co_as_result(
        queue_.async_send(boost::system::error_code{}, request, use_nothrow));
    if (!send_res) {
      co_return fail(Error::DatabaseQueryFailed,
                     std::format("Failed to enqueue batch request: {}",
                                 send_res.error().message()));
    }

    requests_total_.fetch_add(1, std::memory_order_relaxed);
    queue_depth_.fetch_add(1, std::memory_order_relaxed);
    co_return ok();
  }

  auto next_batch() -> task<Result<Batch>> {
    auto executor = co_await boost::asio::this_coro::executor;
    boost::asio::steady_timer timer(executor);
    using namespace boost::asio::experimental::awaitable_operators;

    auto first_res = co_await co_as_result(queue_.async_receive(use_nothrow));
    if (!first_res) {
      co_return ok(Batch{});
    }
    auto first = std::move(*first_res);
    if (!first) {
      co_return ok(Batch{});
    }
    queue_depth_.fetch_sub(1, std::memory_order_relaxed);
    auto first_enqueued_at = std::chrono::steady_clock::now();

    Batch batch;
    batch.requests.reserve(config_.max_batch_size);
    batch.requests.push_back(std::move(first));
    batch.first_enqueued_at = first_enqueued_at;

    // Drain any immediately available items first before considering linger
    while (batch.requests.size() < config_.max_batch_size) {
      bool drained = false;
      queue_.try_receive([&](boost::system::error_code ec, RequestPtr req) {
        if (ec || !req) {
          return;
        }
        queue_depth_.fetch_sub(1, std::memory_order_relaxed);
        batch.requests.push_back(std::move(req));
        drained = true;
      });
      if (!drained) {
        break;
      }
    }

    // Skip linger if batch is already reasonably full
    if (batch.requests.size() >= config_.max_batch_size / 2) {
      co_return ok(std::move(batch));
    }

    // Low-load fast path: if the queue drained completely, do not inject the
    // linger delay. This preserves latency for linear / low-concurrency runs.
    if (queue_depth_.load(std::memory_order_acquire) == 0) {
      co_return ok(std::move(batch));
    }

    // Backlog-aware linger: wait for either more work or the timeout, then
    // drain whatever accumulated. This keeps batching for fan-in bursts while
    // remaining responsive under load.
    const auto linger_deadline = std::chrono::steady_clock::now() + config_.max_linger;
    while (accepting_.load(std::memory_order_acquire) &&
           batch.requests.size() < config_.max_batch_size) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= linger_deadline) {
        break;
      }
      timer.expires_at(linger_deadline);
      auto outcome = co_await (
          queue_.async_receive(use_nothrow) || timer.async_wait(use_nothrow));

      if (outcome.index() != 0) {
        break;
      }

      auto receive_res = as_result(std::move(std::get<0>(outcome)));
      if (!receive_res || !*receive_res) {
        break;
      }

      queue_depth_.fetch_sub(1, std::memory_order_relaxed);
      batch.requests.push_back(std::move(*receive_res));

      while (batch.requests.size() < config_.max_batch_size) {
        bool drained = false;
        queue_.try_receive([&](boost::system::error_code ec, RequestPtr req) {
          if (ec || !req) {
            return;
          }
          queue_depth_.fetch_sub(1, std::memory_order_relaxed);
          batch.requests.push_back(std::move(req));
          drained = true;
        });
        if (!drained) {
          break;
        }
      }
    }

    co_return ok(std::move(batch));
  }

  auto note_flush(std::size_t batch_size,
                  std::chrono::steady_clock::time_point first_enqueued_at,
                  std::chrono::steady_clock::time_point flush_started_at,
                  std::chrono::steady_clock::time_point commit_done,
                  metrics::Histogram *flush_histogram = nullptr) -> void {
    last_size_.store(batch_size, std::memory_order_relaxed);
    last_linger_us_.store(
        std::chrono::duration_cast<std::chrono::microseconds>(flush_started_at -
                                                              first_enqueued_at)
            .count(),
        std::memory_order_relaxed);

    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(commit_done -
                                                             flush_started_at)
            .count();
    if (flush_histogram != nullptr) {
      flush_histogram->observe_ns(
          static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
    }
    last_flush_ms_.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(commit_done -
                                                              flush_started_at)
            .count(),
        std::memory_order_relaxed);
  }

  auto note_commit() -> void {
    commits_total_.fetch_add(1, std::memory_order_relaxed);
  }

  auto note_fallback(std::size_t count = 1) -> void {
    fallback_total_.fetch_add(count, std::memory_order_relaxed);
  }

  auto note_reply_wakeup(
      std::chrono::steady_clock::time_point commit_done) -> void {
    wakeup_lag_us_.store(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - commit_done)
            .count(),
        std::memory_order_relaxed);
  }

  auto note_acquire_failure() -> void {
    writer_acquire_failures_total_.fetch_add(1, std::memory_order_relaxed);
  }

  [[nodiscard]] auto queue_depth() const noexcept -> std::size_t {
    return queue_depth_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto last_size() const noexcept -> std::size_t {
    return last_size_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto last_linger_us() const noexcept -> std::uint64_t {
    return last_linger_us_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto last_flush_ms() const noexcept -> std::uint64_t {
    return last_flush_ms_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto requests_total() const noexcept -> std::uint64_t {
    return requests_total_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto commits_total() const noexcept -> std::uint64_t {
    return commits_total_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto fallback_total() const noexcept -> std::uint64_t {
    return fallback_total_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto rejected_total() const noexcept -> std::uint64_t {
    return rejected_total_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto wakeup_lag_us() const noexcept -> std::uint64_t {
    return wakeup_lag_us_.load(std::memory_order_relaxed);
  }
  [[nodiscard]] auto writer_acquire_failures_total() const noexcept
      -> std::uint64_t {
    return writer_acquire_failures_total_.load(std::memory_order_relaxed);
  }

private:
  Queue queue_;
  Config config_;
  std::atomic<bool> accepting_{false};
  std::atomic<bool> running_{false};
  std::atomic<std::size_t> queue_depth_{0};
  std::atomic<std::size_t> last_size_{0};
  std::atomic<std::uint64_t> last_linger_us_{0};
  std::atomic<std::uint64_t> last_flush_ms_{0};
  std::atomic<std::uint64_t> requests_total_{0};
  std::atomic<std::uint64_t> commits_total_{0};
  std::atomic<std::uint64_t> fallback_total_{0};
  std::atomic<std::uint64_t> rejected_total_{0};
  std::atomic<std::uint64_t> wakeup_lag_us_{0};
  std::atomic<std::uint64_t> writer_acquire_failures_total_{0};
};

} // namespace dagforge::detail
