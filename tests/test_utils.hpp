#pragma once

#include "taskmaster/util/id.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace taskmaster::test {

[[nodiscard]] inline auto dag_id(const char* s) -> DAGId {
  return DAGId{std::string{s}};
}

[[nodiscard]] inline auto dag_id(std::string_view s) -> DAGId {
  return DAGId{std::string{s}};
}

[[nodiscard]] inline auto dag_id(std::string s) -> DAGId {
  return DAGId{std::move(s)};
}

[[nodiscard]] inline auto task_id(const char* s) -> TaskId {
  return TaskId{std::string{s}};
}

[[nodiscard]] inline auto task_id(std::string_view s) -> TaskId {
  return TaskId{std::string{s}};
}

[[nodiscard]] inline auto task_id(std::string s) -> TaskId {
  return TaskId{std::move(s)};
}

[[nodiscard]] inline auto dag_run_id(const char* s) -> DAGRunId {
  return DAGRunId{std::string{s}};
}

[[nodiscard]] inline auto dag_run_id(std::string_view s) -> DAGRunId {
  return DAGRunId{std::string{s}};
}

[[nodiscard]] inline auto dag_run_id(std::string s) -> DAGRunId {
  return DAGRunId{std::move(s)};
}

template <typename T>
class BlockingQueue {
public:
  void push(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(std::move(value));
    cv_.notify_one();
  }

  [[nodiscard]] auto pop() -> T {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return !queue_.empty(); });
    T value = std::move(queue_.front());
    queue_.pop();
    return value;
  }

  template <typename Rep, typename Period>
  [[nodiscard]] auto try_pop_for(const std::chrono::duration<Rep, Period>& timeout)
      -> std::optional<T> {
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) {
      T value = std::move(queue_.front());
      queue_.pop();
      return value;
    }
    return std::nullopt;
  }

private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

inline void busy_wait_for(std::chrono::milliseconds duration) {
  auto start = std::chrono::steady_clock::now();
  while (std::chrono::steady_clock::now() - start < duration) {
    std::this_thread::yield();
  }
}

inline void sleep_ms(std::chrono::milliseconds ms) {
  std::this_thread::sleep_for(ms);
}

class SimpleBarrier {
public:
  explicit SimpleBarrier(std::size_t count)
      : count_(count), waiting_(0), generation_(0) {
  }

  void arrive_and_wait() {
    std::size_t my_generation = generation_.load(std::memory_order_acquire);

    std::size_t arrived = waiting_.fetch_add(1, std::memory_order_acq_rel) + 1;

    if (arrived == count_) {
      waiting_.store(0, std::memory_order_release);
      generation_.fetch_add(1, std::memory_order_acq_rel);
      cv_.notify_all();
      return;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this, my_generation] {
      return generation_.load(std::memory_order_acquire) != my_generation;
    });
  }

private:
  std::size_t count_;
  std::atomic<std::size_t> waiting_;
  std::atomic<std::size_t> generation_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

using Barrier = SimpleBarrier;

}
