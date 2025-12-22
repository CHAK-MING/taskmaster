#pragma once

#include <atomic>
#include <chrono>
#include <optional>
#include <thread>
#include <variant>

#include "taskmaster/lockfree_queue.hpp"
#include "taskmaster/task.hpp"

namespace taskmaster {

struct AddTaskEvent {
  TaskDefinition def;
};

struct RemoveTaskEvent {
  std::string task_id;
};

struct EnableTaskEvent {
  std::string task_id;
  bool enabled;
};

struct TriggerTaskEvent {
  std::string task_id;
};

struct TaskStartedEvent {
  std::string instance_id;
};

struct TaskCompletedEvent {
  std::string instance_id;
  int exit_code;
};

struct TaskFailedEvent {
  std::string instance_id;
  std::string error;
};

struct TickEvent {
  std::chrono::system_clock::time_point now;
};

struct ShutdownEvent {};

using SchedulerEvent =
    std::variant<AddTaskEvent, RemoveTaskEvent, EnableTaskEvent,
                 TriggerTaskEvent, TaskStartedEvent, TaskCompletedEvent,
                 TaskFailedEvent, TickEvent, ShutdownEvent>;

class EventQueue {
public:
  auto push(SchedulerEvent event) -> void {
    // Push with retry
    while (!queue_.push(std::move(event))) {
      std::this_thread::yield();
    }
    pending_.fetch_add(1, std::memory_order_release);
  }

  [[nodiscard]] auto try_pop() -> std::optional<SchedulerEvent> {
    auto result = queue_.try_pop();
    if (result) {
      pending_.fetch_sub(1, std::memory_order_release);
    }
    return result;
  }

  [[nodiscard]] auto wait_pop() -> SchedulerEvent {
    while (true) {
      if (auto event = try_pop()) {
        return *event;
      }
      // Brief sleep to avoid busy-wait
      if (pending_.load(std::memory_order_acquire) == 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      } else {
        std::this_thread::yield();
      }
    }
  }

  [[nodiscard]] auto empty() const -> bool {
    return pending_.load(std::memory_order_acquire) == 0;
  }

  [[nodiscard]] auto size() const -> std::size_t {
    return pending_.load(std::memory_order_acquire);
  }

private:
  // Queue capacity must be power of 2 for lock-free implementation.
  // 512 is sufficient for most workloads; increase if events are dropped.
  static constexpr std::size_t QUEUE_CAPACITY = 512;
  
  LockFreeQueue<SchedulerEvent> queue_{QUEUE_CAPACITY};
  std::atomic<std::size_t> pending_{0};
};

} // namespace taskmaster
