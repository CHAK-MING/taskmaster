#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/lockfree_queue.hpp"
#include "taskmaster/io/stream.hpp"
#include "taskmaster/scheduler/event_queue.hpp"
#include "taskmaster/scheduler/task.hpp"
#include "taskmaster/util/id.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <string_view>
#include <unordered_map>

namespace taskmaster {

class Runtime;

// Single-threaded event loop scheduler.
// All state mutations happen on the Engine shard in Runtime.
// External calls communicate via lock-free event queue.
class Engine {
public:
  using TimePoint = std::chrono::system_clock::time_point;
  using DAGTriggerCallback =
      std::move_only_function<void(const DAGId&, TimePoint execution_date)>;
  using CheckExistsCallback =
      std::move_only_function<bool(const DAGId&, TimePoint execution_date)>;

  explicit Engine(Runtime& runtime);
  ~Engine();

  Engine(const Engine&) = delete;
  auto operator=(const Engine&) -> Engine& = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load();
  }

  [[nodiscard]] auto add_task(ExecutionInfo exec_info) -> bool;
  [[nodiscard]] auto remove_task(DAGId dag_id, TaskId task_id) -> bool;

  auto set_on_dag_trigger(DAGTriggerCallback cb) -> void;
  auto set_check_exists_callback(CheckExistsCallback cb) -> void;

private:
  auto run_loop() -> spawn_task;
  auto process_events() -> void;
  auto tick() -> void;
  auto get_next_run_time() const -> TimePoint;
  auto notify() -> void;
  auto schedule_task(DAGTaskId dag_task_id, TimePoint next_time) -> void;
  auto unschedule_task(DAGTaskId dag_task_id) -> void;

  auto handle_event(const AddTaskEvent& e) -> void;
  auto handle_event(const RemoveTaskEvent& e) -> void;
  auto handle_event(const ShutdownEvent& e) -> void;

  alignas(kCacheLineSize) std::atomic<bool> running_{false};
  alignas(kCacheLineSize) std::atomic<bool> stopped_{true};
  Runtime* runtime_{nullptr};
  io::EventFd wake_fd_;
  EventQueue events_;

  // Accessed only in event loop thread
  std::unordered_map<DAGTaskId, ExecutionInfo> tasks_;
  std::multimap<TimePoint, DAGTaskId> schedule_;
  std::unordered_map<DAGTaskId, std::multimap<TimePoint, DAGTaskId>::iterator>
      task_schedule_;

  DAGTriggerCallback on_dag_trigger_;
  CheckExistsCallback check_exists_;
};

}  // namespace taskmaster
