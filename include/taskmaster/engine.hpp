#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <string_view>
#include <unordered_map>

#include "taskmaster/coroutine.hpp"
#include "taskmaster/event_queue.hpp"
#include "taskmaster/task.hpp"

namespace taskmaster {

class Engine {
public:
  using InstanceReadyCallback =
      std::move_only_function<void(const TaskInstance &)>;
  using TimePoint = std::chrono::system_clock::time_point;

  Engine();
  ~Engine();

  Engine(const Engine &) = delete;
  auto operator=(const Engine &) -> Engine & = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load();
  }

  auto add_task(TaskDefinition def) -> void;
  auto remove_task(std::string_view task_id) -> void;
  auto enable_task(std::string_view task_id, bool enabled) -> void;
  auto trigger(std::string_view task_id) -> void;
  auto task_started(std::string_view instance_id) -> void;
  auto task_completed(std::string_view instance_id, int exit_code) -> void;
  auto task_failed(std::string_view instance_id, std::string_view error)
      -> void;

  auto set_on_ready_callback(InstanceReadyCallback cb) -> void;

private:
  auto run_loop() -> spawn_task;
  auto process_events() -> void;
  auto tick() -> void;
  auto get_next_run_time() const -> TimePoint;
  auto notify() -> void;
  auto schedule_task(const std::string &task_id, TimePoint next_time) -> void;
  auto unschedule_task(const std::string &task_id) -> void;

  auto handle_event(const AddTaskEvent &e) -> void;
  auto handle_event(const RemoveTaskEvent &e) -> void;
  auto handle_event(const EnableTaskEvent &e) -> void;
  auto handle_event(const TriggerTaskEvent &e) -> void;
  auto handle_event(const TaskStartedEvent &e) -> void;
  auto handle_event(const TaskCompletedEvent &e) -> void;
  auto handle_event(const TaskFailedEvent &e) -> void;
  auto handle_event(const TickEvent &e) -> void;
  auto handle_event(const ShutdownEvent &e) -> void;

  [[nodiscard]] auto generate_instance_id() const -> std::string;

  std::atomic<bool> running_{false};
  int event_fd_{-1};
  EventQueue events_;

  std::unordered_map<std::string, TaskDefinition> tasks_;
  std::unordered_map<std::string, TaskInstance> instances_;

  // O(1) access to next scheduled task using multimap (sorted by time)
  std::multimap<TimePoint, std::string> schedule_;
  // Track each task's position in schedule_ for O(log N) removal
  std::unordered_map<std::string, TimePoint> task_schedule_;

  InstanceReadyCallback on_ready_;
};

} // namespace taskmaster
