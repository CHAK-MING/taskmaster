#include "taskmaster/engine.hpp"
#include "taskmaster/runtime.hpp"
#include "taskmaster/util.hpp"

#include <cstring>
#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include "taskmaster/log.hpp"

namespace taskmaster {

Engine::Engine() { event_fd_ = eventfd(0, EFD_NONBLOCK); }

Engine::~Engine() {
  stop();
  if (event_fd_ >= 0) {
    close(event_fd_);
  }
}

auto Engine::start() -> void {
  if (running_.exchange(true))
    return;

  Runtime::instance().start();
  run_loop();
  log::info("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false))
    return;
  events_.push(ShutdownEvent{});
  notify();
  log::info("Engine stopped");
}

auto Engine::run_loop() -> spawn_task {
  while (running_.load()) {
    process_events();

    if (!running_.load())
      break;

    tick();

    auto next_time = get_next_run_time();
    auto now = std::chrono::system_clock::now();

    if (next_time == TimePoint::max()) {
      (void)co_await async_poll_timeout(event_fd_, POLLIN,
                                        std::chrono::seconds(60));
    } else {
      auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
          next_time - now);
      if (delay.count() > 0) {
        (void)co_await async_poll_timeout(event_fd_, POLLIN, delay);
      }
    }

    std::uint64_t val;
    while (read(event_fd_, &val, sizeof(val)) > 0) {
      // drain event_fd
    }
  }
}

auto Engine::process_events() -> void {
  while (auto event = events_.try_pop()) {
    std::visit([this](auto &&e) { handle_event(e); }, *event);
  }
}

auto Engine::tick() -> void {
  auto now = std::chrono::system_clock::now();

  while (!schedule_.empty()) {
    auto it = schedule_.begin();
    if (it->first > now)
      break;

    std::string task_id = it->second;
    schedule_.erase(it);
    task_schedule_.erase(task_id);

    auto task_it = tasks_.find(task_id);
    if (task_it == tasks_.end() || !task_it->second.enabled)
      continue;

    TaskInstance inst;
    inst.instance_id = generate_instance_id();
    inst.task_id = task_id;
    inst.state = TaskState::Pending;
    inst.scheduled_at = now;

    instances_.emplace(inst.instance_id, inst);

    if (on_ready_) {
      on_ready_(inst);
    }

    // Schedule next run
    auto next_time = task_it->second.schedule.next_after(now);
    schedule_task(task_id, next_time);
  }
}

auto Engine::get_next_run_time() const -> TimePoint {
  if (schedule_.empty()) {
    return TimePoint::max();
  }
  return schedule_.begin()->first;
}

auto Engine::schedule_task(const std::string &task_id, TimePoint next_time)
    -> void {
  unschedule_task(task_id);
  schedule_.emplace(next_time, task_id);
  task_schedule_[task_id] = next_time;
}

auto Engine::unschedule_task(const std::string &task_id) -> void {
  auto it = task_schedule_.find(task_id);
  if (it == task_schedule_.end())
    return;

  auto range = schedule_.equal_range(it->second);
  for (auto sit = range.first; sit != range.second; ++sit) {
    if (sit->second == task_id) {
      schedule_.erase(sit);
      break;
    }
  }
  task_schedule_.erase(it);
}

auto Engine::notify() -> void {
  std::uint64_t val = 1;
  if (write(event_fd_, &val, sizeof(val)) < 0) {
    log::warn("Failed to write to event_fd: {}", strerror(errno));
  }
}

auto Engine::add_task(TaskDefinition def) -> void {
  events_.push(AddTaskEvent{std::move(def)});
  notify();
}

auto Engine::remove_task(std::string_view task_id) -> void {
  events_.push(RemoveTaskEvent{std::string(task_id)});
  notify();
}

auto Engine::enable_task(std::string_view task_id, bool enabled) -> void {
  events_.push(EnableTaskEvent{std::string(task_id), enabled});
  notify();
}

auto Engine::trigger(std::string_view task_id) -> void {
  events_.push(TriggerTaskEvent{std::string(task_id)});
  notify();
}

auto Engine::task_started(std::string_view instance_id) -> void {
  events_.push(TaskStartedEvent{std::string(instance_id)});
  notify();
}

auto Engine::task_completed(std::string_view instance_id, int exit_code)
    -> void {
  events_.push(TaskCompletedEvent{std::string(instance_id), exit_code});
  notify();
}

auto Engine::task_failed(std::string_view instance_id, std::string_view error)
    -> void {
  events_.push(TaskFailedEvent{std::string(instance_id), std::string(error)});
  notify();
}

auto Engine::set_on_ready_callback(InstanceReadyCallback cb) -> void {
  on_ready_ = std::move(cb);
}

auto Engine::generate_instance_id() const -> std::string {
  return generate_uuid();
}

auto Engine::handle_event(const AddTaskEvent &e) -> void {
  if (tasks_.count(e.def.id))
    return;

  auto task_id = e.def.id;
  tasks_.emplace(task_id, e.def);

  if (e.def.enabled) {
    auto next_time =
        e.def.schedule.next_after(std::chrono::system_clock::now());
    schedule_task(task_id, next_time);
  }
  log::info("Task added: {}", task_id);
}

auto Engine::handle_event(const RemoveTaskEvent &e) -> void {
  unschedule_task(e.task_id);
  tasks_.erase(e.task_id);
  log::info("Task removed: {}", e.task_id);
}

auto Engine::handle_event(const EnableTaskEvent &e) -> void {
  auto it = tasks_.find(e.task_id);
  if (it == tasks_.end())
    return;

  it->second.enabled = e.enabled;
  if (e.enabled) {
    auto next_time =
        it->second.schedule.next_after(std::chrono::system_clock::now());
    schedule_task(e.task_id, next_time);
  } else {
    unschedule_task(e.task_id);
  }
  log::info("Task {} {}", e.task_id, e.enabled ? "enabled" : "disabled");
}

auto Engine::handle_event(const TriggerTaskEvent &e) -> void {
  auto it = tasks_.find(e.task_id);
  if (it == tasks_.end())
    return;

  TaskInstance inst;
  inst.instance_id = generate_instance_id();
  inst.task_id = e.task_id;
  inst.state = TaskState::Pending;
  inst.scheduled_at = std::chrono::system_clock::now();
  instances_.emplace(inst.instance_id, inst);

  log::info("Task {} triggered", e.task_id);
  if (on_ready_)
    on_ready_(inst);
}

auto Engine::handle_event(const TaskStartedEvent &e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  it->second.state = TaskState::Running;
  it->second.started_at = std::chrono::system_clock::now();
  it->second.attempt++;
}

auto Engine::handle_event(const TaskCompletedEvent &e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  it->second.state = TaskState::Success;
  it->second.finished_at = std::chrono::system_clock::now();
  it->second.exit_code = e.exit_code;
}

auto Engine::handle_event(const TaskFailedEvent &e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  auto &inst = it->second;
  inst.error_message = e.error;
  inst.finished_at = std::chrono::system_clock::now();

  auto task_it = tasks_.find(inst.task_id);
  if (task_it == tasks_.end()) {
    inst.state = TaskState::Failed;
    return;
  }

  if (inst.attempt < task_it->second.retry.max_attempts) {
    inst.state = TaskState::Pending;
    inst.started_at = {};
    inst.finished_at = {};
    log::warn("Instance {} failed, retry {}/{}", e.instance_id, inst.attempt,
              task_it->second.retry.max_attempts);
    if (on_ready_)
      on_ready_(inst);
  } else {
    inst.state = TaskState::Failed;
    log::error("Instance {} failed after {} attempts", e.instance_id,
               inst.attempt);
  }
}

auto Engine::handle_event(const TickEvent &) -> void { tick(); }

auto Engine::handle_event(const ShutdownEvent &) -> void {
  running_.store(false);
}

} // namespace taskmaster
