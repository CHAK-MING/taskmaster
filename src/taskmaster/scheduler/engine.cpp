#include "taskmaster/scheduler/engine.hpp"

#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"

#include <sys/eventfd.h>

#include <cerrno>
#include <cstring>

#include <poll.h>
#include <unistd.h>

namespace taskmaster {

Engine::Engine() {
  event_fd_ = eventfd(0, EFD_NONBLOCK);
  if (event_fd_ < 0) {
    log::error("Failed to create eventfd: {}", std::strerror(errno));
  }
}

Engine::~Engine() {
  stop();
  if (event_fd_ >= 0) {
    close(event_fd_);
    event_fd_ = -1;
  }
}

auto Engine::start() -> void {
  if (running_.exchange(true))
    return;

  event_loop_thread_ = std::thread([this] { run_loop(); });
  log::info("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false))
    return;
  // Use blocking push for shutdown - this must succeed
  events_.push_blocking(ShutdownEvent{});
  notify();
  if (event_loop_thread_.joinable()) {
    event_loop_thread_.join();
  }
  log::info("Engine stopped");
}

auto Engine::run_loop() -> void {
  pollfd pfd{event_fd_, POLLIN, 0};

  while (running_.load(std::memory_order_relaxed)) {
    process_events();

    if (!running_.load(std::memory_order_relaxed))
      break;

    tick();

    auto next_time = get_next_run_time();
    auto now = std::chrono::system_clock::now();

    int timeout_ms = 60000;  // Default 60s
    if (next_time != TimePoint::max()) {
      auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
          next_time - now);
      timeout_ms = std::max(0, static_cast<int>(delay.count()));
    }

    int ret = ::poll(&pfd, 1, timeout_ms);
    if (ret < 0 && errno != EINTR) {
      log::error("poll failed: {}", strerror(errno));
      break;
    }

    // Drain eventfd
    std::uint64_t val;
    while (::read(event_fd_, &val, sizeof(val)) > 0) {
    }
  }
}

auto Engine::process_events() -> void {
  while (auto event = events_.try_pop()) {
    std::visit([this](auto&& e) { handle_event(e); }, *event);
  }
}

auto Engine::tick() -> void {
  auto now = std::chrono::system_clock::now();

  while (!retry_schedule_.empty()) {
    auto it = retry_schedule_.begin();
    if (it->first > now)
      break;

    std::string instance_id = it->second;
    retry_schedule_.erase(it);

    auto inst_it = instances_.find(instance_id);
    if (inst_it == instances_.end() ||
        inst_it->second.state != TaskState::Retrying)
      continue;

    auto& inst = inst_it->second;
    inst.state = TaskState::Pending;
    inst.started_at = {};
    inst.finished_at = {};

    if (on_ready_) {
      on_ready_(inst);
    }
  }

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
  TimePoint next_schedule =
      schedule_.empty() ? TimePoint::max() : schedule_.begin()->first;
  TimePoint next_retry = retry_schedule_.empty()
                             ? TimePoint::max()
                             : retry_schedule_.begin()->first;
  return std::min(next_schedule, next_retry);
}

auto Engine::schedule_task(const std::string& task_id, TimePoint next_time)
    -> void {
  unschedule_task(task_id);
  auto it = schedule_.emplace(next_time, task_id);
  task_schedule_[task_id] = it;
}

auto Engine::unschedule_task(const std::string& task_id) -> void {
  auto it = task_schedule_.find(task_id);
  if (it == task_schedule_.end())
    return;

  if (it->second != schedule_.end()) {
    schedule_.erase(it->second);
  }
  task_schedule_.erase(it);
}

auto Engine::notify() -> void {
  std::uint64_t val = 1;
  if (write(event_fd_, &val, sizeof(val)) < 0) {
    log::warn("Failed to write to event_fd: {}", strerror(errno));
  }
}

auto Engine::add_task(TaskDefinition def) -> bool {
  if (!events_.push(AddTaskEvent{std::move(def)})) {
    log::warn("Event queue full when adding task");
    return false;
  }
  notify();
  return true;
}

auto Engine::remove_task(std::string_view task_id) -> bool {
  if (!events_.push(RemoveTaskEvent{std::string(task_id)})) {
    log::warn("Event queue full when removing task {}", task_id);
    return false;
  }
  notify();
  return true;
}

auto Engine::enable_task(std::string_view task_id, bool enabled) -> bool {
  if (!events_.push(EnableTaskEvent{std::string(task_id), enabled})) {
    return false;
  }
  notify();
  return true;
}

auto Engine::trigger(std::string_view task_id) -> bool {
  if (!events_.push(TriggerTaskEvent{std::string(task_id)})) {
    log::warn("Event queue full when triggering task {}", task_id);
    return false;
  }
  notify();
  return true;
}

auto Engine::task_started(std::string_view instance_id) -> void {
  events_.push_blocking(TaskStartedEvent{std::string(instance_id)});
  notify();
}

auto Engine::task_completed(std::string_view instance_id, int exit_code)
    -> void {
  events_.push_blocking(
      TaskCompletedEvent{std::string(instance_id), exit_code});
  notify();
}

auto Engine::task_failed(std::string_view instance_id, std::string_view error)
    -> void {
  events_.push_blocking(
      TaskFailedEvent{std::string(instance_id), std::string(error)});
  notify();
}

auto Engine::set_on_ready_callback(InstanceReadyCallback cb) -> void {
  on_ready_ = std::move(cb);
}

auto Engine::generate_instance_id() const -> std::string {
  return generate_uuid();
}

auto Engine::handle_event(const AddTaskEvent& e) -> void {
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

auto Engine::handle_event(const RemoveTaskEvent& e) -> void {
  unschedule_task(e.task_id);
  tasks_.erase(e.task_id);
  log::info("Task removed: {}", e.task_id);
}

auto Engine::handle_event(const EnableTaskEvent& e) -> void {
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

auto Engine::handle_event(const TriggerTaskEvent& e) -> void {
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

auto Engine::handle_event(const TaskStartedEvent& e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  it->second.state = TaskState::Running;
  it->second.started_at = std::chrono::system_clock::now();
  it->second.attempt++;
}

auto Engine::handle_event(const TaskCompletedEvent& e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  it->second.state = TaskState::Success;
  it->second.finished_at = std::chrono::system_clock::now();
  it->second.exit_code = e.exit_code;

  // Reschedule task for next cron time if enabled
  auto task_it = tasks_.find(it->second.task_id);
  if (task_it != tasks_.end() && task_it->second.enabled) {
    auto next_time =
        task_it->second.schedule.next_after(std::chrono::system_clock::now());
    schedule_task(it->second.task_id, next_time);
    log::debug("Task {} rescheduled for next cron time", it->second.task_id);
  }
}

auto Engine::handle_event(const TaskFailedEvent& e) -> void {
  auto it = instances_.find(e.instance_id);
  if (it == instances_.end())
    return;

  auto& inst = it->second;
  inst.error_message = e.error;
  inst.finished_at = std::chrono::system_clock::now();

  auto task_it = tasks_.find(inst.task_id);
  if (task_it == tasks_.end()) {
    inst.state = TaskState::Failed;
    return;
  }

  if (inst.attempt < task_it->second.retry.max_attempts) {
    auto retry_delay = task_it->second.retry.delay;

    if (retry_delay.count() > 0) {
      inst.state = TaskState::Retrying;
      log::warn("Instance {} failed, will retry {}/{} after {} seconds",
                e.instance_id, inst.attempt, task_it->second.retry.max_attempts,
                retry_delay.count());

      auto retry_time = std::chrono::system_clock::now() + retry_delay;
      retry_schedule_.emplace(retry_time, e.instance_id);
    } else {
      inst.state = TaskState::Pending;
      inst.started_at = {};
      inst.finished_at = {};
      log::warn("Instance {} failed, retrying immediately {}/{}", e.instance_id,
                inst.attempt, task_it->second.retry.max_attempts);
      if (on_ready_)
        on_ready_(inst);
    }
  } else {
    inst.state = TaskState::Failed;
    log::error("Instance {} failed after {} attempts", e.instance_id,
               inst.attempt);

    // Reschedule task for next cron time if enabled (even after failure)
    if (task_it->second.enabled) {
      auto next_time =
          task_it->second.schedule.next_after(std::chrono::system_clock::now());
      schedule_task(inst.task_id, next_time);
      log::debug("Task {} rescheduled for next cron time after failure",
                 inst.task_id);
    }
  }
}

auto Engine::handle_event(const TickEvent&) -> void {
  tick();
}

auto Engine::handle_event(const ShutdownEvent&) -> void {
  running_.store(false);
}

}  // namespace taskmaster
