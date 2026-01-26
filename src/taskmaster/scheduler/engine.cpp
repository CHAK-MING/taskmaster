#include "taskmaster/scheduler/engine.hpp"

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/scheduler/task.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"

#include <poll.h>
#include <cerrno>
#include <cstring>
#include <chrono>

namespace taskmaster {

Engine::Engine(Runtime& /*runtime*/, Persistence* persistence)
    : persistence_(persistence) {
  auto created = io::EventFd::create(0, EFD_NONBLOCK);
  if (!created) {
    log::error("Failed to create engine eventfd: {}", created.error().message());
    return;
  }
  wake_fd_ = std::move(*created);

  auto timer_created = io::TimerFd::create();
  if (!timer_created) {
    log::error("Failed to create engine timerfd: {}", timer_created.error().message());
    return;
  }
  timer_fd_ = std::move(*timer_created);
}

Engine::~Engine() {
  stop();
}

auto Engine::start() -> void {
  if (running_.exchange(true))
    return;

  if (!wake_fd_ || !timer_fd_) {
    log::error("Engine cannot start: wake eventfd or timerfd is not available");
    running_.store(false);
    return;
  }

  stopped_.store(false);
  log::info("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false))
    return;

  if (loop_active_.load(std::memory_order_acquire)) {
    events_.push_blocking(ShutdownEvent{});
    notify();

    stopped_.wait(false, std::memory_order_acquire);
  } else {
    stopped_.store(true, std::memory_order_release);
    stopped_.notify_all();
  }
}

auto Engine::run_loop() -> void {
  loop_active_.store(true, std::memory_order_release);

  struct pollfd pfd[2];
  pfd[0].fd = wake_fd_.fd();
  pfd[0].events = POLLIN;
  pfd[1].fd = timer_fd_.fd();
  pfd[1].events = POLLIN;

  while (running_.load(std::memory_order_relaxed)) {
    process_events();

    if (!running_.load(std::memory_order_relaxed))
      break;

    tick();

    auto next_time = get_next_run_time();
    
    if (next_time != TimePoint::max()) {
      auto now = std::chrono::system_clock::now();
      auto delay = next_time - now;
      if (delay < std::chrono::milliseconds(0)) {
        delay = std::chrono::milliseconds(0);
      }
      
      auto steady_now = std::chrono::steady_clock::now();
      timer_fd_.set_absolute(steady_now + std::chrono::duration_cast<std::chrono::steady_clock::duration>(delay));
    } else {
      timer_fd_.unset();
    }

    int ret = ::poll(pfd, 2, -1);
    if (ret > 0) {
      if (pfd[0].revents & POLLIN) {
        (void)wake_fd_.consume();
      }
      if (pfd[1].revents & POLLIN) {
        (void)timer_fd_.consume();
      }
    } else if (ret < 0 && errno != EINTR) {
      log::warn("Engine wake poll error: {}", std::strerror(errno));
    }
  }

  loop_active_.store(false, std::memory_order_release);
  stopped_.store(true, std::memory_order_release);
  stopped_.notify_one();
  log::info("Engine stopped");
}

auto Engine::process_events() -> void {
  while (auto event = events_.try_pop()) {
    std::visit([this](auto&& e) { handle_event(e); }, *event);
  }
}

auto Engine::tick() -> void {
  auto now = std::chrono::system_clock::now();
  log::debug("Engine tick: schedule_size={}", schedule_.size());

  while (!schedule_.empty()) {
    auto it = schedule_.begin();
    if (it->first > now)
      break;

    DAGTaskId id = it->second;
    schedule_.erase(it);
    task_schedule_.erase(id);

    auto task_it = tasks_.find(id);
    if (task_it == tasks_.end())
      continue;

    if (on_dag_trigger_) {
      auto execution_date = it->first;
      log::info("Cron triggered DAG: {} for execution_date: {}", task_it->second.dag_id,
                std::chrono::duration_cast<std::chrono::seconds>(
                    execution_date.time_since_epoch()).count());
      on_dag_trigger_(task_it->second.dag_id, execution_date);

      if (persistence_) {
        if (auto r = persistence_->save_watermark(task_it->second.dag_id, execution_date);
            !r.has_value()) {
          log::error("Failed to save watermark for DAG {}: {}",
                     task_it->second.dag_id, r.error().message());
        }
      }
    }

    if (task_it->second.cron_expr.has_value()) {
      auto next_time = task_it->second.cron_expr->next_after(now);
      
      if (task_it->second.end_date.has_value() && next_time > *task_it->second.end_date) {
        log::info("DAG {} finished: next run time exceeds end_date", task_it->second.dag_id);
      } else {
        schedule_task(id, next_time);
        log::debug("DAG {} rescheduled for next cron time", task_it->second.dag_id);
      }
    }
  }
}

auto Engine::get_next_run_time() const -> TimePoint {
  if (schedule_.empty()) {
    return TimePoint::max();
  }
  return schedule_.begin()->first;
}

auto Engine::schedule_task(DAGTaskId dag_task_id, TimePoint next_time)
    -> void {
  unschedule_task(dag_task_id);
  auto it = schedule_.emplace(next_time, dag_task_id);
  task_schedule_[dag_task_id] = it;
}

auto Engine::unschedule_task(DAGTaskId dag_task_id) -> void {
  auto it = task_schedule_.find(dag_task_id);
  if (it == task_schedule_.end())
    return;

  if (it->second != schedule_.end()) {
    schedule_.erase(it->second);
  }
  task_schedule_.erase(it);
}

auto Engine::notify() -> void {
  if (!wake_fd_) {
    return;
  }
  if (!wake_fd_.signal()) {
    log::warn("Failed to signal engine wake eventfd");
  }
}

auto Engine::add_task(ExecutionInfo exec_info) -> bool {
  if (!events_.push(AddTaskEvent{std::move(exec_info)})) {
    log::warn("Event queue full when adding task");
    return false;
  }
  notify();
  return true;
}

auto Engine::remove_task(DAGId dag_id, TaskId task_id) -> bool {
  if (!events_.push(RemoveTaskEvent{dag_id, task_id})) {
    log::warn("Event queue full when removing task {}", task_id);
    return false;
  }
  notify();
  return true;
}

auto Engine::set_on_dag_trigger(DAGTriggerCallback cb) -> void {
  on_dag_trigger_ = std::move(cb);
}

auto Engine::set_check_exists_callback(CheckExistsCallback cb) -> void {
  check_exists_ = std::move(cb);
}

auto Engine::handle_event(const AddTaskEvent& e) -> void {
  auto id = generate_dag_task_id(e.exec_info.dag_id, e.exec_info.task_id);

  if (tasks_.count(id))
    return;

  tasks_.emplace(id, e.exec_info);

  if (e.exec_info.cron_expr.has_value()) {
    auto now = std::chrono::system_clock::now();
    const auto& cron = e.exec_info.cron_expr.value();

    TimePoint baseline_time = now;
    if (e.exec_info.start_date.has_value()) {
      baseline_time = *e.exec_info.start_date;
    }

    if (persistence_) {
      auto watermark = persistence_->get_watermark(e.exec_info.dag_id);
      if (watermark.has_value() && watermark.value().has_value()) {
        baseline_time = *watermark.value();
      }
    }

    TimePoint effective_baseline = baseline_time;

    if (e.exec_info.catchup) {
      auto next_run = cron.next_after(effective_baseline);
      while (next_run <= now) {
        if (e.exec_info.end_date.has_value() &&
            next_run > *e.exec_info.end_date) {
          break;
        }

        bool exists = false;
        if (persistence_) {
          exists = persistence_->run_exists(e.exec_info.dag_id, next_run);
        } else if (check_exists_) {
          exists = check_exists_(e.exec_info.dag_id, next_run);
        }

        if (exists) {
          // Already exists
        } else {
          // Trigger catchup run immediately without scheduling to avoid
          // rescheduling logic in tick()
          if (on_dag_trigger_) {
            log::info("Catchup triggering DAG: {} for execution_date: {}",
                      e.exec_info.dag_id,
                      std::chrono::duration_cast<std::chrono::seconds>(
                          next_run.time_since_epoch())
                          .count());
            on_dag_trigger_(e.exec_info.dag_id, next_run);

            if (persistence_) {
              if (auto r = persistence_->save_watermark(e.exec_info.dag_id,
                                                        next_run);
                  !r.has_value()) {
                log::error("Failed to save watermark for DAG {}: {}",
                           e.exec_info.dag_id, r.error().message());
              }
            }
          }
        }

        effective_baseline = next_run;
        next_run = cron.next_after(effective_baseline);
      }
    }

    auto next_time = cron.next_after(std::max(effective_baseline, now));

    if (e.exec_info.end_date.has_value() && next_time > *e.exec_info.end_date) {
      if (task_schedule_.find(id) == task_schedule_.end()) {
        log::info("DAG {} not scheduled: next run time exceeds end_date",
                  e.exec_info.dag_id);
        return;
      }
    } else {
      schedule_task(id, next_time);
    }

    log::info("DAG : {}, Task added: {}, next scheduled at: {}",
              e.exec_info.dag_id, e.exec_info.task_id,
              std::chrono::duration_cast<std::chrono::seconds>(
                  next_time.time_since_epoch())
                  .count());
  }

}

auto Engine::handle_event(const RemoveTaskEvent& e) -> void {
  auto id = generate_dag_task_id(e.dag_id, e.task_id);
  unschedule_task(id);
  tasks_.erase(id);
  log::info("DAG: {}, Task removed: {}", e.dag_id, e.task_id);
}

auto Engine::handle_event(const ShutdownEvent&) -> void {
  running_.store(false);
}

}  // namespace taskmaster
