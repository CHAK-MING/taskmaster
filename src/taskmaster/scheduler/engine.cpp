#include "taskmaster/scheduler/engine.hpp"

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/scheduler/task.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"

#include <chrono>

namespace taskmaster {

Engine::Engine(Runtime& runtime) : runtime_(&runtime) {
  auto created = io::EventFd::create(0, EFD_NONBLOCK);
  if (!created) {
    log::error("Failed to create engine eventfd: {}", created.error().message());
    return;
  }
  wake_fd_ = std::move(*created);
}

Engine::~Engine() {
  stop();
}

auto Engine::start() -> void {
  if (running_.exchange(true))
    return;

  if (runtime_ == nullptr) {
    log::error("Engine cannot start: Runtime not set");
    running_.store(false);
    return;
  }

  if (!wake_fd_) {
    log::error("Engine cannot start: wake eventfd is not available");
    running_.store(false);
    return;
  }

  stopped_.store(false);
  auto t = run_loop();
  runtime_->schedule_on(0, t.take());
  log::info("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false))
    return;
  events_.push_blocking(ShutdownEvent{});
  notify();

  stopped_.wait(false, std::memory_order_acquire);
}

auto Engine::run_loop() -> spawn_task {
  auto& io_ctx = current_io_context();
  auto wake = io::AsyncFd::borrow(io_ctx, wake_fd_.fd());

  while (running_.load(std::memory_order_relaxed)) {
    process_events();

    if (!running_.load(std::memory_order_relaxed))
      break;

    tick();

    auto next_time = get_next_run_time();
    auto now = std::chrono::system_clock::now();

    auto timeout = std::chrono::milliseconds(60000);
    if (next_time != TimePoint::max()) {
      auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
          next_time - now);
      if (delay.count() < 0) {
        delay = std::chrono::milliseconds{0};
      }
      timeout = delay;
    }

    auto poll_result = co_await wake.async_poll_timeout(POLLIN, timeout);
    if (poll_result.ready) {
      (void)wake_fd_.consume();
    } else if (poll_result.error) {
      log::warn("Engine wake poll error: {}", poll_result.error.message());
    }
  }

  stopped_.store(true, std::memory_order_release);
  stopped_.notify_one();
  log::info("Engine stopped");
  co_return;
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
    }

    if (task_it->second.cron_expr.has_value()) {
      auto next_time = task_it->second.cron_expr->next_after(now);
      schedule_task(id, next_time);
      log::debug("DAG {} rescheduled for next cron time", task_it->second.dag_id);
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

auto Engine::handle_event(const AddTaskEvent& e) -> void {
  auto id = generate_dag_task_id(e.exec_info.dag_id, e.exec_info.task_id);

  if (tasks_.count(id))
    return;

  tasks_.emplace(id, e.exec_info);

  if(e.exec_info.cron_expr.has_value()) {
    auto now = std::chrono::system_clock::now();
    const auto& cron = e.exec_info.cron_expr.value();
    
    TimePoint end_boundary = now;
    if (e.exec_info.end_date.has_value()) {
      end_boundary = std::min(now, *e.exec_info.end_date);
    }

    if (e.exec_info.catchup && e.exec_info.start_date.has_value()) {
      auto backfill_times = cron.all_between(*e.exec_info.start_date, end_boundary);
      
      if (!backfill_times.empty()) {
        log::info("DAG {} catchup: scheduling {} backfill runs", 
                  e.exec_info.dag_id, backfill_times.size());
        
        for (const auto& backfill_time : backfill_times) {
          schedule_.emplace(backfill_time, id);
        }
        
        auto first_it = schedule_.lower_bound(backfill_times.front());
        if (first_it != schedule_.end() && first_it->second == id) {
          task_schedule_[id] = first_it;
        }
      }
    }

    auto next_time = cron.next_after(now);
    
    if (e.exec_info.end_date.has_value() && next_time > *e.exec_info.end_date) {
      if (task_schedule_.find(id) == task_schedule_.end()) {
        log::info("DAG {} not scheduled: next run time exceeds end_date", e.exec_info.dag_id);
        return;
      }
    } else {
      schedule_task(id, next_time);
    }
    
    log::info("DAG : {}, Task added: {}, next scheduled at: {}", e.exec_info.dag_id, e.exec_info.task_id,
              std::chrono::duration_cast<std::chrono::seconds>(
                  next_time.time_since_epoch()).count());
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
