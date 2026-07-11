#include "dagforge/scheduler/engine.hpp"

#include "dagforge/core/runtime.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <future>
#include <ranges>
#include <system_error>
#include <unordered_set>
#include <utility>

namespace dagforge {

Engine::Engine(Runtime &runtime, shard_id owner_shard)
    : runtime_(runtime), owner_shard_(owner_shard) {}

Engine::~Engine() { stop(); }

auto Engine::start() -> void {
  if (running_.exchange(true)) {
    return;
  }

  log::debug("Engine started");
}

auto Engine::stop() -> void {
  if (!running_.exchange(false)) {
    return;
  }

  if (!runtime_.is_running() || (runtime_.is_current_shard() &&
                                 runtime_.current_shard() == owner_shard_)) {
    stop_on_owner();
    log::debug("Engine stopped");
    return;
  }

  std::promise<void> completed;
  auto completion = completed.get_future();
  runtime_.post_to(owner_shard_,
                   [this, completed = std::move(completed)]() mutable {
                     stop_on_owner();
                     completed.set_value();
                   });
  completion.get();

  log::debug("Engine stopped");
}

auto Engine::stop_on_owner() -> void {
  for (const auto &[id, scheduled] : scheduled_tasks_) {
    (void)id;
    runtime_.cancel_after_on(owner_shard_, scheduled.handle);
  }
  scheduled_tasks_.clear();
  scheduled_task_count_snapshot_.store(0, std::memory_order_relaxed);
}

auto Engine::run_cron_task(DAGTaskId dag_task_id, TimePoint execution_date,
                           std::uint64_t generation)
    -> boost::asio::awaitable<void> {
  if (!running_.load(std::memory_order_acquire)) {
    co_return;
  }

  auto task_it = tasks_.find(dag_task_id);
  auto scheduled_it = scheduled_tasks_.find(dag_task_id);
  if (task_it == tasks_.end() || scheduled_it == scheduled_tasks_.end() ||
      scheduled_it->second.generation != generation) {
    co_return;
  }

  auto dag_id = task_it->second.dag_id.clone();
  if (on_dag_trigger_) {
    log::debug("Cron triggered DAG: {} for execution_date: {}", dag_id,
               std::chrono::duration_cast<std::chrono::seconds>(
                   execution_date.time_since_epoch())
                   .count());
    on_dag_trigger_(dag_id, execution_date);

    if (save_watermark_) {
      auto wm_res = co_await save_watermark_(dag_id, execution_date);
      wm_res.or_else([&](std::error_code err) -> Result<void> {
        log::error("Failed to save watermark for DAG {}: {}", dag_id,
                   err.message());
        return fail(err);
      });
    }
  }

  if (!running_.load(std::memory_order_acquire)) {
    co_return;
  }

  task_it = tasks_.find(dag_task_id);
  scheduled_it = scheduled_tasks_.find(dag_task_id);
  if (task_it == tasks_.end() || scheduled_it == scheduled_tasks_.end() ||
      scheduled_it->second.generation != generation) {
    co_return;
  }

  const auto &task = task_it->second;
  if (!task.cron_expr.has_value()) {
    unschedule_task(dag_task_id);
    co_return;
  }

  auto next_run = task.cron_expr->next_after(execution_date);
  if (task.end_date.has_value() && next_run > *task.end_date) {
    log::debug("DAG {} finished: next run time exceeds end_date", task.dag_id);
    unschedule_task(dag_task_id);
    co_return;
  }

  schedule_task(dag_task_id, next_run);
}

auto Engine::schedule_task(const DAGTaskId &dag_task_id, TimePoint next_time)
    -> void {
  unschedule_task(dag_task_id);

  const auto now = std::chrono::system_clock::now();
  const auto delay =
      next_time > now ? next_time - now : TimePoint::duration::zero();
  const auto generation = next_schedule_generation_++;
  auto callback_id = dag_task_id.clone();
  auto handle = runtime_.schedule_after_on(
      owner_shard_, delay,
      [this, dag_task_id = std::move(callback_id), next_time,
       generation]() mutable {
        runtime_.spawn_on(owner_shard_, run_cron_task(std::move(dag_task_id),
                                                      next_time, generation));
      });

  scheduled_tasks_.insert_or_assign(
      dag_task_id, ScheduledTask{.handle = handle, .generation = generation});
  scheduled_task_count_snapshot_.store(scheduled_tasks_.size(),
                                       std::memory_order_relaxed);
}

auto Engine::unschedule_task(const DAGTaskId &dag_task_id) -> void {
  auto it = scheduled_tasks_.find(dag_task_id);
  if (it == scheduled_tasks_.end()) {
    return;
  }

  runtime_.cancel_after_on(owner_shard_, it->second.handle);
  scheduled_tasks_.erase(it);
  scheduled_task_count_snapshot_.store(scheduled_tasks_.size(),
                                       std::memory_order_relaxed);
}

auto Engine::add_task(ExecutionInfo exec_info) -> Result<void> {
  if (!running_.load(std::memory_order_acquire)) {
    return fail(Error::SystemNotRunning);
  }

  runtime_.spawn_on(owner_shard_, add_task_on_owner(std::move(exec_info)));
  return ok();
}

auto Engine::remove_task(DAGId dag_id, TaskId task_id) -> Result<void> {
  if (!running_.load(std::memory_order_acquire)) {
    return fail(Error::SystemNotRunning);
  }

  runtime_.post_to(owner_shard_, [this, dag_id = std::move(dag_id),
                                  task_id = std::move(task_id)]() mutable {
    remove_task_on_owner(std::move(dag_id), std::move(task_id));
  });
  return ok();
}

auto Engine::set_on_dag_trigger(DAGTriggerCallback cb) -> void {
  on_dag_trigger_ = std::move(cb);
}

auto Engine::set_run_exists_callback(RunExistsCallback cb) -> void {
  run_exists_ = std::move(cb);
}

auto Engine::set_list_run_execution_dates_callback(
    ListRunExecutionDatesCallback cb) -> void {
  list_run_execution_dates_ = std::move(cb);
}

auto Engine::set_get_watermark_callback(GetWatermarkCallback cb) -> void {
  get_watermark_ = std::move(cb);
}

auto Engine::set_save_watermark_callback(SaveWatermarkCallback cb) -> void {
  save_watermark_ = std::move(cb);
}

auto Engine::scheduled_task_count() const -> std::size_t {
  return scheduled_task_count_snapshot_.load(std::memory_order_relaxed);
}

auto Engine::missed_schedules_total() const -> std::uint64_t {
  return missed_schedules_total_.load(std::memory_order_relaxed);
}

auto Engine::add_task_on_owner(ExecutionInfo exec_info)
    -> boost::asio::awaitable<void> {
  auto id = generate_dag_task_id(exec_info.dag_id, exec_info.task_id);

  if (tasks_.contains(id)) {
    co_return;
  }

  tasks_.emplace(id, exec_info);

  if (const auto &cron_expr = exec_info.cron_expr; cron_expr) {
    auto now = std::chrono::system_clock::now();
    const auto &cron = *cron_expr;

    TimePoint baseline_time = now;
    if (exec_info.start_date.has_value()) {
      baseline_time = *exec_info.start_date;
    }

    if (get_watermark_) {
      auto watermark_res = co_await get_watermark_(exec_info.dag_id);
      if (watermark_res && watermark_res->has_value()) {
        baseline_time = **watermark_res;
      }
    }

    TimePoint effective_baseline = baseline_time;

    if (exec_info.catchup) {
      std::vector<TimePoint> catchup_runs;
      auto next_run = cron.next_after(effective_baseline);
      while (next_run <= now) {
        if (!running_.load(std::memory_order_acquire)) {
          co_return;
        }
        if (exec_info.end_date.has_value() && next_run > *exec_info.end_date) {
          break;
        }
        catchup_runs.push_back(next_run);
        effective_baseline = next_run;
        next_run = cron.next_after(effective_baseline);
      }

      std::unordered_set<std::int64_t> existing_runs;
      bool use_batched_existence = false;
      if (list_run_execution_dates_ && !catchup_runs.empty()) {
        auto existing_res = co_await list_run_execution_dates_(
            exec_info.dag_id, catchup_runs.front(), catchup_runs.back());
        if (!existing_res) {
          log::error("Failed to batch load run existence for DAG {}: {}",
                     exec_info.dag_id, existing_res.error().message());
        } else {
          existing_runs.reserve(existing_res->size());
          for (const auto execution_date : *existing_res) {
            existing_runs.insert(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    execution_date.time_since_epoch())
                    .count());
          }
          use_batched_existence = true;
        }
      }

      const auto execution_date_to_ms = [](TimePoint date) -> std::int64_t {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   date.time_since_epoch())
            .count();
      };

      auto trigger_catchup_run = [&](TimePoint execution_date)
          -> boost::asio::awaitable<void> {
        missed_schedules_total_.fetch_add(1, std::memory_order_relaxed);
        if (!running_.load(std::memory_order_acquire)) {
          co_return;
        }
        log::debug("Catchup triggering DAG: {} for execution_date: {}",
                   exec_info.dag_id,
                   std::chrono::duration_cast<std::chrono::seconds>(
                       execution_date.time_since_epoch())
                       .count());
        on_dag_trigger_(exec_info.dag_id, execution_date);

        if (save_watermark_) {
          auto wm_res =
              co_await save_watermark_(exec_info.dag_id, execution_date);
          wm_res.or_else([&](std::error_code ec) -> Result<void> {
            log::error("Failed to save watermark for DAG {}: {}",
                       exec_info.dag_id, ec.message());
            return fail(ec);
          });
        }
        co_return;
      };

      if (use_batched_existence) {
        auto pending_runs =
            catchup_runs | std::views::filter([&](const TimePoint &date) {
              return !existing_runs.contains(execution_date_to_ms(date));
            });
        for (const auto execution_date : pending_runs) {
          co_await trigger_catchup_run(execution_date);
        }
      } else {
        for (const auto execution_date : catchup_runs) {
          bool exists = false;
          if (run_exists_) {
            auto res = co_await run_exists_(exec_info.dag_id, execution_date);
            if (!res) {
              log::error("Failed to check run existence for DAG {}: {}",
                         exec_info.dag_id, res.error().message());
              exists = true;
            } else {
              exists = *res;
            }
          }
          if (!exists) {
            co_await trigger_catchup_run(execution_date);
          }
        }
      }
    }

    auto next_time = cron.next_after(std::max(effective_baseline, now));

    if (exec_info.end_date.has_value() && next_time > *exec_info.end_date) {
      if (!scheduled_tasks_.contains(id)) {
        log::debug("DAG {} not scheduled: next run time exceeds end_date",
                   exec_info.dag_id);
        co_return;
      }
    } else {
      schedule_task(id, next_time);
    }

    log::debug("DAG : {}, Task added: {}, next scheduled at: {}",
               exec_info.dag_id, exec_info.task_id,
               std::chrono::duration_cast<std::chrono::seconds>(
                   next_time.time_since_epoch())
                   .count());
  }
}

auto Engine::remove_task_on_owner(DAGId dag_id, TaskId task_id) -> void {
  auto id = generate_dag_task_id(dag_id, task_id);
  unschedule_task(id);
  tasks_.erase(id);
  log::debug("DAG: {}, Task removed: {}", dag_id, task_id);
}

} // namespace dagforge
