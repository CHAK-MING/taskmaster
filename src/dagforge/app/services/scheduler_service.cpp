#include "dagforge/app/services/scheduler_service.hpp"

#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <future>
#include <optional>
#include <thread>

namespace dagforge {

SchedulerService::SchedulerService(Runtime &runtime, unsigned scheduler_shards)
    : runtime_(runtime) {
  const auto runtime_shards = std::max(1U, runtime_.shard_count());
  const auto shard_count = std::clamp(
      scheduler_shards == 0 ? runtime_shards : scheduler_shards, 1U,
      runtime_shards);
  engines_.reserve(shard_count);
  engine_shards_.reserve(shard_count);
  for (unsigned sid = 0; sid < shard_count; ++sid) {
    auto engine_shard = static_cast<shard_id>(sid);
    engines_.emplace_back(std::make_unique<Engine>(runtime_, engine_shard));
    engine_shards_.push_back(engine_shard);
  }
  refresh_engine_callbacks();
}

SchedulerService::~SchedulerService() { (void)stop(); }

auto SchedulerService::set_on_dag_trigger(DAGTriggerCallback callback) -> void {
  if (!can_update_callbacks("set_on_dag_trigger")) {
    return;
  }
  on_dag_trigger_ = std::move(callback);
  refresh_engine_callbacks();
}

auto SchedulerService::set_run_exists_callback(RunExistsCallback callback)
    -> void {
  if (!can_update_callbacks("set_run_exists_callback")) {
    return;
  }
  run_exists_callback_ = std::move(callback);
  refresh_engine_callbacks();
}

auto SchedulerService::set_list_run_execution_dates_callback(
    ListRunExecutionDatesCallback callback) -> void {
  if (!can_update_callbacks("set_list_run_execution_dates_callback")) {
    return;
  }
  list_run_execution_dates_callback_ = std::move(callback);
  refresh_engine_callbacks();
}

auto SchedulerService::set_get_watermark_callback(GetWatermarkCallback callback)
    -> void {
  if (!can_update_callbacks("set_get_watermark_callback")) {
    return;
  }
  get_watermark_callback_ = std::move(callback);
  refresh_engine_callbacks();
}

auto SchedulerService::set_save_watermark_callback(
    SaveWatermarkCallback callback) -> void {
  if (!can_update_callbacks("set_save_watermark_callback")) {
    return;
  }
  save_watermark_callback_ = std::move(callback);
  refresh_engine_callbacks();
}

auto SchedulerService::set_zombie_reaper_callback(ZombieReaperCallback callback)
    -> void {
  if (is_running()) {
    log::warn(
        "SchedulerService::set_zombie_reaper_callback ignored while running");
    return;
  }
  zombie_reaper_state_->callback = std::move(callback);
}

auto SchedulerService::set_zombie_reaper_config(int interval_sec,
                                                int heartbeat_timeout_sec)
    -> void {
  if (is_running()) {
    log::warn("SchedulerService::set_zombie_reaper_config ignored while "
              "running");
    return;
  }
  zombie_reaper_state_->interval_sec = interval_sec;
  zombie_reaper_state_->heartbeat_timeout_sec = heartbeat_timeout_sec;
}

auto SchedulerService::register_dag(DAGId dag_id, const DAGInfo &dag_info)
    -> void {
  if (dag_info.cron.empty() || !is_running()) {
    return;
  }

  auto cron_expr_result = CronExpr::parse(dag_info.cron);
  if (!cron_expr_result) {
    cron_parse_errors_total_.fetch_add(1, std::memory_order_relaxed);
    log::warn("Invalid cron expression for DAG {}: {}", dag_id, dag_info.cron);
    return;
  }

  TaskId schedule_task_id{"__schedule__"};
  ExecutionInfo info{
      .dag_id = dag_id,
      .task_id = schedule_task_id,
      .name = dag_info.name,
      .cron_expr = std::make_optional(std::move(*cron_expr_result)),
      .start_date = dag_info.start_date,
      .end_date = dag_info.end_date,
      .catchup = dag_info.catchup,
  };

  owner_engine(dag_id)
      .add_task(std::move(info))
      .and_then([&]() -> Result<void> {
        log::debug("Registered DAG schedule: {} with cron: {}", dag_id,
                   dag_info.cron);
        registered_root_tasks_[dag_id] = schedule_task_id;
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::warn("Failed to register DAG schedule: {} - error: {}", dag_id,
                  ec.message());
        return ok();
      });
}

auto SchedulerService::unregister_dag(const DAGId &dag_id) -> void {
  auto it = registered_root_tasks_.find(dag_id);
  if (it == registered_root_tasks_.end()) {
    return;
  }

  owner_engine(dag_id)
      .remove_task(dag_id, it->second)
      .and_then([&]() -> Result<void> {
        log::debug("Unregistered DAG schedule: {}", dag_id);
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::warn("Failed to unregister DAG schedule: {} - error: {}", dag_id,
                  ec.message());
        return ok();
      });
  registered_root_tasks_.erase(it);
}

auto SchedulerService::start() -> void {
  for (auto &engine : engines_) {
    engine->start();
  }

  auto state = zombie_reaper_state_;
  if (state->interval_sec <= 0 || state->heartbeat_timeout_sec <= 0) {
    state->running.store(false, std::memory_order_release);
    return;
  }
  if (state->running.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  const auto generation =
      state->generation.fetch_add(1, std::memory_order_acq_rel) + 1;
  auto reaper_loop = [](std::shared_ptr<ZombieReaperState> state,
                        std::uint64_t generation) -> task<void> {
    const auto is_active = [&]() {
      return state->running.load(std::memory_order_acquire) &&
             state->generation.load(std::memory_order_acquire) == generation;
    };

    while (is_active()) {
      try {
        co_await async_sleep_on_timing_wheel(
            std::chrono::seconds(state->interval_sec));
      } catch (const std::exception &e) {
        log::debug("Zombie reaper sleep cancelled/stopped: {}", e.what());
        break;
      }
      if (!is_active()) {
        break;
      }
      if (!state->callback) {
        continue;
      }

      const auto timeout_ms =
          static_cast<std::int64_t>(state->heartbeat_timeout_sec * 1000LL);
      Result<std::size_t> reaped = fail(Error::Cancelled);
      state->inflight.fetch_add(1, std::memory_order_acq_rel);
      try {
        reaped = co_await state->callback(timeout_ms);
      } catch (const std::exception &e) {
        state->inflight.fetch_sub(1, std::memory_order_acq_rel);
        log::error("Zombie reaper callback threw exception: {}", e.what());
        break;
      } catch (...) {
        state->inflight.fetch_sub(1, std::memory_order_acq_rel);
        log::error("Zombie reaper callback threw non-standard exception");
        break;
      }
      state->inflight.fetch_sub(1, std::memory_order_acq_rel);
      if (!reaped) {
        log::warn("Zombie reaper failed: {}", reaped.error().message());
        continue;
      }
      if (*reaped > 0) {
        log::warn("Zombie reaper marked {} task instance(s) failed", *reaped);
      }
    }
  };
  runtime_.spawn_on(zombie_reaper_shard_,
                    reaper_loop(std::move(state), generation));
}

auto SchedulerService::stop(std::chrono::steady_clock::time_point deadline)
    -> Result<void> {
  auto state = zombie_reaper_state_;
  state->running.store(false, std::memory_order_release);
  state->generation.fetch_add(1, std::memory_order_acq_rel);

  bool barrier_complete = true;
  if (runtime_.is_running() &&
      (!runtime_.is_current_shard() ||
       runtime_.current_shard() != zombie_reaper_shard_)) {
    std::promise<void> barrier;
    auto barrier_future = barrier.get_future();
    runtime_.post_to(
        zombie_reaper_shard_,
        [barrier = std::move(barrier)]() mutable { barrier.set_value(); });
    if (deadline == std::chrono::steady_clock::time_point::max()) {
      barrier_future.wait();
    } else {
      barrier_complete =
          barrier_future.wait_until(deadline) == std::future_status::ready;
    }
  }

  while (barrier_complete &&
         state->inflight.load(std::memory_order_acquire) > 0 &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  for (auto &engine : engines_) {
    engine->stop();
  }

  if (!barrier_complete ||
      state->inflight.load(std::memory_order_acquire) > 0) {
    return fail(Error::Timeout);
  }
  return ok();
}

auto SchedulerService::is_running() const -> bool {
  return std::ranges::any_of(
      engines_, [](const std::unique_ptr<Engine> &engine) {
        return engine->is_running();
      });
}

auto SchedulerService::queue_depth() const -> std::size_t {
  std::size_t total = 0;
  for (const auto &engine : engines_) {
    total += engine->scheduled_task_count();
  }
  return total;
}

auto SchedulerService::missed_schedules_total() const -> std::uint64_t {
  std::uint64_t total = 0;
  for (const auto &engine : engines_) {
    total += engine->missed_schedules_total();
  }
  return total;
}

auto SchedulerService::cron_parse_errors_total() const -> std::uint64_t {
  return cron_parse_errors_total_.load(std::memory_order_relaxed);
}

auto SchedulerService::owner_engine_index(const DAGId &dag_id) const noexcept
    -> std::size_t {
  const auto engine_count = std::max<std::size_t>(1, engines_.size());
  return util::shard_of(std::hash<std::string_view>{}(dag_id.value()),
                        engine_count);
}

auto SchedulerService::owner_shard(const DAGId &dag_id) const noexcept
    -> shard_id {
  return engine_shards_[owner_engine_index(dag_id)];
}

auto SchedulerService::owner_engine(const DAGId &dag_id) -> Engine & {
  return *engines_[owner_engine_index(dag_id)];
}

auto SchedulerService::can_update_callbacks(std::string_view callback_name) const
    -> bool {
  if (is_running()) {
    log::warn("SchedulerService::{} ignored while running", callback_name);
    return false;
  }
  return true;
}

auto SchedulerService::refresh_engine_callbacks() -> void {
  for (auto &engine : engines_) {
    engine->set_on_dag_trigger(
        [this](
            const DAGId &dag_id,
            std::chrono::system_clock::time_point execution_date) {
          if (on_dag_trigger_) {
            on_dag_trigger_(dag_id, execution_date);
          }
        });

    if (run_exists_callback_) {
      engine->set_run_exists_callback(
          [this](const DAGId &dag_id,
                 std::chrono::system_clock::time_point execution_date)
              -> task<Result<bool>> {
            return run_exists_callback_(dag_id, execution_date);
          });
    } else {
      engine->set_run_exists_callback({});
    }

    if (list_run_execution_dates_callback_) {
      engine->set_list_run_execution_dates_callback(
          [this](const DAGId &dag_id,
                 std::chrono::system_clock::time_point start,
                 std::chrono::system_clock::time_point end)
              -> task<
                  Result<std::vector<std::chrono::system_clock::time_point>>> {
            return list_run_execution_dates_callback_(dag_id, start, end);
          });
    } else {
      engine->set_list_run_execution_dates_callback({});
    }

    if (get_watermark_callback_) {
      engine->set_get_watermark_callback(
          [this](const DAGId &dag_id)
              -> task<Result<
                  std::optional<std::chrono::system_clock::time_point>>> {
            return get_watermark_callback_(dag_id);
          });
    } else {
      engine->set_get_watermark_callback({});
    }

    if (save_watermark_callback_) {
      engine->set_save_watermark_callback(
          [this](const DAGId &dag_id,
                 std::chrono::system_clock::time_point watermark)
              -> task<Result<void>> {
            return save_watermark_callback_(dag_id, watermark);
          });
    } else {
      engine->set_save_watermark_callback({});
    }
  }
}

} // namespace dagforge
