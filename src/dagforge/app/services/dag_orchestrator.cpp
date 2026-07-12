#include "dagforge/app/services/dag_orchestrator.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/hash.hpp"

#include <algorithm>
#include <format>
#include <ranges>

namespace dagforge {

DAGOrchestrator::DAGOrchestrator(Dependencies deps)
    : deps_(std::move(deps)),
      dag_owner_states_(std::max<std::size_t>(1, deps_.owner_shard_count)) {}

auto DAGOrchestrator::trigger_run(
    DAGId dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date,
    RunConfMap conf_values) -> task<Result<DAGRunId>> {
  if (!deps_.runtime.is_running()) {
    co_return fail(Error::InvalidState);
  }

  const auto request_now = std::chrono::system_clock::now();
  auto owner = owner_shard(dag_id);
  if (!deps_.runtime.is_current_shard() || deps_.runtime.current_shard() != owner) {
    auto run_res = co_await co_as_result(boost::asio::co_spawn(
        deps_.runtime.executor_for(owner),
        trigger_run_on_dag_owner_shard(dag_id.clone(), trigger, execution_date,
                                       std::move(conf_values), request_now),
        dagforge::use_nothrow));
    if (!run_res) {
      co_return fail(run_res.error());
    }
    co_return std::move(*run_res);
  }

  auto run_res = co_await trigger_run_on_dag_owner_shard(
      std::move(dag_id), trigger, execution_date, std::move(conf_values),
      request_now);
  if (!run_res) {
    co_return fail(run_res.error());
  }
  co_return ok(std::move(*run_res));
}

auto DAGOrchestrator::trigger_scheduled(
    DAGId dag_id, std::chrono::system_clock::time_point execution_date)
    -> spawn_task {
  auto owner = owner_shard(dag_id);
  if (!deps_.runtime.is_current_shard() || deps_.runtime.current_shard() != owner) {
    auto hop_res = co_await co_as_result(boost::asio::co_spawn(
        deps_.runtime.executor_for(owner),
        trigger_scheduled_on_owner_shard(dag_id.clone(), execution_date),
        dagforge::use_nothrow));
    if (!hop_res) {
      log::error("Failed to trigger scheduled DAG hop: {} ({})", dag_id,
                 hop_res.error().message());
    }
    co_return;
  }

  auto r = co_await trigger_run_on_dag_owner_shard(
      dag_id, TriggerType::Schedule, execution_date, {},
      std::chrono::system_clock::now());
  if (!r) {
    log::error("Failed to trigger scheduled DAG: {} ({})", dag_id,
               r.error().message());
  }
}

auto DAGOrchestrator::trigger_scheduled_on_owner_shard(
    DAGId dag_id, std::chrono::system_clock::time_point execution_date)
    -> spawn_task {
  auto r = co_await trigger_run_on_dag_owner_shard(
      dag_id.clone(), TriggerType::Schedule, execution_date, {},
      std::chrono::system_clock::now());
  if (!r) {
    log::error("Failed to trigger scheduled DAG: {} ({})", dag_id,
               r.error().message());
  }
}

auto DAGOrchestrator::trigger_run_blocking(
    const DAGId &dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date)
    -> Result<DAGRunId> {
  if (!deps_.runtime.is_running()) {
    return fail(Error::InvalidState);
  }

  return sync_wait_on_runtime(
      deps_.runtime,
      trigger_run(dag_id.clone(), trigger, execution_date, {}));
}

auto DAGOrchestrator::trigger_run_on_dag_owner_shard(
    DAGId dag_id, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date,
    RunConfMap conf_values,
    std::chrono::system_clock::time_point request_now)
    -> task<Result<DAGRunId>> {
  const auto started_at = std::chrono::steady_clock::now();
  auto dag_res = deps_.dag_manager.get_dag(dag_id);
  if (!dag_res) {
    co_return fail(dag_res.error());
  }

  DAGInfo info = std::move(*dag_res);
  const auto has_task_rowids = std::ranges::all_of(
      info.tasks, [](const TaskConfig &task) { return task.task_rowid > 0; });
  if (info.dag_rowid <= 0 || !has_task_rowids) {
    log::error(
        "DAG snapshot is not fully materialized for trigger path: dag_id={} dag_rowid={} has_task_rowids={}",
        dag_id, info.dag_rowid, has_task_rowids);
    co_return fail(Error::InvalidState);
  }
  if (!info.compiled_graph || !info.compiled_executor_configs ||
      !info.compiled_indexed_task_configs ||
      info.compiled_indexed_task_configs->empty()) {
    log::error("DAG snapshot is missing runtime artifacts for {}", dag_id);
    co_return fail(Error::InvalidState);
  }

  auto plan = RunLaunchPlan{
      .dag_id = dag_id.clone(),
      .dag_rowid = info.dag_rowid,
      .version = info.version,
      .graph = info.compiled_graph,
      .executor_configs = info.compiled_executor_configs,
      .indexed_task_configs = info.compiled_indexed_task_configs,
      .conf_values = std::move(conf_values),
  };
  const auto after_build_launch_plan = std::chrono::steady_clock::now();

  if (auto slot_res = try_acquire_dag_run_slot(info); !slot_res) {
    co_return fail(slot_res.error());
  }

  auto dag_run_id = generate_dag_run_id(dag_id);

  auto run_result = co_await trigger_run_on_owner_shard(
      std::move(plan), trigger, execution_date, dag_run_id.clone(), request_now);
  const auto after_run_create = std::chrono::steady_clock::now();

  if (!run_result) {
    release_dag_run_slot(dag_id);
    co_return fail(run_result.error());
  }

  log::debug("trigger_run_on_dag_owner_shard dag_id={} dag_run_id={} "
             "build_launch_plan_ms={} "
             "run_create_ms={} total_ms={}",
             dag_id, *run_result,
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 after_build_launch_plan - started_at)
                 .count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 after_run_create - after_build_launch_plan)
                 .count(),
             std::chrono::duration_cast<std::chrono::milliseconds>(
                 after_run_create - started_at)
                 .count());

  co_return run_result;
}

auto DAGOrchestrator::trigger_run_on_owner_shard(
    RunLaunchPlan plan, TriggerType trigger,
    std::optional<std::chrono::system_clock::time_point> execution_date,
    DAGRunId dag_run_id, std::chrono::system_clock::time_point request_now)
    -> task<Result<DAGRunId>> {
  const auto started_at = std::chrono::steady_clock::now();

  auto run_res = DAGRun::create(dag_run_id.clone(), plan.graph);
  if (!run_res) {
    co_return fail(run_res.error());
  }
  auto cfgs = plan.executor_configs;
  auto run = std::make_unique<DAGRun>(std::move(*run_res));
  run->set_scheduled_at(request_now);
  run->set_started_at(request_now);
  run->set_trigger_type(trigger);
  run->set_execution_date(execution_date.value_or(request_now));
  run->set_dag_rowid(plan.dag_rowid);
  run->set_dag_version(plan.version);

  for (NodeIndex idx = 0; idx < plan.indexed_task_configs->size(); ++idx) {
    const auto &task = (*plan.indexed_task_configs)[idx];
    if (task.task_id.empty()) {
      continue;
    }
    if (auto r = run->set_task_rowid(idx, task.task_rowid); !r) {
      co_return fail(r.error());
    }
  }

  auto indexed_task_cfgs = plan.indexed_task_configs;
  DAGRun run_snapshot = *run;
  auto all_task_infos = run->all_task_info();
  std::vector<TaskInstanceInfo> task_infos;
  task_infos.reserve(all_task_infos.size());

  for (const auto &info : all_task_infos) {
    if (run->is_task_ready(info.task_idx)) {
      task_infos.emplace_back(info);
    }
  }
  if (task_infos.empty()) {
    task_infos = std::move(all_task_infos);
  }

  const auto unresolved =
      std::count_if(task_infos.begin(), task_infos.end(),
                    [](const auto &info) { return info.task_rowid <= 0; });
  if (unresolved != 0) {
    log::error(
        "Refusing to persist run {} with unresolved task_rowid values: "
        "batch_size={} unresolved={}",
        run_snapshot.id(), task_infos.size(), unresolved);
  }
  auto persist_result =
      co_await deps_.persistence.create_run_with_task_instances(
          std::move(run_snapshot), std::move(task_infos));
  const auto after_first_persist = std::chrono::steady_clock::now();

  co_return persist_result
      .and_then(
          [this, run = std::move(run), cfgs = std::move(cfgs),
           indexed_task_cfgs = std::move(indexed_task_cfgs), dag_run_id,
           dag_id = plan.dag_id.clone(), conf_values = std::move(plan.conf_values),
           trigger, started_at,
           after_first_persist](int64_t rowid) mutable -> Result<DAGRunId> {
            run->set_run_rowid(rowid);
            deps_.execution.start_run(
                dag_run_id, ExecutionService::RunContext{
                                .run = std::move(run),
                                .executor_configs = std::move(cfgs),
                                .task_configs = std::move(indexed_task_cfgs),
                                .dag_id = dag_id.clone(),
                                .conf_values = std::move(conf_values)});
            const auto after_dispatch = std::chrono::steady_clock::now();
            log::debug("trigger_run_on_owner_shard dag_id={} dag_run_id={} "
                       "first_persist_ms={} "
                       "dispatch_ms={} total_ms={}",
                       dag_id, dag_run_id,
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           after_first_persist - started_at)
                           .count(),
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           after_dispatch - after_first_persist)
                           .count(),
                       std::chrono::duration_cast<std::chrono::milliseconds>(
                           after_dispatch - started_at)
                           .count());
            return ok(dag_run_id);
          })
      .transform([dag_id = plan.dag_id.clone(), trigger](DAGRunId id) {
        log::info("DAG run {} triggered for {} ({})", id, dag_id,
                  trigger == TriggerType::Schedule ? "schedule" : "manual");
        return id;
      })
      .or_else([dag_run_id](std::error_code ec) -> Result<DAGRunId> {
        log::error("Failed to persist dag run {}: {}", dag_run_id,
                   ec.message());
        return fail(ec);
      });
}

auto DAGOrchestrator::owner_shard(const DAGId &dag_id) const noexcept
    -> shard_id {
  const auto shard_count =
      std::max(1U, static_cast<unsigned>(dag_owner_states_.size()));
  return static_cast<shard_id>(util::shard_of(dag_id.value(), shard_count));
}

auto DAGOrchestrator::owner_shard(const DAGRunId &dag_run_id) const noexcept
    -> shard_id {
  const auto shard_count = std::max(1U, deps_.runtime.shard_count());
  return static_cast<shard_id>(
      util::shard_of(dag_run_id.value(), shard_count));
}

auto DAGOrchestrator::try_acquire_dag_run_slot(const DAGInfo &info)
    -> Result<void> {
  if (info.is_paused) {
    return fail(Error::InvalidState);
  }

  const auto owner = owner_shard(info.dag_id);
  if (!deps_.runtime.is_current_shard() || deps_.runtime.current_shard() != owner) {
    return fail(Error::InvalidState);
  }

  auto &state = dag_owner_states_[owner].dags[info.dag_id];
  if (info.max_concurrent_runs > 0 &&
      state.active_runs >= info.max_concurrent_runs) {
    return fail(Error::HasActiveRuns);
  }
  ++state.active_runs;
  return ok();
}

auto DAGOrchestrator::release_dag_run_slot(const DAGId &dag_id) -> void {
  const auto owner = owner_shard(dag_id);
  if (!deps_.runtime.is_current_shard() || deps_.runtime.current_shard() != owner) {
    deps_.runtime.post_to(owner, [this, dag_id = dag_id.clone()]() mutable {
      release_dag_run_slot(dag_id);
    });
    return;
  }

  auto &dags = dag_owner_states_[owner].dags;
  if (auto it = dags.find(dag_id); it != dags.end()) {
    if (it->second.active_runs > 0) {
      --it->second.active_runs;
    }
  }
}

auto DAGOrchestrator::resolve_dag_id(const DAGRunId &dag_run_id) const
    -> std::optional<DAGId> {
  return dag_id_from_run_id(dag_run_id);
}

auto DAGOrchestrator::on_run_finished(const DAGRunId &dag_run_id,
                                      DAGRunState status) -> void {
  if (status == DAGRunState::Queued || status == DAGRunState::Running) {
    return;
  }

  auto dag_id = resolve_dag_id(dag_run_id);
  if (!dag_id) {
    return;
  }
  release_dag_run_slot(*dag_id);
}

auto DAGOrchestrator::get_run_state(const DAGRunId &dag_run_id) const
    -> Result<DAGRunState> {
  return sync_wait_on_runtime(deps_.runtime, get_run_state_async(dag_run_id));
}

auto DAGOrchestrator::get_run_state_async(const DAGRunId &dag_run_id) const
    -> task<Result<DAGRunState>> {
  auto snapshot_res = co_await deps_.execution.get_run_snapshot(dag_run_id);
  if (snapshot_res) {
    co_return ok((*snapshot_res)->state());
  }

  auto history_res = co_await deps_.persistence.get_run_history(dag_run_id);
  if (!history_res) {
    co_return fail(history_res.error());
  }
  co_return ok(history_res->state);
}

auto DAGOrchestrator::has_active_runs() const -> bool {
  return deps_.execution.has_active_runs();
}

auto DAGOrchestrator::wait_for_completion_async(int timeout_ms)
    -> task<Result<void>> {
  auto wait_res = co_await deps_.execution.wait_for_completion_async(timeout_ms);
  if (!wait_res) {
    co_return fail(wait_res.error());
  }
  co_return ok();
}

auto DAGOrchestrator::get_max_retries(const DAGRunId &dag_run_id,
                                      NodeIndex idx) const -> int {
  std::optional<DAGId> dag_id = deps_.execution.get_cached_dag_id(dag_run_id);
  if (!dag_id) {
    dag_id = resolve_dag_id(dag_run_id);
  }
  if (!dag_id) {
    return 3;
  }

  if (auto dag_info = deps_.dag_manager.get_dag(*dag_id);
      dag_info && idx < dag_info->tasks.size()) {
    return dag_info->tasks[idx].max_retries;
  }

  return 3;
}

auto DAGOrchestrator::get_retry_interval(const DAGRunId &dag_run_id,
                                         NodeIndex idx) const
    -> std::chrono::seconds {
  std::optional<DAGId> dag_id = deps_.execution.get_cached_dag_id(dag_run_id);
  if (!dag_id) {
    dag_id = resolve_dag_id(dag_run_id);
  }
  if (!dag_id) {
    return std::chrono::seconds(60);
  }

  if (auto dag_info = deps_.dag_manager.get_dag(*dag_id);
      dag_info && idx < dag_info->tasks.size()) {
    return dag_info->tasks[idx].retry_interval;
  }

  return std::chrono::seconds(60);
}

auto DAGOrchestrator::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  auto db_res = co_await deps_.persistence.set_dag_paused(dag_id, paused);
  if (!db_res) {
    co_return db_res;
  }

  auto dag_res = deps_.dag_manager.get_dag(dag_id);
  if (dag_res) {
    DagStateRecord state = state_from_snapshot_info(*dag_res);
    state.is_paused = paused;
    state.updated_at = std::chrono::system_clock::now();
    deps_.dag_manager.apply_dag_state(dag_id, state);
  }

  if (paused) {
    deps_.scheduler.unregister_dag(dag_id);
  } else if (dag_res) {
    const auto &dag = *dag_res;
    if (!dag.cron.empty()) {
      deps_.scheduler.register_dag(dag_id, dag);
    }
  }

  log::info("DAG {} {}", dag_id, paused ? "paused" : "unpaused");
  co_return ok();
}

auto DAGOrchestrator::register_dag_cron(DAGId dag_id,
                                        std::string_view /*cron_expr*/)
    -> Result<void> {
  return deps_.dag_manager.get_dag(dag_id)
      .and_then([&](const DAGInfo &dag) -> Result<void> {
        if (dag.tasks.empty()) {
          log::error("Cannot register cron for DAG {}: empty", dag_id);
          return fail(Error::InvalidArgument);
        }
        deps_.scheduler.register_dag(dag_id, dag);
        return ok();
      })
      .or_else([&](std::error_code ec) -> Result<void> {
        log::error("Cannot register cron for DAG {}: not found", dag_id);
        return fail(ec);
      });
}

auto DAGOrchestrator::unregister_dag_cron(const DAGId &dag_id) -> void {
  deps_.scheduler.unregister_dag(dag_id);
}

auto DAGOrchestrator::update_dag_cron(const DAGId &dag_id,
                                      std::string_view cron_expr,
                                      bool is_active) -> Result<void> {
  unregister_dag_cron(dag_id);

  if (!cron_expr.empty() && is_active) {
    return register_dag_cron(dag_id, cron_expr);
  }
  return ok();
}

} // namespace dagforge
