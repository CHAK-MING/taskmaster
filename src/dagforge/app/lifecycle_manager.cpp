#include "dagforge/app/lifecycle_manager.hpp"

#include "dagforge/util/log.hpp"

#include <ranges>

namespace dagforge {
namespace {

[[nodiscard]] auto build_dag_state_index(std::vector<DagStateRecord> states)
    -> LifecycleManager::DagStateIndex {
  LifecycleManager::DagStateIndex out;
  out.reserve(states.size());
  for (auto &state : states) {
    out.insert_or_assign(state.dag_id, std::move(state));
  }
  return out;
}

} // namespace

LifecycleManager::LifecycleManager(Dependencies deps) : deps_(std::move(deps)) {}

auto LifecycleManager::rollback_partial_start(bool log_started) noexcept
    -> void {
  (void)deps_.stop_config_watcher(std::chrono::steady_clock::time_point::max());
  deps_.stop_api();
  (void)deps_.stop_scheduler(std::chrono::steady_clock::time_point::max());

  if (deps_.persistence.is_open() && deps_.runtime.is_running()) {
    (void)deps_.close_persistence(std::chrono::steady_clock::time_point::max());
  }

  if (log_started) {
    log::stop();
  }

  if (deps_.runtime.is_running()) {
    deps_.runtime.stop();
  }

  deps_.running.store(false, std::memory_order_release);
}

auto LifecycleManager::init() -> Result<void> {
  return deps_.ensure_api_initialized();
}

auto LifecycleManager::init_db_only() -> Result<void> {
  if (deps_.running.exchange(true)) {
    return ok();
  }

  auto runtime_res = deps_.runtime.start();
  if (!runtime_res) {
    deps_.running = false;
    return fail(runtime_res.error());
  }

  if (!deps_.persistence.is_open()) {
    if (auto open_res = deps_.persistence.sync_wait(deps_.persistence.open());
        !open_res) {
      deps_.running = false;
      deps_.runtime.stop();
      return fail(open_res.error());
    }
  }

  Result<void> close_res = ok();
  if (deps_.persistence.is_open()) {
    close_res = deps_.persistence.sync_wait(deps_.persistence.close());
  }

  deps_.runtime.stop();
  deps_.running = false;
  if (!close_res) {
    return fail(close_res.error());
  }
  return ok();
}

auto LifecycleManager::start() -> Result<void> {
  if (deps_.running.exchange(true)) {
    return ok();
  }

  bool log_started = false;

  auto runtime_res = deps_.runtime.start();
  if (!runtime_res) {
    deps_.running = false;
    return fail(runtime_res.error());
  }

  log::start();
  log_started = true;
  log::debug("Runtime started");

  if (!deps_.persistence.is_open()) {
    if (auto open_res = deps_.persistence.sync_wait(deps_.persistence.open());
        !open_res) {
      rollback_partial_start(log_started);
      return fail(open_res.error());
    }
  }

  DagStateIndex dag_state_index;
  if (deps_.persistence.is_open()) {
    auto states_res = deps_.persistence.sync_wait(deps_.persistence.list_dag_states());
    if (!states_res) {
      rollback_partial_start(log_started);
      return fail(states_res.error());
    }
    dag_state_index = build_dag_state_index(std::move(*states_res));
  }

  if (!deps_.config.dag_source.directory.empty()) {
    if (auto load_res = deps_.dag_catalog.load_directory(
            deps_.config.dag_source.directory, &dag_state_index);
        !load_res) {
      rollback_partial_start(log_started);
      return fail(load_res.error());
    }
  }

  if (deps_.persistence.is_open()) {
    bool has_db_only_dags = false;
    for (const auto &[dag_id, _state] : dag_state_index) {
      if (deps_.dag_manager.has_dag(dag_id)) {
        continue;
      }
      has_db_only_dags = true;
      break;
    }
    if (has_db_only_dags) {
      if (auto load_res = deps_.dag_manager.load_from_database(); !load_res) {
        rollback_partial_start(log_started);
        return fail(load_res.error());
      }
    } else {
      log::debug("Loaded {} DAG states from database", dag_state_index.size());
    }
  } else if (auto load_res = deps_.dag_manager.load_from_database(); !load_res) {
    rollback_partial_start(log_started);
    return fail(load_res.error());
  }

  deps_.scheduler.start();
  for (const auto &d :
       deps_.dag_manager.list_dags() | std::views::filter([](const auto &dag) {
         return !dag.cron.empty();
       })) {
    deps_.scheduler.register_dag(d.dag_id, d);
  }

  if (deps_.config.api.enabled) {
    if (auto api_res = deps_.start_api(); !api_res) {
      rollback_partial_start(log_started);
      return fail(api_res.error());
    }
    log::debug("API server started on {}:{}", deps_.config.api.host,
               deps_.config.api.port);
  }

  deps_.start_config_watcher();
  return ok();
}

auto LifecycleManager::stop() noexcept -> void {
  if (!deps_.running.exchange(false)) {
    return;
  }

  log::debug("Stopping DAGForge...");
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);

  if (auto watcher_res = deps_.stop_config_watcher(deadline); !watcher_res) {
    log::warn("Config watcher stop timed out");
  }
  deps_.stop_api();

  if (auto scheduler_res = deps_.stop_scheduler(deadline); !scheduler_res) {
    log::warn("Scheduler stop timed out");
  }

  if (auto execution_res = deps_.wait_for_execution_quiesced(deadline);
      !execution_res) {
    log::warn("Shutdown timeout: execution still active");
  }

  if (auto persistence_res = deps_.close_persistence(deadline); !persistence_res) {
    log::warn("Persistence close timed out");
  }

  if (deps_.runtime.is_running()) {
    deps_.runtime.stop();
  }

  log::debug("DAGForge stopped");
}

} // namespace dagforge
