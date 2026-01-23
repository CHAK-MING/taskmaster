#include "taskmaster/app/services/scheduler_service.hpp"

#include "taskmaster/config/dag_definition.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/scheduler/cron.hpp"
#include "taskmaster/scheduler/task.hpp"
#include "taskmaster/util/log.hpp"

#include <optional>
#include <ranges>

namespace taskmaster {

SchedulerService::SchedulerService(Runtime& runtime)
    : engine_(runtime) {
}

auto SchedulerService::set_on_dag_trigger(DAGTriggerCallback callback) -> void {
  on_dag_trigger_ = std::move(callback);
  engine_.set_on_dag_trigger([this](const DAGId& dag_id, 
                                     std::chrono::system_clock::time_point execution_date) {
    if (on_dag_trigger_) {
      on_dag_trigger_(dag_id, execution_date);
    }
  });
}

auto SchedulerService::set_check_exists_callback(CheckExistsCallback callback) -> void {
  engine_.set_check_exists_callback(std::move(callback));
}

auto SchedulerService::register_dag(DAGId dag_id, const DAGInfo& dag_info)
    -> void {
  if (dag_info.cron.empty()) {
    return;
  }

  auto cron_expr = CronExpr::parse(dag_info.cron);
  if (!cron_expr) {
    log::warn("Invalid cron expression for DAG {}: {}", dag_id, dag_info.cron);
    return;
  }

  TaskId schedule_task_id{"__schedule__"};

  ExecutionInfo info{
      .dag_id = dag_id,
      .task_id = schedule_task_id,
      .name = dag_info.name,
      .cron_expr = std::make_optional(*cron_expr),
      .start_date = dag_info.start_date,
      .end_date = dag_info.end_date,
      .catchup = dag_info.catchup,
  };

  if (!engine_.add_task(std::move(info))) {
    log::warn("Failed to register DAG schedule: {}", dag_id);
    return;
  }

  log::info("Registered DAG schedule: {} with cron: {}",
            dag_id, dag_info.cron);
  registered_root_tasks_[dag_id] = schedule_task_id;
}

auto SchedulerService::unregister_dag(DAGId dag_id) -> void {
  auto it = registered_root_tasks_.find(dag_id);
  if (it == registered_root_tasks_.end()) {
    return;
  }

  if (!engine_.remove_task(dag_id, it->second)) {
    log::warn("Failed to unregister DAG schedule: {}", dag_id);
  } else {
    log::info("Unregistered DAG schedule: {}", dag_id);
  }
  registered_root_tasks_.erase(it);
}

auto SchedulerService::start() -> void {
  engine_.start();
}

auto SchedulerService::stop() -> void {
  engine_.stop();
}

auto SchedulerService::is_running() const -> bool {
  return engine_.is_running();
}

auto SchedulerService::engine() -> Engine& {
  return engine_;
}

}  // namespace taskmaster
