#include "taskmaster/app/services/persistence_service.hpp"

#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/util/log.hpp"

namespace taskmaster {

PersistenceService::PersistenceService(std::string_view db_path)
    : db_(std::make_unique<Persistence>(db_path)) {}

PersistenceService::~PersistenceService() {
  close();
}

auto PersistenceService::open() -> Result<void> {
  return db_->open();
}

auto PersistenceService::close() -> void {
  if (db_) {
    db_->close();
  }
}

auto PersistenceService::is_open() const -> bool {
  return db_ && db_->is_open();
}

auto PersistenceService::persistence() -> Persistence* {
  return db_.get();
}

auto PersistenceService::save_run(const DAGRun& run) -> void {
  if (!db_)
    return;
  if (auto r = db_->save_dag_run(run); !r.has_value()) {
    log::warn("Failed to persist run {}: {}", run.id(), r.error().message());
  }
}

auto PersistenceService::save_task(DAGRunId dag_run_id, const TaskInstanceInfo& info) -> void {
  if (!db_)
    return;
  if (auto r = db_->update_task_instance(dag_run_id, info); !r.has_value()) {
    if (auto r2 = db_->save_task_instance(dag_run_id, info); !r2) {
      log::warn("Failed to persist task: {}", r2.error().message());
    }
  }
}

auto PersistenceService::save_log(DAGRunId dag_run_id,
                                  TaskId task, int attempt,
                                  std::string_view level,
                                  std::string_view msg) -> void {
  if (!db_)
    return;
  if (auto r = db_->save_task_log(dag_run_id, task, attempt, level, "stdout", msg);
      !r) {
    log::debug("Failed to save log: {}", r.error().message());
  }
}

auto PersistenceService::save_xcom(DAGRunId dag_run_id,
                                   TaskId task,
                                   std::string_view key,
                                   const nlohmann::json& value) -> void {
  if (!db_)
    return;
  if (auto r = db_->save_xcom(dag_run_id, task, key, value); !r.has_value()) {
    log::debug("Failed to save xcom: {}", r.error().message());
  }
}

auto PersistenceService::get_xcom(DAGRunId dag_run_id,
                                  TaskId task,
                                  std::string_view key) -> Result<nlohmann::json> {
  if (!db_)
    return fail(Error::NotFound);
  return db_->get_xcom(dag_run_id, task, key);
}

auto PersistenceService::get_previous_task_state(
    DAGId dag_id, NodeIndex task_idx,
    std::chrono::system_clock::time_point current_execution_date,
    std::string_view current_dag_run_id) const
    -> Result<std::optional<TaskState>> {
  if (!db_)
    return fail(Error::NotFound);
  return db_->get_previous_task_state(dag_id, task_idx, current_execution_date, current_dag_run_id);
}

}  // namespace taskmaster
