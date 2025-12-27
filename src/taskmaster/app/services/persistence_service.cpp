#include "taskmaster/app/services/persistence_service.hpp"

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
  if (auto r = db_->save_dag_run(run); !r) {
    log::warn("Failed to persist run {}: {}", run.id(), r.error().message());
  }
}

auto PersistenceService::save_task(std::string_view run_id,
                                   const TaskInstanceInfo& info) -> void {
  if (!db_)
    return;
  if (auto r = db_->update_task_instance(run_id, info); !r) {
    if (auto r2 = db_->save_task_instance(run_id, info); !r2) {
      log::warn("Failed to persist task: {}", r2.error().message());
    }
  }
}

auto PersistenceService::save_log(std::string_view run_id,
                                  std::string_view task, int attempt,
                                  std::string_view level,
                                  std::string_view msg) -> void {
  if (!db_)
    return;
  if (auto r = db_->save_task_log(run_id, task, attempt, level, "stdout", msg);
      !r) {
    log::debug("Failed to save log: {}", r.error().message());
  }
}

}  // namespace taskmaster
