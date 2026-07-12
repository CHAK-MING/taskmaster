#include "dagforge/cli/management_client.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <functional>
#include <future>
#include <utility>


namespace dagforge::cli {
namespace {

template <typename T>
auto run_task(io::IoContext &io, task<Result<T>> op) -> Result<T> {
  auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
  while (fut.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
    (void)io.run_one();
  }
  io.restart();
  try {
    return fut.get();
  } catch (const std::future_error &e) {
    log::error("ManagementClient async operation failed: {}", e.what());
    return fail(Error::Unknown);
  } catch (const std::exception &e) {
    log::error("ManagementClient async operation failed: {}", e.what());
    return fail(Error::Unknown);
  }
}

auto reset_task_for_clear(TaskInstanceInfo &task) -> void {
  task.state = TaskState::Pending;
  task.started_at = {};
  task.finished_at = {};
  task.exit_code = 0;
  task.error_message.clear();
  task.error_type.clear();
}

template <typename Mutator>
auto rebuild_run_after_clear(io::IoContext &io, storage::MySQLDatabase &db,
                             const DAGRunId &run_id, Mutator &&mutate)
    -> Result<void> {
  auto run_entry = run_task(io, db.get_run_history(run_id));
  if (!run_entry) {
    return fail(run_entry.error());
  }

  auto dag = run_task(io, db.get_dag(run_entry->dag_id));
  if (!dag) {
    return fail(dag.error());
  }
  if (auto prepared = dag->prepare_runtime_artifacts(); !prepared) {
    return prepared;
  }
  if (!dag->compiled_graph || !dag->compiled_indexed_task_configs) {
    return fail(Error::InvalidState);
  }

  auto run = DAGRun::create(run_id.clone(), dag->compiled_graph);
  if (!run) {
    return fail(run.error());
  }

  run->set_run_rowid(run_entry->run_rowid);
  run->set_dag_rowid(run_entry->dag_rowid);
  run->set_dag_version(run_entry->dag_version);
  run->set_trigger_type(run_entry->trigger_type);
  run->set_scheduled_at(run_entry->scheduled_at);
  run->set_started_at(run_entry->started_at);
  run->set_finished_at(run_entry->finished_at);
  run->set_execution_date(run_entry->execution_date);

  for (NodeIndex idx = 0; idx < dag->compiled_indexed_task_configs->size(); ++idx) {
    const auto &task = (*dag->compiled_indexed_task_configs)[idx];
    if (task.task_rowid > 0) {
      if (auto set_rowid = run->set_task_rowid(idx, task.task_rowid); !set_rowid) {
        return set_rowid;
      }
    }
  }

  auto tasks = run_task(io, db.get_task_instances(run_id));
  if (!tasks) {
    return fail(tasks.error());
  }

  for (auto &task : *tasks) {
    task.task_idx = dag->compiled_graph->get_index(task.task_id);
    if (task.task_idx == kInvalidNode) {
      return fail(Error::NotFound);
    }
  }

  std::invoke(std::forward<Mutator>(mutate), *tasks);

  for (auto &task : *tasks) {
    if (auto restored = run->restore_task_instance(task); !restored) {
      return restored;
    }
  }

  for (auto &task : run->all_task_info()) {
    if (auto updated = run_task(io, db.update_task_instance(run_id, task)); !updated) {
      return updated;
    }
  }

  return run_task(io, db.update_dag_run_state(run_id, run->state()));
}

} // namespace

ManagementClient::ManagementClient(const DatabaseConfig &db_config)
    : db_config_(db_config), db_(io_.get_executor(), db_config_) {}

ManagementClient::~ManagementClient() {
  auto close_res = run_task(io_, db_.close());
  if (!close_res) {
    log::error("ManagementClient database close failed: {}",
               close_res.error().message());
  }
}

auto ManagementClient::open() -> Result<void> {
  return run_task(io_, db_.open());
}

auto ManagementClient::list_dags() const -> Result<std::vector<DAGInfo>> {
  return run_task(io_, db_.list_dags());
}

auto ManagementClient::get_dag(const DAGId &dag_id) const -> Result<DAGInfo> {
  return run_task(io_, db_.get_dag(dag_id));
}

auto ManagementClient::list_runs(std::size_t limit) const
    -> Result<std::vector<RunHistoryEntry>> {
  return run_task(io_, db_.list_run_history(limit));
}

auto ManagementClient::list_dag_runs(const DAGId &dag_id,
                                     std::size_t limit) const
    -> Result<std::vector<RunHistoryEntry>> {
  return run_task(io_, db_.list_dag_run_history(dag_id, limit));
}

auto ManagementClient::get_run(const DAGRunId &run_id) const
    -> Result<RunHistoryEntry> {
  return run_task(io_, db_.get_run_history(run_id));
}

auto ManagementClient::get_task_instances(const DAGRunId &run_id) const
    -> Result<std::vector<TaskInstanceInfo>> {
  return run_task(io_, db_.get_task_instances(run_id));
}

auto ManagementClient::clear_failed_tasks(const DAGRunId &run_id)
    -> Result<void> {
  return rebuild_run_after_clear(io_, db_, run_id, [](std::vector<TaskInstanceInfo> &tasks) {
    for (auto &task : tasks) {
      if (task.state == TaskState::Failed) {
        reset_task_for_clear(task);
      }
    }
  });
}

auto ManagementClient::clear_all_tasks(const DAGRunId &run_id) -> Result<void> {
  return rebuild_run_after_clear(io_, db_, run_id, [](std::vector<TaskInstanceInfo> &tasks) {
    for (auto &task : tasks) {
      reset_task_for_clear(task);
    }
  });
}

auto ManagementClient::clear_task(const DAGRunId &run_id, NodeIndex task_idx)
    -> Result<void> {
  bool found = false;
  auto result = rebuild_run_after_clear(
      io_, db_, run_id, [task_idx, &found](std::vector<TaskInstanceInfo> &tasks) {
        for (auto &task : tasks) {
          if (task.task_idx == task_idx) {
            reset_task_for_clear(task);
            found = true;
          }
        }
      });
  if (!result) {
    return result;
  }
  return found ? result : fail(Error::NotFound);
}

auto ManagementClient::set_dag_active(const DAGId &dag_id, bool active)
    -> Result<void> {
  return run_task(io_, db_.set_dag_active(dag_id, active));
}

auto ManagementClient::set_dag_paused(const DAGId &dag_id, bool paused)
    -> Result<void> {
  return run_task(io_, db_.set_dag_paused(dag_id, paused));
}

auto ManagementClient::delete_dag(const DAGId &dag_id) -> Result<void> {
  return run_task(io_, db_.delete_dag(dag_id));
}

auto ManagementClient::get_run_logs(const DAGRunId &run_id,
                                    std::size_t limit) const
    -> Result<std::vector<orm::TaskLogEntry>> {
  return run_task(io_, db_.get_run_logs(run_id, limit));
}

auto ManagementClient::get_task_logs(const DAGRunId &run_id,
                                     const TaskId &task_id, int attempt,
                                     std::size_t limit) const
    -> Result<std::vector<orm::TaskLogEntry>> {
  return run_task(io_, db_.get_task_logs(run_id, task_id, attempt, limit));
}

auto ManagementClient::get_latest_run(const DAGId &dag_id) const
    -> Result<RunHistoryEntry> {
  auto runs = run_task(io_, db_.list_dag_run_history(dag_id, 1));
  if (!runs)
    return fail(runs.error());
  if (runs->empty())
    return fail(Error::NotFound);
  return ok(std::move(runs->front()));
}

} // namespace dagforge::cli
