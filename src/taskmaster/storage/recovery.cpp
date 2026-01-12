#include "taskmaster/storage/recovery.hpp"

#include "taskmaster/util/log.hpp"

#include <ranges>

namespace taskmaster {

Recovery::Recovery(Persistence& persistence) : persistence_(persistence) {
}

auto Recovery::recover(
    std::move_only_function<TaskDAG(DAGRunId)> dag_provider,
    std::move_only_function<void(DAGRunId, DAGRun&)> on_recovered)
    -> Result<RecoveryResult> {
  RecoveryResult result;

  auto incomplete_runs_result = persistence_.get_incomplete_dag_runs();
  if (!incomplete_runs_result) {
    log::error("Failed to get incomplete DAG runs");
    return fail(incomplete_runs_result.error());
  }
  auto& incomplete_runs = *incomplete_runs_result;
  log::info("Found {} incomplete DAG runs to recover", incomplete_runs.size());

  for (const auto& dag_run_id : incomplete_runs) {
    log::info("Recovering DAG run: {}", dag_run_id);

    auto task_instances_result = persistence_.get_task_instances(dag_run_id);
    if (!task_instances_result || task_instances_result->empty()) {
      log::warn("No task instances found for DAG run {}", dag_run_id);
      result.failed_dag_runs.push_back(dag_run_id);
      continue;
    }
    auto& task_instances = *task_instances_result;

    TaskDAG dag = dag_provider(dag_run_id);
    if (dag.empty()) {
      log::warn("Could not reconstruct DAG for run {}", dag_run_id);
      result.failed_dag_runs.push_back(dag_run_id);
      continue;
    }

    auto run_result = DAGRun::create(DAGRunId{dag_run_id}, dag);
    if (!run_result) {
      log::warn("Failed to create DAGRun for {}", dag_run_id);
      result.failed_dag_runs.push_back(dag_run_id);
      continue;
    }
    DAGRun run = std::move(*run_result);

    for (const auto& info : task_instances) {
      if (info.state == TaskState::Success) {
        run.mark_task_completed(info.task_idx, info.exit_code);
      } else if (info.state == TaskState::Running) {
        log::info("Task {} was running during crash, marking for retry",
                  info.task_idx);
        result.tasks_to_retry++;

        TaskInstanceInfo updated_info = info;
        updated_info.state = TaskState::Pending;
        updated_info.error_message = "Recovered after crash";
        if (auto r =
                persistence_.update_task_instance(dag_run_id, updated_info);
            !r) {
          log::warn("Failed to update task instance during recovery: {}",
                    r.error().message());
        }
      } else if (info.state == TaskState::Failed) {
        run.mark_task_failed(info.task_idx, info.error_message, 0);
      }
    }

    if (!run.is_complete()) {
      result.recovered_dag_runs.push_back(dag_run_id);
      on_recovered(dag_run_id, run);
    }
  }

  log::info(
      "Recovery complete: {} runs recovered, {} failed, {} tasks to retry",
      result.recovered_dag_runs.size(), result.failed_dag_runs.size(),
      result.tasks_to_retry);

  return ok(std::move(result));
}

auto Recovery::mark_running_as_failed(Persistence& persistence,
                                      DAGRunId dag_run_id)
    -> Result<void> {
  auto task_instances_result = persistence.get_task_instances(dag_run_id);
  if (!task_instances_result) {
    return fail(task_instances_result.error());
  }
  auto& task_instances = *task_instances_result;

  auto running_tasks =
      task_instances | std::views::filter([](const auto& info) {
        return info.state == TaskState::Running;
      });

  for (const auto& info : running_tasks) {
    TaskInstanceInfo updated_info = info;
    updated_info.state = TaskState::Failed;
    updated_info.error_message = "Scheduler crashed during execution";
    updated_info.finished_at = std::chrono::system_clock::now();
    if (auto r = persistence.update_task_instance(dag_run_id, updated_info);
        !r) {
      return r;
    }
  }

  return persistence.update_dag_run_state(dag_run_id, DAGRunState::Failed);
}

}  // namespace taskmaster
