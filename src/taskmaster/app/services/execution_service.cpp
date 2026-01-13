#include "taskmaster/app/services/execution_service.hpp"

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/xcom/xcom_extractor.hpp"

#include <algorithm>
#include <format>
#include <ranges>

namespace taskmaster {

ExecutionService::ExecutionService(Runtime& runtime, IExecutor& executor)
    : runtime_(runtime), executor_(executor) {}

ExecutionService::~ExecutionService() = default;

auto ExecutionService::set_callbacks(ExecutionCallbacks callbacks) -> void {
  callbacks_ = std::move(callbacks);
}

auto ExecutionService::start_run(DAGRunId dag_run_id,
                                 std::unique_ptr<DAGRun> run,
                                 std::vector<ExecutorConfig> configs,
                                 std::vector<TaskConfig> task_configs) -> void {
  {
    std::lock_guard lock(mu_);
    runs_.insert_or_assign(dag_run_id.clone(), std::move(run));
    run_cfgs_.insert_or_assign(dag_run_id.clone(), std::move(configs));
    if (!task_configs.empty()) {
      task_cfgs_.insert_or_assign(dag_run_id.clone(), std::move(task_configs));
    }
  }
  dispatch(dag_run_id);
}

auto ExecutionService::get_run(const DAGRunId& dag_run_id) -> DAGRun* {
  std::lock_guard lock(mu_);
  auto it = runs_.find(dag_run_id);
  return it != runs_.end() ? it->second.get() : nullptr;
}

auto ExecutionService::has_active_runs() const -> bool {
  std::lock_guard lock(mu_);
  return std::ranges::any_of(runs_ | std::views::values,
                             [](const auto& r) { return !r->is_complete(); });
}

auto ExecutionService::wait_for_completion(int timeout_ms) -> void {
  std::unique_lock lock(mu_);
  done_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                    [this] {
                      return std::ranges::none_of(
                          runs_ | std::views::values,
                          [](const auto& r) { return !r->is_complete(); });
                    });
}

auto ExecutionService::coro_count() const -> int {
  return coro_count_.load();
}

auto ExecutionService::dispatch(const DAGRunId& dag_run_id) -> void {
  std::vector<TaskJob> jobs;

  {
    std::lock_guard lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end()) [[unlikely]] {
      log::debug("dispatch: run {} not found", dag_run_id);
      return;
    }

    auto cfg_it = run_cfgs_.find(dag_run_id);
    if (cfg_it == run_cfgs_.end()) [[unlikely]] {
      log::debug("dispatch: run_cfgs for {} not found", dag_run_id);
      return;
    }

    auto& run = *it->second;
    const auto& g = run.dag();
    const auto& task_cfgs = cfg_it->second;

    auto ready = run.get_ready_tasks();
    if (!ready.empty()) {
      log::info("dispatch: run {} has {} ready tasks", dag_run_id, ready.size());
    } else {
      log::debug("dispatch: run {} has 0 ready tasks", dag_run_id);
    }

    for (NodeIndex idx : ready) {
      if (idx >= task_cfgs.size()) [[unlikely]] {
        log::error("Config not found for task {}", idx);
        continue;
      }

      TaskId task_id{std::string{g.get_key(idx)}};
      InstanceId inst_id = generate_instance_id(dag_run_id, task_id);

      run.mark_task_started(idx, inst_id);

      int attempt = 1;
      if (auto info = run.get_task_info(idx)) {
        attempt = info->attempt;
      }

      std::vector<XComPushConfig> xcom_push;
      auto tcfg_it = task_cfgs_.find(dag_run_id);
      if (tcfg_it != task_cfgs_.end() && idx < tcfg_it->second.size()) {
        xcom_push = tcfg_it->second[idx].xcom_push;
      }

      jobs.push_back({idx, task_id, inst_id, cfg_it->second[idx], std::move(xcom_push), attempt});
      log::debug("dispatch: scheduled task {} (idx={}, attempt={})",
                 task_id, idx, attempt);
    }

    if (!jobs.empty() && callbacks_.on_persist_run) {
      callbacks_.on_persist_run(run);
      for (const auto& job : jobs) {
        if (auto info = run.get_task_info(job.idx)) {
          if (callbacks_.on_persist_task) {
            callbacks_.on_persist_task(dag_run_id, *info);
          }
        }
      }
    }
  }

  for (auto& job : jobs) {
    log::info("Executing task {} (idx={}, inst_id={})", job.task_id, job.idx,
              job.inst_id);
    auto coro = run_task(dag_run_id, std::move(job));
    runtime_.schedule_external(coro.take());
  }
}

auto ExecutionService::run_task(DAGRunId dag_run_id, TaskJob job) -> spawn_task {
  ++coro_count_;
  struct Guard {
    std::atomic<int>& c;
    ~Guard() { --c; }
  } guard{coro_count_};

  log::debug("run_task: starting {} (idx={}, attempt={})", job.task_id, job.idx,
             job.attempt);
  log::info("run_task: dag_run_id={} task={} inst_id={} attempt={}", dag_run_id,
            job.task_id, job.inst_id, job.attempt);

  std::string start_msg;
  if (job.attempt > 1) {
    start_msg = std::format("Task '{}' starting (attempt {})", job.task_id.value(), job.attempt);
  } else {
    start_msg = std::format("Task '{}' starting", job.task_id.value());
  }

  if (callbacks_.on_log) {
    callbacks_.on_log(dag_run_id, job.task_id, "stdout", start_msg);
  }
  if (callbacks_.on_persist_log) {
    callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "INFO", start_msg);
  }
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(dag_run_id, job.task_id, "running");
  }

  auto result = co_await execute_async(runtime_, executor_, job.inst_id,
                                         job.cfg);
  log::info("run_task: completed dag_run_id={} task={} inst_id={} exit_code={} err='{}'",
            dag_run_id, job.task_id, job.inst_id, result.exit_code, result.error);

  if (!result.stdout_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stdout", result.stdout_output);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "INFO",
                                result.stdout_output);
    }
  }
  if (!result.stderr_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stderr", result.stderr_output);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "ERROR",
                                result.stderr_output);
    }
  }

  if (result.exit_code == 0 && result.error.empty()) {
    if (!job.xcom_push.empty()) {
      XComExtractor extractor;
      auto xcoms = extractor.extract(result, job.xcom_push);
      if (xcoms) {
        for (const auto& xcom : *xcoms) {
          if (callbacks_.on_persist_xcom) {
            callbacks_.on_persist_xcom(dag_run_id, job.task_id, xcom.key, xcom.value);
          }
        }
      }
    }

    std::string success_msg = std::format("Task '{}' completed successfully", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stdout", success_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "INFO",
                                success_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, "success");
    }
    log::debug("run_task: calling on_task_success for {} (idx={})", job.task_id,
               job.idx);
    on_task_success(dag_run_id, job.idx);
  } else {
    std::string error_msg = std::format("Task '{}' failed: {}", job.task_id.value(), result.error);
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stderr", error_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "ERROR",
                                error_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, "failed");
    }

    if (on_task_failure(dag_run_id, job.idx, result.error, result.exit_code)) {
      auto retry = dispatch_after_yield(dag_run_id);
      runtime_.schedule_external(retry.take());
    }
  }
}

auto ExecutionService::dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task {
  co_await async_yield();
  dispatch(dag_run_id);
}

auto ExecutionService::on_task_success(const DAGRunId& dag_run_id, NodeIndex idx)
    -> void {
  bool completed = false;
  log::info("on_task_success: dag_run_id={} idx={}", dag_run_id, idx);

  {
    std::lock_guard lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end())
      return;

    auto& run = *it->second;
    run.mark_task_completed(idx, 0);

    if (auto info = run.get_task_info(idx)) {
      if (callbacks_.on_persist_task) {
        callbacks_.on_persist_task(dag_run_id, *info);
      }
    }

    if (run.is_complete()) {
      on_run_complete(run, dag_run_id);
      runs_.erase(it);
      run_cfgs_.erase(dag_run_id);
      completed = true;
    }
  }

  if (completed) {
    done_cv_.notify_all();
  } else {
    dispatch(dag_run_id);
  }
}

auto ExecutionService::on_task_failure(const DAGRunId& dag_run_id, NodeIndex idx,
                                        const std::string& error, int exit_code) -> bool {
  log::info("on_task_failure: dag_run_id={} idx={} err='{}'", dag_run_id, idx, error);
  std::lock_guard lock(mu_);
  auto it = runs_.find(dag_run_id);
  if (it == runs_.end())
    return false;

  auto& run = *it->second;

  int max_retries = 3;
  if (callbacks_.get_max_retries) {
    max_retries = callbacks_.get_max_retries(dag_run_id, idx);
  }

  run.mark_task_failed(idx, error, max_retries, exit_code);

  bool needs_retry = false;
  if (auto info = run.get_task_info(idx)) {
    needs_retry = (info->state == TaskState::Pending);
  }

  for (const auto& info : run.all_task_info()) {
    if (info.state == TaskState::Failed ||
        info.state == TaskState::UpstreamFailed ||
        info.state == TaskState::Pending) {
      if (callbacks_.on_persist_task) {
        callbacks_.on_persist_task(dag_run_id, info);
      }
    }
  }

  if (run.is_complete()) {
    on_run_complete(run, dag_run_id);
    runs_.erase(it);
    run_cfgs_.erase(dag_run_id);
    done_cv_.notify_all();
    return false;
  }

  return needs_retry;
}

auto ExecutionService::on_run_complete(DAGRun& run, const DAGRunId& dag_run_id)
    -> void {
  if (callbacks_.on_persist_run) {
    callbacks_.on_persist_run(run);
  }
  std::string status =
      run.state() == DAGRunState::Success ? "success" : "failed";
  if (callbacks_.on_run_status) {
    callbacks_.on_run_status(dag_run_id, status);
  }
  log::info("DAG run {} {}", dag_run_id, status);
}

}  // namespace taskmaster
