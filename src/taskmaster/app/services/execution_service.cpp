#include "taskmaster/app/services/execution_service.hpp"

#include "taskmaster/core/runtime.hpp"
#include "taskmaster/util/log.hpp"

#include <algorithm>
#include <ranges>

namespace taskmaster {

ExecutionService::ExecutionService(Runtime& runtime, IExecutor& executor)
    : runtime_(runtime), executor_(executor) {}

ExecutionService::~ExecutionService() = default;

auto ExecutionService::set_callbacks(ExecutionCallbacks callbacks) -> void {
  callbacks_ = std::move(callbacks);
}

auto ExecutionService::start_run(std::string run_id,
                                 std::unique_ptr<DAGRun> run,
                                 std::vector<ExecutorConfig> configs) -> void {
  {
    std::lock_guard lock(mu_);
    runs_[run_id] = std::move(run);
    run_cfgs_[run_id] = std::move(configs);
  }
  dispatch(run_id);
}

auto ExecutionService::get_run(std::string_view run_id) -> DAGRun* {
  std::lock_guard lock(mu_);
  auto it = runs_.find(std::string(run_id));
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

auto ExecutionService::dispatch(const std::string& run_id) -> void {
  std::vector<TaskJob> jobs;

  {
    std::lock_guard lock(mu_);
    auto it = runs_.find(run_id);
    if (it == runs_.end()) {
      log::debug("dispatch: run {} not found", run_id);
      return;
    }

    auto cfg_it = run_cfgs_.find(run_id);
    if (cfg_it == run_cfgs_.end()) {
      log::debug("dispatch: run_cfgs for {} not found", run_id);
      return;
    }

    auto& run = *it->second;
    const auto& g = run.dag();
    const auto& task_cfgs = cfg_it->second;

    auto ready = run.get_ready_tasks();
    log::debug("dispatch: run {} has {} ready tasks", run_id, ready.size());

    for (NodeIndex idx : ready) {
      if (idx >= task_cfgs.size()) {
        log::error("Config not found for task {}", idx);
        continue;
      }

      std::string name{g.get_key(idx)};
      std::string inst_id = run_id + "_" + name;

      run.mark_task_started(idx, inst_id);

      int attempt = 1;
      if (auto info = run.get_task_info(idx)) {
        attempt = info->attempt;
      }

      jobs.push_back(
          {idx, std::move(name), std::move(inst_id), task_cfgs[idx], attempt});
      log::debug("dispatch: scheduled task {} (idx={}, attempt={})",
                 jobs.back().name, idx, attempt);
    }

    if (!jobs.empty() && callbacks_.on_persist_run) {
      callbacks_.on_persist_run(run);
      for (const auto& job : jobs) {
        if (auto info = run.get_task_info(job.idx)) {
          if (callbacks_.on_persist_task) {
            callbacks_.on_persist_task(run_id, *info);
          }
        }
      }
    }
  }

  for (auto& job : jobs) {
    log::debug("Executing task {} (idx={})", job.name, job.idx);
    auto coro = run_task(run_id, std::move(job));
    runtime_.schedule_external(coro.take());
  }
}

auto ExecutionService::run_task(std::string run_id, TaskJob job) -> spawn_task {
  ++coro_count_;
  struct Guard {
    std::atomic<int>& c;
    ~Guard() { --c; }
  } guard{coro_count_};

  log::debug("run_task: starting {} (idx={}, attempt={})", job.name, job.idx,
             job.attempt);

  std::string start_msg;
  if (job.attempt > 1) {
    start_msg = "Task '" + job.name + "' starting (attempt " +
                std::to_string(job.attempt) + ")";
  } else {
    start_msg = "Task '" + job.name + "' starting";
  }

  if (callbacks_.on_log) {
    callbacks_.on_log(run_id, job.name, "stdout", start_msg);
  }
  if (callbacks_.on_persist_log) {
    callbacks_.on_persist_log(run_id, job.name, job.attempt, "INFO", start_msg);
  }
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(run_id, job.name, "running");
  }

  auto result = co_await execute_async(executor_, job.inst_id, job.cfg);
  log::debug("run_task: {} completed with exit_code={}", job.name,
             result.exit_code);

  if (!result.stdout_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(run_id, job.name, "stdout", result.stdout_output);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(run_id, job.name, job.attempt, "INFO",
                                result.stdout_output);
    }
  }
  if (!result.stderr_output.empty()) {
    if (callbacks_.on_log) {
      callbacks_.on_log(run_id, job.name, "stderr", result.stderr_output);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(run_id, job.name, job.attempt, "ERROR",
                                result.stderr_output);
    }
  }

  if (result.exit_code == 0 && result.error.empty()) {
    std::string success_msg = "Task '" + job.name + "' completed successfully";
    if (callbacks_.on_log) {
      callbacks_.on_log(run_id, job.name, "stdout", success_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(run_id, job.name, job.attempt, "INFO",
                                success_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(run_id, job.name, "success");
    }
    log::debug("run_task: calling on_task_success for {} (idx={})", job.name,
               job.idx);
    on_task_success(run_id, job.idx);
  } else {
    std::string error_msg = "Task '" + job.name + "' failed: " + result.error;
    if (callbacks_.on_log) {
      callbacks_.on_log(run_id, job.name, "stderr", error_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(run_id, job.name, job.attempt, "ERROR",
                                error_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(run_id, job.name, "failed");
    }

    if (on_task_failure(run_id, job.idx, result.error)) {
      auto retry = [](ExecutionService* self, std::string rid) -> spawn_task {
        co_await async_yield();
        self->dispatch(rid);
      }(this, run_id);
      runtime_.schedule_external(retry.take());
    }
  }
}

auto ExecutionService::on_task_success(const std::string& run_id, NodeIndex idx)
    -> void {
  bool completed = false;

  {
    std::lock_guard lock(mu_);
    auto it = runs_.find(run_id);
    if (it == runs_.end())
      return;

    auto& run = *it->second;
    run.mark_task_completed(idx, 0);

    if (auto info = run.get_task_info(idx)) {
      if (callbacks_.on_persist_task) {
        callbacks_.on_persist_task(run_id, *info);
      }
    }

    if (run.is_complete()) {
      on_run_complete(run, run_id);
      completed = true;
    }
  }

  if (completed) {
    done_cv_.notify_all();
  } else {
    dispatch(run_id);
  }
}

auto ExecutionService::on_task_failure(const std::string& run_id, NodeIndex idx,
                                       const std::string& error) -> bool {
  std::lock_guard lock(mu_);
  auto it = runs_.find(run_id);
  if (it == runs_.end())
    return false;

  auto& run = *it->second;

  int max_retries = 3;
  if (callbacks_.get_max_retries) {
    max_retries = callbacks_.get_max_retries(run_id, idx);
  }

  run.mark_task_failed(idx, error, max_retries);

  bool needs_retry = false;
  if (auto info = run.get_task_info(idx)) {
    needs_retry = (info->state == TaskState::Pending);
  }

  for (const auto& info : run.all_task_info()) {
    if (info.state == TaskState::Failed ||
        info.state == TaskState::UpstreamFailed ||
        info.state == TaskState::Pending) {
      if (callbacks_.on_persist_task) {
        callbacks_.on_persist_task(run_id, info);
      }
    }
  }

  if (run.is_complete()) {
    on_run_complete(run, run_id);
    done_cv_.notify_all();
    return false;
  }

  return needs_retry;
}

auto ExecutionService::on_run_complete(DAGRun& run, const std::string& run_id)
    -> void {
  if (callbacks_.on_persist_run) {
    callbacks_.on_persist_run(run);
  }
  std::string status =
      run.state() == DAGRunState::Success ? "success" : "failed";
  if (callbacks_.on_run_status) {
    callbacks_.on_run_status(run_id, status);
  }
  log::info("DAG run {} {}", run_id, status);
}

}  // namespace taskmaster
