#include "taskmaster/app/services/execution_service.hpp"

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/storage/state_strings.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/xcom/xcom_extractor.hpp"

#include <algorithm>
#include <format>
#include <ranges>

namespace taskmaster {

ExecutionService::ExecutionService(Runtime& runtime, IExecutor& executor)
    : runtime_(runtime), executor_(executor) {}

ExecutionService::~ExecutionService() = default;

auto ExecutionService::set_max_concurrency(int max_concurrency) -> void {
  max_concurrency_ = max_concurrency;
}

auto ExecutionService::dispatch_pending() -> void {
  std::vector<DAGRunId> active_ids;
  {
    std::scoped_lock lock(mu_);
    for (const auto& [id, run] : runs_) {
      if (!run->is_complete()) {
        active_ids.push_back(id);
      }
    }
  }
  for (const auto& id : active_ids) {
    if (running_tasks_ >= max_concurrency_) break;
    dispatch(id);
  }
}

auto ExecutionService::set_callbacks(ExecutionCallbacks callbacks) -> void {
  callbacks_ = std::move(callbacks);
}

auto ExecutionService::start_run(DAGRunId dag_run_id,
                                 std::unique_ptr<DAGRun> run,
                                 std::vector<ExecutorConfig> configs,
                                 std::vector<TaskConfig> task_configs) -> void {
  {
    std::scoped_lock lock(mu_);
    runs_.insert_or_assign(dag_run_id.clone(), std::move(run));
    run_cfgs_.insert_or_assign(dag_run_id.clone(), std::move(configs));
    if (!task_configs.empty()) {
      task_cfgs_.insert_or_assign(dag_run_id.clone(), std::move(task_configs));
    }
  }
  dispatch(dag_run_id);
}

auto ExecutionService::get_run(const DAGRunId& dag_run_id) -> DAGRun* {
  std::scoped_lock lock(mu_);
  auto it = runs_.find(dag_run_id);
  return it != runs_.end() ? it->second.get() : nullptr;
}

auto ExecutionService::has_active_runs() const -> bool {
  std::scoped_lock lock(mu_);
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
  std::vector<NodeIndex> depends_on_past_blocked;

  {
    std::scoped_lock lock(mu_);
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
      if (running_tasks_ >= max_concurrency_) {
        log::debug("Max concurrency reached ({}/{})", running_tasks_.load(), max_concurrency_);
        break;
      }

      if (idx >= task_cfgs.size()) [[unlikely]] {
        log::error("Config not found for task {}", idx);
        continue;
      }

      bool depends_on_past = false;
      auto tcfg_it = task_cfgs_.find(dag_run_id);
      if (tcfg_it != task_cfgs_.end() && idx < tcfg_it->second.size()) {
        depends_on_past = tcfg_it->second[idx].depends_on_past;
      }

      if (depends_on_past) {
        if (!callbacks_.check_previous_task_state) {
          log::error("dispatch: task {} has depends_on_past but no callback configured", idx);
          depends_on_past_blocked.push_back(idx);
          continue;
        }
        auto prev_state = callbacks_.check_previous_task_state(dag_run_id, idx, run.execution_date(), dag_run_id.value());
        if (!prev_state) {
          log::error("dispatch: task {} blocked by depends_on_past (persistence error)", idx);
          depends_on_past_blocked.push_back(idx);
          continue;
        }
        if (prev_state->has_value()) {
          auto state = **prev_state;
          if (state != TaskState::Success && state != TaskState::Skipped) {
            log::info("dispatch: task {} blocked by depends_on_past (previous state: {})",
                      idx, task_state_name(state));
            depends_on_past_blocked.push_back(idx);
            continue;
          }
        }
      }

      TaskId task_id{std::string{g.get_key(idx)}};
      InstanceId inst_id = generate_instance_id(dag_run_id, task_id);

      run.mark_task_started(idx, inst_id);

      int attempt = 1;
      if (auto info = run.get_task_info(idx)) {
        attempt = info->attempt;
      }

      std::vector<XComPushConfig> xcom_push;
      std::vector<XComPullConfig> xcom_pull;
      if (tcfg_it != task_cfgs_.end() && idx < tcfg_it->second.size()) {
        xcom_push = tcfg_it->second[idx].xcom_push;
        xcom_pull = tcfg_it->second[idx].xcom_pull;
      }

      jobs.push_back({idx, task_id, inst_id, cfg_it->second[idx], std::move(xcom_push), std::move(xcom_pull), attempt});
      running_tasks_++;
      log::debug("dispatch: scheduled task {} (idx={}, attempt={})",
                 task_id, idx, attempt);
    }

    for (NodeIndex idx : depends_on_past_blocked) {
      run.mark_task_skipped(idx);
    }

    if (!jobs.empty() && callbacks_.on_persist_run) {
      callbacks_.on_persist_run(run);
      for (const auto& job : jobs) {
        if (auto info = run.get_task_info(job.idx); info && callbacks_.on_persist_task) {
          callbacks_.on_persist_task(dag_run_id, *info);
        }
      }
    }

    if (!depends_on_past_blocked.empty() && callbacks_.on_persist_run) {
      for (NodeIndex idx : depends_on_past_blocked) {
        if (auto info = run.get_task_info(idx); info && callbacks_.on_persist_task) {
          callbacks_.on_persist_task(dag_run_id, *info);
        }
      }
      callbacks_.on_persist_run(run);
      
      if (run.is_complete()) {
        on_run_complete(run, dag_run_id);
      }
    }
  }

  for (auto& job : jobs) {
    log::info("Executing task {} (idx={}, inst_id={})", job.task_id, job.idx,
              job.inst_id);
    auto coro = run_task(dag_run_id, std::move(job));
    runtime_.schedule_external(coro.take());
  }

  if (!depends_on_past_blocked.empty()) {
    auto redispatch = dispatch_after_yield(dag_run_id.clone());
    runtime_.schedule_external(redispatch.take());
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

  // Simple template substitution for ShellExecutor
  if (auto* shell_cfg = std::get_if<ShellExecutorConfig>(&job.cfg)) {
    std::string& cmd = shell_cfg->command;
    auto replace_all = [&](std::string_view key, std::string_view value) {
      size_t pos = 0;
      while ((pos = cmd.find(key, pos)) != std::string::npos) {
        cmd.replace(pos, key.length(), value);
        pos += value.length();
      }
    };

    auto dag_id = extract_dag_id(dag_run_id);
    replace_all("{{dag_id}}", dag_id.value());
    replace_all("{{task_id}}", job.task_id.value());
    replace_all("{{run_id}}", dag_run_id.value());
    replace_all("{{dag_run_id}}", dag_run_id.value());
  }

  if (!job.xcom_pull.empty() && callbacks_.get_xcom) {
    auto* shell_cfg = std::get_if<ShellExecutorConfig>(&job.cfg);
    if (shell_cfg) {
      for (const auto& pull : job.xcom_pull) {
        auto xcom_result = callbacks_.get_xcom(dag_run_id, pull.source_task, pull.key);
        if (xcom_result) {
          std::string value;
          if (xcom_result->is_string()) {
            value = xcom_result->get<std::string>();
          } else if (xcom_result->is_number_integer()) {
            value = std::to_string(xcom_result->get<int64_t>());
          } else if (xcom_result->is_number_float()) {
            value = std::to_string(xcom_result->get<double>());
          } else if (xcom_result->is_boolean()) {
            value = xcom_result->get<bool>() ? "true" : "false";
          } else {
            value = xcom_result->dump();
          }
          shell_cfg->env[pull.env_var] = std::move(value);
          log::debug("xcom_pull: {}={} from task {}", pull.env_var, 
                     shell_cfg->env[pull.env_var], pull.source_task.value());
        } else {
          log::warn("xcom_pull: failed to get key '{}' from task '{}'",
                    pull.key, pull.source_task.value());
        }
      }
    }
  }

  auto result = co_await execute_async(runtime_, executor_, job.inst_id,
                                         job.cfg);
  running_tasks_--;
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
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "INFO",
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
  } else if (result.exit_code == 100) {
    std::string skip_msg = std::format("Task '{}' skipped (exit code 100)", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stdout", skip_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "INFO", skip_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, "skipped");
    }
    on_task_skipped(dag_run_id, job.idx);
  } else if (result.exit_code == 101) {
    std::string fail_msg = std::format("Task '{}' failed immediately (exit code 101)", job.task_id.value());
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, job.task_id, "stderr", fail_msg);
    }
    if (callbacks_.on_persist_log) {
      callbacks_.on_persist_log(dag_run_id, job.task_id, job.attempt, "ERROR", fail_msg);
    }
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, job.task_id, "failed");
    }
    on_task_fail_immediately(dag_run_id, job.idx, result.error, result.exit_code);
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
      auto retry_interval = std::chrono::seconds(60);
      if (callbacks_.get_retry_interval) {
        retry_interval = callbacks_.get_retry_interval(dag_run_id, job.idx);
      }
      auto retry = dispatch_after_delay(dag_run_id, retry_interval);
      runtime_.schedule_external(retry.take());
    }
  }
}

auto ExecutionService::dispatch_after_delay(DAGRunId dag_run_id,
                                             std::chrono::seconds delay) -> spawn_task {
  co_await async_sleep(delay);
  dispatch(dag_run_id);
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
    std::scoped_lock lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end())
      return;

    auto& run = *it->second;
    
    bool is_branch = false;
    std::string branch_key = "branch";
    {
      auto tcfg_it = task_cfgs_.find(dag_run_id);
      if (tcfg_it != task_cfgs_.end() && idx < tcfg_it->second.size()) {
        is_branch = tcfg_it->second[idx].is_branch;
        branch_key = tcfg_it->second[idx].branch_xcom_key;
      }
    }

    if (is_branch && callbacks_.get_xcom) {
      auto xcom = callbacks_.get_xcom(dag_run_id, 
          TaskId{std::string(run.dag().get_key(idx))}, branch_key);
      std::vector<TaskId> selected;
      if (xcom && xcom->is_array()) {
        for (const auto& item : *xcom) {
          if (item.is_string()) {
            selected.push_back(TaskId{item.get<std::string>()});
          }
        }
      }
      run.mark_task_completed_with_branch(idx, 0, selected);
    } else {
      run.mark_task_completed(idx, 0);
    }

    if (auto info = run.get_task_info(idx); info && callbacks_.on_persist_task) {
      callbacks_.on_persist_task(dag_run_id, *info);
    }

    if (is_branch && callbacks_.on_persist_task) {
      for (const auto& info : run.all_task_info()) {
        if (info.state == TaskState::Skipped || info.state == TaskState::UpstreamFailed) {
          callbacks_.on_persist_task(dag_run_id, info);
          if (callbacks_.on_task_status) {
            TaskId tid{std::string(run.dag().get_key(info.task_idx))};
            callbacks_.on_task_status(dag_run_id, tid, task_state_name(info.state));
          }
        }
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
    dispatch_pending();
  }
}

auto ExecutionService::on_task_failure(const DAGRunId& dag_run_id, NodeIndex idx,
                                        const std::string& error, int exit_code) -> bool {
  log::info("on_task_failure: dag_run_id={} idx={} err='{}'", dag_run_id, idx, error);
  
  bool needs_retry = false;
  bool run_complete = false;
  
  {
    std::scoped_lock lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end())
      return false;

    auto& run = *it->second;

    int max_retries = 3;
    if (callbacks_.get_max_retries) {
      max_retries = callbacks_.get_max_retries(dag_run_id, idx);
    }

    run.mark_task_failed(idx, error, max_retries, exit_code);

    if (auto info = run.get_task_info(idx)) {
      needs_retry = (info->state == TaskState::Pending);
    }

    for (const auto& info : run.all_task_info()) {
      if ((info.state == TaskState::Failed ||
           info.state == TaskState::UpstreamFailed ||
           info.state == TaskState::Pending) &&
          callbacks_.on_persist_task) {
        callbacks_.on_persist_task(dag_run_id, info);
      }
    }

    if (run.is_complete()) {
      on_run_complete(run, dag_run_id);
      runs_.erase(it);
      run_cfgs_.erase(dag_run_id);
      done_cv_.notify_all();
      run_complete = true;
    }
  }

  if (run_complete) {
    return false;
  }

  if (!needs_retry) {
    dispatch_pending();
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

auto ExecutionService::on_task_skipped(const DAGRunId& dag_run_id, NodeIndex idx)
    -> void {
  log::info("on_task_skipped: dag_run_id={} idx={}", dag_run_id, idx);
  
  bool completed = false;
  
  {
    std::scoped_lock lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end())
      return;

    auto& run = *it->second;
    run.mark_task_skipped(idx);

    if (auto info = run.get_task_info(idx); info && callbacks_.on_persist_task) {
      callbacks_.on_persist_task(dag_run_id, *info);
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
    dispatch_pending();
  }
}

auto ExecutionService::on_task_fail_immediately(const DAGRunId& dag_run_id,
                                                 NodeIndex idx,
                                                 const std::string& error,
                                                 int exit_code) -> void {
  log::info("on_task_fail_immediately: dag_run_id={} idx={} err='{}'", 
            dag_run_id, idx, error);
  
  bool completed = false;
  
  {
    std::scoped_lock lock(mu_);
    auto it = runs_.find(dag_run_id);
    if (it == runs_.end())
      return;

    auto& run = *it->second;
    run.mark_task_failed(idx, error, 0, exit_code);

    if (auto info = run.get_task_info(idx); info && callbacks_.on_persist_task) {
      callbacks_.on_persist_task(dag_run_id, *info);
    }

    for (const auto& info : run.all_task_info()) {
      if (info.state == TaskState::UpstreamFailed && callbacks_.on_persist_task) {
        callbacks_.on_persist_task(dag_run_id, info);
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
    dispatch_pending();
  }
}

}  // namespace taskmaster
