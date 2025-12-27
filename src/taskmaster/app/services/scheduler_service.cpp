#include "taskmaster/app/services/scheduler_service.hpp"

#include "taskmaster/scheduler/cron.hpp"
#include "taskmaster/util/log.hpp"

#include <ranges>

namespace taskmaster {

auto SchedulerService::set_on_ready(EngineReadyCallback callback) -> void {
  on_ready_ = std::move(callback);
  engine_.set_on_ready_callback([this](const TaskInstance& inst) {
    if (on_ready_) {
      on_ready_(inst);
    }
  });
}

auto SchedulerService::init_from_config(const std::vector<TaskConfig>& tasks,
                                        const std::vector<ExecutorConfig>& cfgs)
    -> Result<void> {
  for (auto [idx, tc] : std::views::enumerate(tasks)) {
    // Only register root tasks (no dependencies)
    if (!tc.deps.empty())
      continue;

    TaskDefinition def;
    def.id = tc.id;
    def.name = tc.name;
    def.executor = cfgs[idx];
    def.retry.max_attempts = tc.max_retries;
    def.retry.delay = tc.retry_interval;
    def.enabled = tc.enabled;

    if (!engine_.add_task(std::move(def))) {
      log::warn("Failed to add task {} to engine", tc.id);
    }
  }
  return ok();
}

auto SchedulerService::register_task(std::string_view dag_id,
                                     const TaskConfig& task) -> void {
  // Only register root tasks
  if (!task.deps.empty())
    return;

  std::string id = std::string(dag_id) + ":" + task.id;

  TaskDefinition def;
  def.id = id;
  def.name = task.name.empty() ? task.id : task.name;

  ShellExecutorConfig exec;
  exec.command = task.command;
  exec.working_dir = task.working_dir;
  exec.timeout = task.timeout;
  def.executor = exec;

  def.retry.max_attempts = task.max_retries;
  def.retry.delay = task.retry_interval;
  def.enabled = task.enabled;

  if (!engine_.add_task(std::move(def))) {
    log::warn("Failed to register task {}", id);
  } else {
    log::info("Registered task: {}", id);
  }
}

auto SchedulerService::unregister_task(std::string_view dag_id,
                                       std::string_view task_id) -> void {
  std::string id = std::string(dag_id) + ":" + std::string(task_id);
  if (!engine_.remove_task(id)) {
    log::warn("Failed to unregister task {}", id);
  } else {
    log::info("Unregistered task {}", id);
  }
}

auto SchedulerService::enable_task(std::string_view dag_id,
                                   std::string_view task_id, bool enabled)
    -> bool {
  std::string id = std::string(dag_id) + ":" + std::string(task_id);
  return engine_.enable_task(id, enabled);
}

auto SchedulerService::trigger_task(std::string_view dag_id,
                                    std::string_view task_id) -> bool {
  std::string id = std::string(dag_id) + ":" + std::string(task_id);
  return engine_.trigger(id);
}

auto SchedulerService::register_dag_cron(std::string_view dag_id,
                                         std::string_view cron_expr) -> bool {
  if (cron_expr.empty()) {
    return false;
  }

  auto cron_result = CronExpr::parse(cron_expr);
  if (!cron_result) {
    log::error("Failed to parse cron expression '{}' for DAG {}", cron_expr,
               dag_id);
    return false;
  }

  // Use dag_id as the task_id for cron-triggered DAGs
  // The format is "cron:<dag_id>" to distinguish from task triggers
  std::string id = "cron:" + std::string(dag_id);

  TaskDefinition def;
  def.id = id;
  def.name = std::string(dag_id) + " (cron)";
  def.schedule = std::move(*cron_result);
  def.enabled = true;

  // No executor config needed - we'll handle DAG triggering in the callback
  ShellExecutorConfig exec;
  exec.command = "";  // Empty command - handled specially
  def.executor = exec;

  if (!engine_.add_task(std::move(def))) {
    log::warn("Failed to register cron schedule for DAG {}", dag_id);
    return false;
  }

  log::info("Registered cron schedule '{}' for DAG {}", cron_expr, dag_id);
  return true;
}

auto SchedulerService::unregister_dag_cron(std::string_view dag_id) -> void {
  std::string id = "cron:" + std::string(dag_id);
  if (!engine_.remove_task(id)) {
    log::warn("Failed to unregister cron schedule for DAG {}", dag_id);
  } else {
    log::info("Unregistered cron schedule for DAG {}", dag_id);
  }
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
