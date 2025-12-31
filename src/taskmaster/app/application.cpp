#include "taskmaster/app/application.hpp"

#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/app/services/event_service.hpp"
#include "taskmaster/app/services/execution_service.hpp"
#include "taskmaster/app/services/persistence_service.hpp"
#include "taskmaster/app/services/scheduler_service.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/recovery.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"

#include <format>
#include <print>

namespace taskmaster {

Application::Application()
    : events_(std::make_unique<EventService>()),
      scheduler_(std::make_unique<SchedulerService>()) {
}

Application::Application(std::string_view db_path)
    : persistence_(std::make_unique<PersistenceService>(db_path)),
      events_(std::make_unique<EventService>()),
      scheduler_(std::make_unique<SchedulerService>()) {
  dag_manager_.set_persistence(persistence_->persistence());
}

Application::~Application() {
  stop();
}

auto Application::load_config(std::string_view path) -> Result<void> {
  auto cfg = ConfigLoader::load_from_file(path);
  if (!cfg)
    return fail(cfg.error());
  config_ = std::move(*cfg);
  return ok();
}

auto Application::load_config_string(std::string_view json) -> Result<void> {
  auto cfg = ConfigLoader::load_from_string(json);
  if (!cfg)
    return fail(cfg.error());
  config_ = std::move(*cfg);
  return ok();
}

auto Application::start() -> void {
  if (running_.exchange(true))
    return;

  runtime_.start();

  // Open database
  if (persistence_ && !persistence_->is_open()) {
    if (auto r = persistence_->open(); !r) {
      log::error("Failed to open database: {}", r.error().message());
      running_.store(false);
      return;
    }
  }

  // Load DAGs from database
  if (auto r = dag_manager_.load_from_database(); !r) {
    log::warn("Failed to load DAGs: {}", r.error().message());
  }

  // Register database DAGs with scheduler
  for (const auto& d : dag_manager_.list_dags())  {
    if (d.from_config)
      continue;
    for (const auto& t : d.tasks) {
      scheduler_->register_task(d.id, t);
    }
    // Register cron schedule if DAG has one and is active
    if (!d.cron.empty() && d.is_active) {
      scheduler_->register_dag_cron(d.id, d.cron);
    }
  }

  // Initialize from config
  if (auto r = init_from_config(); !r) {
    log::error("Failed to init from config: {}", r.error().message());
    running_.store(false);
    return;
  }

  // Create executor
  executor_ = create_shell_executor(runtime_);

  // Create execution service
  execution_ = std::make_unique<ExecutionService>(runtime_, *executor_);
  setup_callbacks();

  // Setup scheduler callback
  scheduler_->set_on_ready(
      [this](const TaskInstance& inst) { on_engine_ready(inst); });

  // Start services
  scheduler_->start();

  // Start API server
  if (config_.api.enabled) {
    api_ =
        std::make_unique<ApiServer>(*this, config_.api.port, config_.api.host);
    events_->set_api_server(api_.get());
    api_->start();
  }

  log::info("TaskMaster started with {} tasks", config_.tasks.size());
}

auto Application::stop() -> void {
  if (!running_.exchange(false))
    return;

  log::info("Stopping TaskMaster...");

  if (api_)
    api_->stop();

  scheduler_->stop();

  // Wait for coroutines
  auto start = std::chrono::steady_clock::now();
  while ((execution_ && execution_->coro_count() > 0) ||
         (execution_ && execution_->has_active_runs())) {
    if (std::chrono::steady_clock::now() - start > std::chrono::seconds(3)) {
      log::warn("Shutdown timeout");
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  runtime_.stop();
  api_.reset();
  execution_.reset();
  executor_.reset();

  if (persistence_)
    persistence_->close();

  log::info("TaskMaster stopped");
}

auto Application::is_running() const noexcept -> bool {
  return running_.load();
}

auto Application::setup_callbacks() -> void {
  ExecutionCallbacks callbacks;

  callbacks.on_task_status = [this](std::string_view run_id,
                                    std::string_view task,
                                    std::string_view status) {
    events_->emit_task_status(run_id, task, status);
  };

  callbacks.on_run_status = [this](std::string_view run_id,
                                   std::string_view status) {
    events_->emit_run_status(run_id, status);
  };

  callbacks.on_log = [this](std::string_view run_id, std::string_view task,
                            std::string_view stream, std::string msg) {
    events_->emit_log(run_id, task, stream, std::move(msg));
  };

  callbacks.on_persist_run = [this](const DAGRun& run) {
    if (persistence_)
      persistence_->save_run(run);
  };

  callbacks.on_persist_task = [this](std::string_view run_id,
                                     const TaskInstanceInfo& info) {
    if (persistence_)
      persistence_->save_task(run_id, info);
  };

  callbacks.on_persist_log =
      [this](std::string_view run_id, std::string_view task, int attempt,
             std::string_view level, std::string_view msg) {
        if (persistence_)
          persistence_->save_log(run_id, task, attempt, level, msg);
      };

  callbacks.get_max_retries = [this](std::string_view run_id, NodeIndex idx) {
    return get_max_retries(run_id, idx);
  };

  execution_->set_callbacks(std::move(callbacks));
}

auto Application::init_from_config() -> Result<void> {
  config_dag_.clear();
  config_tasks_.clear();
  config_cfgs_.clear();

  size_t n = config_.tasks.size();
  config_tasks_.resize(n);
  config_cfgs_.resize(n);

  for (const auto& tc : config_.tasks) {
    NodeIndex idx = config_dag_.add_node(tc.id);
    config_tasks_[idx] = tc;

    ShellExecutorConfig exec;
    exec.command = tc.command;
    exec.working_dir = tc.working_dir;
    exec.timeout = tc.timeout;
    config_cfgs_[idx] = exec;
  }

  for (const auto& tc : config_.tasks) {
    for (const auto& dep : tc.deps) {
      if (auto r = config_dag_.add_edge(dep, tc.id); !r) {
        log::error("Failed to add edge {} -> {}", dep, tc.id);
        return r;
      }
    }
  }

  if (auto r = config_dag_.is_valid(); !r) {
    log::error("DAG contains a cycle!");
    return r;
  }

  // Load config into DAG manager
  if (!config_.tasks.empty()) {
    if (auto r = dag_manager_.load_from_config(config_); !r) {
      log::warn("Failed to load config: {}", r.error().message());
    }
  }

  // Initialize scheduler with root tasks
  return scheduler_->init_from_config(config_tasks_, config_cfgs_);
}

auto Application::trigger_dag(std::string_view dag_id) -> void {
  auto run_id = std::format("{}_{}", dag_id, generate_uuid());
  auto run = std::make_unique<DAGRun>(run_id, config_dag_);
  auto now = std::chrono::system_clock::now();
  run->set_scheduled_at(now);
  run->set_started_at(now);

  if (persistence_)
    persistence_->save_run(*run);

  execution_->start_run(run_id, std::move(run), config_cfgs_);
  log::info("DAG run {} triggered", run_id);
}

auto Application::trigger_dag_by_id(std::string_view dag_id,
                                    TriggerType trigger) -> void {
  auto info = dag_manager_.get_dag(dag_id);
  if (!info) {
    log::error("DAG {} not found", dag_id);
    return;
  }
  if (info->tasks.empty()) {
    log::warn("DAG {} has no tasks", dag_id);
    return;
  }

  auto graph = dag_manager_.build_dag_graph(dag_id);
  if (!graph) {
    log::error("Failed to build DAG graph for {}", dag_id);
    return;
  }

  auto run_id = std::format("{}_{}", dag_id, generate_uuid());

  std::vector<ExecutorConfig> cfgs(info->tasks.size());
  for (const auto& t : info->tasks) {
    NodeIndex idx = graph->get_index(t.id);
    if (idx != INVALID_NODE && idx < cfgs.size()) {
      ShellExecutorConfig exec;
      exec.command = t.command;
      exec.working_dir = t.working_dir;
      exec.timeout = t.timeout;
      cfgs[idx] = exec;
    }
  }

  auto run = std::make_unique<DAGRun>(run_id, *graph);
  auto now = std::chrono::system_clock::now();
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_trigger_type(trigger);

  // Initialize instance_id for all tasks
  for (const auto& t : info->tasks) {
    NodeIndex idx = graph->get_index(t.id);
    if (idx != INVALID_NODE && idx < graph->size()) {
      run->set_instance_id(idx, std::format("{}_{}", run_id, t.id));
    }
  }

  if (persistence_) {
    persistence_->save_run(*run);
    for (const auto& ti : run->all_task_info()) {
      persistence_->save_task(run_id, ti);
    }
  }

  execution_->start_run(run_id, std::move(run), std::move(cfgs));
  log::info("DAG run {} triggered for {} ({})", run_id, dag_id,
            trigger == TriggerType::Schedule ? "schedule" : "manual");
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  if (execution_)
    execution_->wait_for_completion(timeout_ms);
}

auto Application::has_active_runs() const -> bool {
  return execution_ && execution_->has_active_runs();
}

auto Application::on_engine_ready(const TaskInstance& inst) -> void {
  // Check if this is a cron-triggered DAG
  if (inst.task_id.starts_with("cron:")) {
    std::string_view dag_id = inst.task_id;
    dag_id.remove_prefix(5);  // Remove "cron:" prefix
    log::info("Cron triggered DAG: {}", dag_id);

    // Mark the cron task as completed immediately
    scheduler_->engine().task_started(inst.instance_id);
    scheduler_->engine().task_completed(inst.instance_id, 0);

    // Trigger the DAG with schedule trigger type
    trigger_dag_by_id(dag_id, TriggerType::Schedule);
    return;
  }

  auto pos = inst.task_id.find(':');
  if (pos != std::string::npos) {
    std::string_view dag_id = inst.task_id;
    dag_id = dag_id.substr(0, pos);
    std::string_view task_id = inst.task_id;
    task_id = task_id.substr(pos + 1);

    scheduler_->engine().task_started(inst.instance_id);

    auto task = dag_manager_.get_task(dag_id, task_id);
    if (!task) {
      log::error("Task {} not found in DAG {}", task_id, dag_id);
      scheduler_->engine().task_failed(inst.instance_id, "Task not found");
      return;
    }

    ShellExecutorConfig exec;
    exec.command = task->command;
    exec.working_dir = task->working_dir;
    exec.timeout = task->timeout;

    run_cron_task(std::string(dag_id), std::string(task_id), inst.instance_id,
                  exec);
    return;
  }

  scheduler_->engine().task_started(inst.instance_id);

  auto run_id = std::format("{}_{}", inst.task_id, generate_uuid());
  auto run = std::make_unique<DAGRun>(run_id, config_dag_);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (persistence_)
    persistence_->save_run(*run);

  execution_->start_run(run_id, std::move(run), config_cfgs_);
  log::info("DAG run {} triggered", run_id);
}

auto Application::run_cron_task(std::string dag_id, std::string task_id,
                                std::string inst_id, ExecutorConfig cfg)
    -> void {
  auto coro = [](Application* self, std::string dag_id, std::string task_id,
                 std::string inst_id, ExecutorConfig cfg) -> spawn_task {
    self->events_->emit_log(dag_id, task_id, "stdout", "[INFO] Cron started");

    auto result = co_await execute_async(*self->executor_, inst_id, cfg);

    if (!result.stdout_output.empty())
      self->events_->emit_log(dag_id, task_id, "stdout", result.stdout_output);
    if (!result.stderr_output.empty())
      self->events_->emit_log(dag_id, task_id, "stderr", result.stderr_output);

    if (result.exit_code == 0 && result.error.empty()) {
      log::info("Cron {}:{} completed", dag_id, task_id);
      self->scheduler_->engine().task_completed(inst_id, 0);
      self->events_->emit_log(dag_id, task_id, "stdout",
                              "[SUCCESS] Cron completed");
    } else {
      log::error("Cron {}:{} failed: {}", dag_id, task_id, result.error);
      self->scheduler_->engine().task_failed(inst_id, result.error);
      self->events_->emit_log(dag_id, task_id, "stderr",
                              "[ERROR] " + result.error);
    }
  }(this, std::move(dag_id), std::move(task_id), std::move(inst_id),
                                                          std::move(cfg));

  runtime_.schedule_external(coro.take());
}

auto Application::get_max_retries(std::string_view run_id, NodeIndex idx)
    -> int {
  std::string dag_id{run_id.substr(0, run_id.rfind('_'))};

  if (auto dag_info = dag_manager_.get_dag(dag_id)) {
    if (idx < dag_info->tasks.size()) {
      return dag_info->tasks[idx].max_retries;
    }
  }

  if (idx < config_tasks_.size()) {
    return config_tasks_[idx].max_retries;
  }

  return 3;
}

auto Application::register_task_with_engine(std::string_view dag_id,
                                            const TaskConfig& task) -> void {
  scheduler_->register_task(dag_id, task);
}

auto Application::unregister_task_from_engine(std::string_view dag_id,
                                              std::string_view task_id)
    -> void {
  scheduler_->unregister_task(dag_id, task_id);
}

auto Application::set_task_enabled(std::string_view dag_id,
                                   std::string_view task_id, bool enabled)
    -> Result<void> {
  auto dag = dag_manager_.get_dag(dag_id);
  if (!dag)
    return fail(Error::NotFound);

  auto task = dag->find_task(task_id);
  if (!task)
    return fail(Error::NotFound);

  TaskConfig updated = *task;
  updated.enabled = enabled;
  if (auto r = dag_manager_.update_task(dag_id, task_id, updated); !r) {
    return r;
  }

  if (!scheduler_->enable_task(dag_id, task_id, enabled)) {
    log::warn("Failed to {} task {}:{}", enabled ? "enable" : "disable", dag_id,
              task_id);
  }

  return ok();
}

auto Application::trigger_task(std::string_view dag_id,
                               std::string_view task_id) -> Result<void> {
  auto task = dag_manager_.get_task(dag_id, task_id);
  if (!task)
    return fail(Error::NotFound);

  auto inst_id = std::format("{}:{}_{}", dag_id, task_id, generate_uuid());

  ShellExecutorConfig exec;
  exec.command = task->command;
  exec.working_dir = task->working_dir;
  exec.timeout = task->timeout;

  run_cron_task(std::string(dag_id), std::string(task_id), inst_id, exec);

  log::info("Task {}:{} triggered", dag_id, task_id);
  return ok();
}

auto Application::register_dag_cron(std::string_view dag_id,
                                    std::string_view cron_expr) -> bool {
  return scheduler_->register_dag_cron(dag_id, cron_expr);
}

auto Application::unregister_dag_cron(std::string_view dag_id) -> void {
  scheduler_->unregister_dag_cron(dag_id);
}

auto Application::update_dag_cron(std::string_view dag_id,
                                  std::string_view cron_expr, bool is_active)
    -> void {
  // First unregister any existing cron
  scheduler_->unregister_dag_cron(dag_id);

  // Register new cron if provided and DAG is active
  if (!cron_expr.empty() && is_active) {
    scheduler_->register_dag_cron(dag_id, cron_expr);
  }
}

auto Application::recover_from_crash() -> Result<void> {
  if (!persistence_ || !persistence_->is_open()) {
    return fail(Error::DatabaseError);
  }

  Recovery recovery(*persistence_->persistence());
  auto result = recovery.recover(
      [this](const std::string&) -> const DAG& { return config_dag_; },
      [this](const std::string& run_id, DAGRun& run) {
        execution_->start_run(run_id, std::make_unique<DAGRun>(std::move(run)),
                              config_cfgs_);
      });

  if (!result)
    return fail(result.error());

  log::info("Recovery: {} runs, {} tasks to retry",
            result->recovered_dag_runs.size(), result->tasks_to_retry);
  return ok();
}

auto Application::list_tasks() const -> void {

  std::println("Tasks:");
  for (const auto& t : config_.tasks) {
      std::print("  - {}", t.id);
    if (!t.name.empty() && t.name != t.id)
      std::print(" ({})", t.name);
    std::print("{}", executor_type_to_string(t.executor));
    if (!t.deps.empty()) {
      std::print("deps:[");
      for (size_t i = 0; i < t.deps.size(); ++i) {
        if (i > 0)
          std::print(",");
        std::print("{}", t.deps[i]);
      }
      std::print("]");
    }
    std::println("");
    std::println("    command: {}", t.command);
  }
}

auto Application::show_status() const -> void {
  std::println("Status: {}", running_.load() ? "running" : "stopped");
  std::println("Tasks: {}", config_.tasks.size());
  std::println("Active runs: {}", execution_ ? "checking..." : "N/A");
}

auto Application::config() const noexcept -> const Config& {
  return config_;
}

auto Application::config() noexcept -> Config& {
  return config_;
}

auto Application::dag_manager() -> DAGManager& {
  return dag_manager_;
}

auto Application::dag_manager() const -> const DAGManager& {
  return dag_manager_;
}

auto Application::engine() -> Engine& {
  return scheduler_->engine();
}

auto Application::persistence() -> Persistence* {
  return persistence_ ? persistence_->persistence() : nullptr;
}

auto Application::api_server() -> ApiServer* {
  return api_.get();
}

auto Application::get_active_dag_run(std::string_view run_id) -> DAGRun* {
  return execution_ ? execution_->get_run(run_id) : nullptr;
}

}  // namespace taskmaster
