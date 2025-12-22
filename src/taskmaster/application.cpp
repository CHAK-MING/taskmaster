#include "taskmaster/application.hpp"
#include "taskmaster/api/api_server.hpp"
#include "taskmaster/api/websocket_hub.hpp"
#include "taskmaster/dag_manager.hpp"
#include "taskmaster/runtime.hpp"

#include <algorithm>
#include <iostream>
#include <ranges>

#include "taskmaster/log.hpp"

#include "taskmaster/cron.hpp"
#include "taskmaster/recovery.hpp"
#include "taskmaster/util.hpp"

namespace taskmaster {

Application::Application() : dag_manager_(nullptr), persistence_(nullptr) {}

Application::Application(std::string_view db_path)
    : dag_manager_(nullptr),
      persistence_(std::make_unique<Persistence>(db_path)) {
  dag_manager_.set_persistence(persistence_.get());
}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  auto cfg = ConfigLoader::load_from_file(path);
  if (!cfg) {
    return fail(cfg.error());
  }
  config_ = std::move(*cfg);
  return ok();
}

auto Application::load_config_string(std::string_view json_str)
    -> Result<void> {
  auto cfg = ConfigLoader::load_from_string(json_str);
  if (!cfg) {
    return fail(cfg.error());
  }
  config_ = std::move(*cfg);
  return ok();
}

auto Application::setup_dag() -> Result<void> {
  dag_.clear();
  task_configs_.clear();
  executor_configs_.clear();

  size_t n = config_.tasks.size();
  task_configs_.resize(n);
  executor_configs_.resize(n);

  for (const auto &task_cfg : config_.tasks) {
    NodeIndex idx = dag_.add_node(task_cfg.id);
    task_configs_[idx] = task_cfg;

    ShellExecutorConfig exec;
    exec.command = task_cfg.command;
    exec.working_dir = task_cfg.working_dir;
    exec.timeout = task_cfg.timeout;
    executor_configs_[idx] = exec;
  }

  for (const auto &task_cfg : config_.tasks) {
    for (const auto &dep : task_cfg.deps) {
      if (auto r = dag_.add_edge(dep, task_cfg.id); !r) {
        log::error("Failed to add edge {} -> {}", dep, task_cfg.id);
        return r;
      }
    }
  }

  if (auto r = dag_.validate(); !r) {
    log::error("DAG contains a cycle!");
    return r;
  }

  // Load config tasks into DAGManager
  if (!config_.tasks.empty()) {
    if (auto r = dag_manager_.load_from_config(config_); !r) {
      log::warn("Failed to load config into DAGManager: {}",
                   r.error().message());
    }
  }

  return ok();
}

auto Application::setup_engine() -> Result<void> {
  for (auto [idx, task_cfg] : std::views::enumerate(task_configs_)) {
    if (task_cfg.cron.empty())
      continue;
    if (!task_cfg.deps.empty())
      continue;

    auto cron = CronExpr::parse(task_cfg.cron);
    if (!cron) {
      log::error("Invalid cron for task {}: {}", task_cfg.id, task_cfg.cron);
      return fail(cron.error());
    }

    TaskDefinition def;
    def.id = task_cfg.id;
    def.name = task_cfg.name;
    def.schedule = *cron;
    def.executor = executor_configs_[idx];
    def.retry.max_attempts = task_cfg.max_retries;
    def.enabled = task_cfg.enabled;

    engine_.add_task(std::move(def));
  }

  engine_.set_on_ready_callback(
      [this](const TaskInstance &inst) { on_task_ready(inst); });

  return ok();
}

auto Application::start() -> void {
  if (running_.exchange(true))
    return;

  if (persistence_ && !persistence_->is_open()) {
    if (auto r = persistence_->open(); !r) {
      log::error("Failed to open database: {}", r.error().message());
      running_.store(false);
      return;
    }
  }

  // Load DAGs from database
  if (auto r = dag_manager_.load_from_database(); !r) {
    log::warn("Failed to load DAGs from database: {}", r.error().message());
  }

  // Register all tasks with cron schedules from loaded DAGs
  for (const auto &dag : dag_manager_.list_dags()) {
    if (dag.from_config)
      continue; // Config tasks are handled separately
    for (const auto &task : dag.tasks) {
      register_task_with_engine(dag.id, task);
    }
  }

  if (auto r = setup_dag(); !r) {
    log::error("Failed to setup DAG: {}", r.error().message());
    running_.store(false);
    return;
  }

  if (auto r = setup_engine(); !r) {
    log::error("Failed to setup engine: {}", r.error().message());
    running_.store(false);
    return;
  }

  executor_ = create_shell_executor();
  engine_.start();

  if (config_.api.enabled) {
    api_server_ =
        std::make_unique<ApiServer>(*this, config_.api.port, config_.api.host);
    api_server_->start();
  }

  log::info("TaskMaster started with {} tasks", config_.tasks.size());
}

auto Application::stop() -> void {
  if (!running_.exchange(false))
    return;

  log::info("Stopping TaskMaster...");

  // Stop accepting new tasks from engine
  engine_.stop();

  // Wait for active coroutines to complete (with timeout)
  auto start = std::chrono::steady_clock::now();
  constexpr auto shutdown_timeout = std::chrono::seconds(5);

  while (active_coroutines_.load() > 0 || has_active_runs()) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > shutdown_timeout) {
      log::warn("Shutdown timeout - {} active coroutines, {} active runs "
                   "still pending",
                   active_coroutines_.load(), [this]() {
                     std::lock_guard lock(dag_runs_mu_);
                     return std::ranges::count_if(
                         dag_runs_ | std::views::values,
                         [](const auto &run) { return !run->is_complete(); });
                   }());
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // Stop Runtime to cancel any pending IO operations
  Runtime::instance().stop();

  if (api_server_) {
    api_server_->stop();
    api_server_.reset();
  }

  executor_.reset();

  if (persistence_) {
    persistence_->close();
  }

  log::info("TaskMaster stopped");
}

auto Application::trigger_dag(std::string_view dag_id) -> void {
  std::string dag_run_id = std::string(dag_id) + "_" + generate_uuid();

  auto run = std::make_unique<DAGRun>(dag_run_id, dag_);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (persistence_) {
    if (auto r = persistence_->save_dag_run(*run); !r) {
      log::warn("Failed to persist DAG run {}: {}", dag_run_id,
                   r.error().message());
    }
  }

  restore_dag_run(dag_run_id, std::move(run));
  log::info("DAG run {} triggered", dag_run_id);
}

auto Application::restore_dag_run(std::string dag_run_id,
                                  std::unique_ptr<DAGRun> run) -> void {
  {
    std::lock_guard lock(dag_runs_mu_);
    dag_runs_[dag_run_id] = std::move(run);
  }
  execute_ready_tasks(dag_run_id);
}

auto Application::trigger_dag_by_id(std::string_view dag_id) -> void {
  // Get DAG from DAGManager
  auto dag_info = dag_manager_.get_dag(dag_id);
  if (!dag_info) {
    log::error("DAG {} not found", dag_id);
    return;
  }

  if (dag_info->tasks.empty()) {
    log::warn("DAG {} has no tasks", dag_id);
    return;
  }

  // Build DAG graph
  auto dag_graph = dag_manager_.build_dag_graph(dag_id);
  if (!dag_graph) {
    log::error("Failed to build DAG graph for {}", dag_id);
    return;
  }

  std::string dag_run_id = std::string(dag_id) + "_" + generate_uuid();

  // Store task configs and executor configs for this DAG run
  std::vector<TaskConfig> run_task_configs;
  std::vector<ExecutorConfig> run_exec_configs;
  run_task_configs.resize(dag_info->tasks.size());
  run_exec_configs.resize(dag_info->tasks.size());

  for (const auto &task : dag_info->tasks) {
    NodeIndex idx = dag_graph->get_index(task.id);
    if (idx != INVALID_NODE && idx < run_task_configs.size()) {
      run_task_configs[idx] = task;

      ShellExecutorConfig exec;
      exec.command = task.command;
      exec.working_dir = task.working_dir;
      exec.timeout = task.timeout;
      run_exec_configs[idx] = exec;
    }
  }

  auto run = std::make_unique<DAGRun>(dag_run_id, *dag_graph);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (persistence_) {
    if (auto r = persistence_->save_dag_run(*run); !r) {
      log::warn("Failed to persist DAG run {}: {}", dag_run_id,
                   r.error().message());
    }
  }

  {
    std::lock_guard lock(dag_runs_mu_);
    dag_runs_[dag_run_id] = std::move(run);
  }

  log::info("DAG run {} triggered for DAG {}", dag_run_id, dag_id);

  // Execute ready tasks directly with the DAG's task configs
  struct TaskExecInfo {
    NodeIndex task_idx;
    std::string task_name;
    std::string instance_id;
    ExecutorConfig config;
  };
  std::vector<TaskExecInfo> tasks_to_execute;

  {
    std::scoped_lock lock(dag_runs_mu_, instance_map_mu_);
    auto it = dag_runs_.find(dag_run_id);
    if (it == dag_runs_.end()) {
      return;
    }

    auto ready_tasks = it->second->get_ready_tasks();
    tasks_to_execute.reserve(ready_tasks.size());

    for (NodeIndex task_idx : ready_tasks) {
      if (task_idx >= run_exec_configs.size()) {
        log::error("Executor config not found for task index {}", task_idx);
        continue;
      }

      const std::string &task_name = dag_graph->get_key(task_idx);
      std::string instance_id = dag_run_id + "_" + task_name;

      it->second->mark_task_started(task_idx, instance_id);

      instance_to_dag_run_[instance_id] = dag_run_id;
      instance_to_task_[instance_id] = task_idx;

      tasks_to_execute.push_back({
          .task_idx = task_idx,
          .task_name = task_name,
          .instance_id = std::move(instance_id),
          .config = run_exec_configs[task_idx],
      });
    }
  }

  for (auto &task : tasks_to_execute) {
    log::info("Executing task {} (instance {})", task.task_name,
                 task.instance_id);

    execute_task_coro(dag_run_id, task.task_idx, std::move(task.task_name),
                      std::move(task.instance_id), std::move(task.config));
  }
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  std::unique_lock lock(completion_mu_);
  auto timeout = std::chrono::milliseconds(timeout_ms);

  bool completed = completion_cv_.wait_for(lock, timeout, [this] {
    return !has_active_runs();
  });

  if (!completed) {
    log::warn("wait_for_completion timed out after {} ms", timeout_ms);
  }
}

auto Application::has_active_runs() const -> bool {
  std::lock_guard lock(dag_runs_mu_);
  return std::ranges::any_of(
      dag_runs_ | std::views::values,
      [](const auto &run) { return !run->is_complete(); });
}

auto Application::execute_ready_tasks(const std::string &dag_run_id) -> void {
  struct TaskExecInfo {
    NodeIndex task_idx;
    std::string task_name;
    std::string instance_id;
    ExecutorConfig config;
  };
  std::vector<TaskExecInfo> tasks_to_execute;

  {
    std::scoped_lock lock(dag_runs_mu_, instance_map_mu_);
    auto it = dag_runs_.find(dag_run_id);
    if (it == dag_runs_.end()) {
      return;
    }

    auto ready_tasks = it->second->get_ready_tasks();
    tasks_to_execute.reserve(ready_tasks.size());

    for (NodeIndex task_idx : ready_tasks) {
      if (task_idx >= executor_configs_.size()) {
        log::error("Executor config not found for task index {}", task_idx);
        continue;
      }

      const std::string &task_name = dag_.get_key(task_idx);
      std::string instance_id = dag_run_id + "_" + task_name;

      it->second->mark_task_started(task_idx, instance_id);

      if (persistence_) {
        if (auto info = it->second->get_task_info(task_idx)) {
          if (auto r = persistence_->save_task_instance(dag_run_id, *info);
              !r) {
            log::warn("Failed to persist task instance: {}",
                         r.error().message());
          }
        }
      }

      instance_to_dag_run_[instance_id] = dag_run_id;
      instance_to_task_[instance_id] = task_idx;

      tasks_to_execute.push_back({
          .task_idx = task_idx,
          .task_name = task_name,
          .instance_id = std::move(instance_id),
          .config = executor_configs_[task_idx],
      });
    }
  }

  for (auto &task : tasks_to_execute) {
    log::info("Executing task {} (instance {})", task.task_name,
                 task.instance_id);

    execute_task_coro(dag_run_id, task.task_idx, std::move(task.task_name),
                      std::move(task.instance_id), std::move(task.config));
  }
}

auto Application::execute_task_coro(std::string dag_run_id, NodeIndex task_idx,
                                    std::string task_name,
                                    std::string instance_id,
                                    ExecutorConfig config) -> spawn_task {
  ++active_coroutines_;
  struct CoroutineGuard {
    std::atomic<int> &counter;
    ~CoroutineGuard() { --counter; }
  } guard{active_coroutines_};

  auto broadcast = [this, &dag_run_id, &task_name](std::string_view stream,
                                                   std::string content) {
    if (api_server_) {
      api_server_->hub().broadcast_log({format_timestamp(), dag_run_id,
                                        task_name, std::string(stream),
                                        std::move(content)});
    }
  };

  broadcast("stdout", "[INFO] Task started: " + task_name);

  auto result = co_await execute_async(*executor_, instance_id, config);

  if (!result.stdout_output.empty())
    broadcast("stdout", result.stdout_output);
  if (!result.stderr_output.empty())
    broadcast("stderr", result.stderr_output);

  if (result.exit_code == 0 && result.error.empty()) {
    log::info("Task {} completed", task_name);
    broadcast("stdout", "[SUCCESS] Task completed: " + task_name);
    on_dag_run_task_completed(dag_run_id, task_idx);
  } else {
    log::error("Task {} failed: {}", task_name, result.error);
    broadcast("stderr",
              "[ERROR] Task failed: " + task_name + " - " + result.error);

    std::lock_guard lock(dag_runs_mu_);
    if (auto it = dag_runs_.find(dag_run_id); it != dag_runs_.end()) {
      int max_retries = task_idx < task_configs_.size()
                            ? task_configs_[task_idx].max_retries
                            : 3;
      it->second->mark_task_failed(task_idx, result.error, max_retries);

      if (persistence_) {
        if (auto info = it->second->get_task_info(task_idx)) {
          if (auto r = persistence_->update_task_instance(dag_run_id, *info);
              !r) {
            log::warn("Failed to update task instance: {}",
                         r.error().message());
          }
        }
      }

      if (it->second->is_complete()) {
        completion_cv_.notify_all();
      }
    }
  }
}

auto Application::on_task_ready(const TaskInstance &inst) -> void {
  auto pos = inst.task_id.find(':');
  if (pos != std::string::npos) {
    std::string dag_id = inst.task_id.substr(0, pos);
    std::string task_id = inst.task_id.substr(pos + 1);

    engine_.task_started(inst.instance_id);

    auto task_result = dag_manager_.get_task(dag_id, task_id);
    if (!task_result) {
      log::error("Task {} not found in DAG {}", task_id, dag_id);
      engine_.task_failed(inst.instance_id, "Task not found");
      return;
    }

    const auto &task = *task_result;
    ShellExecutorConfig exec;
    exec.command = task.command;
    exec.working_dir = task.working_dir;
    exec.timeout = task.timeout;

    execute_single_task_coro(dag_id, task_id, inst.instance_id, exec);
    return;
  }

  NodeIndex task_idx = dag_.get_index(inst.task_id);
  {
    std::lock_guard lock(instance_map_mu_);
    instance_to_task_[inst.instance_id] = task_idx;
  }

  engine_.task_started(inst.instance_id);
  trigger_dag(inst.task_id);
}

auto Application::register_task_with_engine(std::string_view dag_id,
                                            const TaskConfig &task) -> void {
  if (task.cron.empty() || !task.deps.empty()) {
    return;
  }

  auto cron = CronExpr::parse(task.cron);
  if (!cron) {
    log::error("Invalid cron expression for task {}: {}", task.id,
                  task.cron);
    return;
  }

  std::string engine_task_id = std::string(dag_id) + ":" + task.id;

  TaskDefinition def;
  def.id = engine_task_id;
  def.name = task.name.empty() ? task.id : task.name;
  def.schedule = *cron;

  ShellExecutorConfig exec;
  exec.command = task.command;
  exec.working_dir = task.working_dir;
  exec.timeout = task.timeout;
  def.executor = exec;

  def.retry.max_attempts = task.max_retries;
  def.enabled = task.enabled;

  engine_.add_task(std::move(def));
  log::info("Registered cron task: {} [{}]", engine_task_id, task.cron);
}

auto Application::unregister_task_from_engine(std::string_view dag_id,
                                              std::string_view task_id)
    -> void {
  std::string engine_task_id = std::string(dag_id) + ":" + std::string(task_id);
  engine_.remove_task(engine_task_id);
  log::info("Unregistered task {} from engine", engine_task_id);
}

auto Application::execute_single_task_coro(std::string dag_id,
                                           std::string task_id,
                                           std::string instance_id,
                                           ExecutorConfig config)
    -> spawn_task {
  ++active_coroutines_;
  struct CoroutineGuard {
    std::atomic<int> &counter;
    ~CoroutineGuard() { --counter; }
  } guard{active_coroutines_};

  auto broadcast = [this, &dag_id, &task_id](std::string_view stream,
                                             std::string content) {
    if (api_server_) {
      api_server_->hub().broadcast_log({format_timestamp(), dag_id, task_id,
                                        std::string(stream),
                                        std::move(content)});
    }
  };

  broadcast("stdout", "[INFO] Task started: " + task_id);

  auto result = co_await execute_async(*executor_, instance_id, config);

  if (!result.stdout_output.empty())
    broadcast("stdout", result.stdout_output);
  if (!result.stderr_output.empty())
    broadcast("stderr", result.stderr_output);

  if (result.exit_code == 0 && result.error.empty()) {
    log::info("Task {}:{} completed", dag_id, task_id);
    engine_.task_completed(instance_id, 0);
    broadcast("stdout", "[SUCCESS] Task completed: " + task_id);
  } else {
    log::error("Task {}:{} failed: {}", dag_id, task_id, result.error);
    engine_.task_failed(instance_id, result.error);
    broadcast("stderr",
              "[ERROR] Task failed: " + task_id + " - " + result.error);
  }
}

auto Application::on_dag_run_task_completed(std::string_view dag_run_id,
                                            NodeIndex task_idx) -> void {
  std::string dag_run_id_str(dag_run_id);
  bool run_completed = false;
  {
    std::lock_guard lock(dag_runs_mu_);
    auto it = dag_runs_.find(dag_run_id_str);
    if (it != dag_runs_.end()) {
      it->second->mark_task_completed(task_idx, 0);

      if (persistence_) {
        auto info = it->second->get_task_info(task_idx);
        if (info) {
          if (auto r = persistence_->update_task_instance(dag_run_id, *info);
              !r) {
            log::warn("Failed to update task instance: {}",
                         r.error().message());
          }
        }

        if (it->second->is_complete()) {
          if (auto r = persistence_->update_dag_run_state(dag_run_id,
                                                          it->second->state());
              !r) {
            log::warn("Failed to update DAG run state: {}",
                         r.error().message());
          }
        }
      }

      if (it->second->is_complete()) {
        log::info("DAG run {} completed", dag_run_id);
        run_completed = true;
      }
    }
  }

  if (run_completed) {
    completion_cv_.notify_all();
    return;
  }

  execute_ready_tasks(dag_run_id_str);
}

auto Application::list_tasks() const -> void {
  std::cout << "Tasks:\n";

  for (const auto &task : config_.tasks) {
    std::cout << "  - " << task.id;
    if (!task.name.empty() && task.name != task.id)
      std::cout << " (" << task.name << ")";
    if (!task.cron.empty())
      std::cout << " [" << task.cron << "]";
    if (!task.deps.empty()) {
      std::cout << " deps:[";
      bool first = true;
      for (const auto &dep : task.deps) {
        if (!first)
          std::cout << ",";
        std::cout << dep;
        first = false;
      }
      std::cout << "]";
    }
    std::cout << "\n    command: " << task.command << "\n";
  }
}

auto Application::show_status() const -> void {
  std::cout << "Status: " << (running_.load() ? "running" : "stopped") << "\n";
  std::cout << "Tasks loaded: " << config_.tasks.size() << "\n";
  std::cout << "Active DAG runs: " << dag_runs_.size() << "\n";
}

auto Application::recover_from_crash() -> Result<void> {
  if (!persistence_ || !persistence_->is_open()) {
    return fail(Error::DatabaseError);
  }

  Recovery recovery(*persistence_);
  auto result = recovery.recover(
      [this](const std::string &) -> const DAG & { return dag_; },
      [this](const std::string &dag_run_id, DAGRun &run) {
        restore_dag_run(dag_run_id, std::make_unique<DAGRun>(std::move(run)));
      });

  if (!result) {
    return fail(result.error());
  }

  log::info("Recovery: {} runs recovered, {} tasks to retry",
               result->recovered_dag_runs.size(), result->tasks_to_retry);

  return ok();
}

} // namespace taskmaster
