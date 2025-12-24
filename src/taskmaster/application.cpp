#include "taskmaster/application.hpp"

#include <algorithm>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <ranges>
#include <unordered_map>
#include <vector>

#include "taskmaster/api/api_server.hpp"
#include "taskmaster/config.hpp"
#include "taskmaster/coroutine.hpp"
#include "taskmaster/cron.hpp"
#include "taskmaster/dag.hpp"
#include "taskmaster/dag_manager.hpp"
#include "taskmaster/dag_run.hpp"
#include "taskmaster/engine.hpp"
#include "taskmaster/executor.hpp"
#include "taskmaster/log.hpp"
#include "taskmaster/persistence.hpp"
#include "taskmaster/recovery.hpp"
#include "taskmaster/runtime.hpp"
#include "taskmaster/util.hpp"

namespace taskmaster {

struct TaskExecInfo {
  NodeIndex task_idx;
  std::string task_name;
  std::string instance_id;
  ExecutorConfig config;
};

struct Application::Impl {
  std::atomic<bool> running{false};
  Config config;
  Runtime runtime{0};
  Engine engine;
  std::unique_ptr<IExecutor> executor;
  DAGManager dag_manager{nullptr};
  std::unique_ptr<Persistence> persistence;
  std::unique_ptr<ApiServer> api_server;

  DAG dag;
  std::vector<TaskConfig> task_configs;
  std::vector<ExecutorConfig> executor_configs;
  std::unordered_map<std::string, std::unique_ptr<DAGRun>> dag_runs;
  std::unordered_map<std::string, std::string> instance_to_dag_run;
  std::unordered_map<std::string, NodeIndex> instance_to_task;

  mutable std::mutex dag_runs_mu;
  mutable std::mutex instance_map_mu;
  std::atomic<int> active_coroutines{0};

  mutable std::mutex completion_mu;
  std::condition_variable completion_cv;

  Impl() = default;

  explicit Impl(std::string_view db_path)
      : runtime(0), persistence(std::make_unique<Persistence>(db_path)) {
    dag_manager.set_persistence(persistence.get());
  }

  auto setup_dag() -> Result<void>;
  auto setup_engine() -> Result<void>;
  auto on_task_ready(const TaskInstance &inst) -> void;
  auto on_dag_run_task_completed(std::string_view dag_run_id,
                                 NodeIndex task_idx) -> void;
  auto execute_ready_tasks(const std::string &dag_run_id) -> void;
  auto collect_ready_tasks(const std::string &dag_run_id, const DAG &dag,
                           const std::vector<ExecutorConfig> &exec_configs)
      -> std::vector<TaskExecInfo>;
  auto execute_collected_tasks(const std::string &dag_run_id,
                               std::vector<TaskExecInfo> tasks) -> void;
  auto execute_task_coro(std::string dag_run_id, NodeIndex task_idx,
                         std::string task_name, std::string instance_id,
                         ExecutorConfig config) -> spawn_task;
  auto execute_single_task_coro(std::string dag_id, std::string task_id,
                                std::string instance_id, ExecutorConfig config)
      -> spawn_task;
  auto restore_dag_run(std::string dag_run_id, std::unique_ptr<DAGRun> run)
      -> void;
  auto register_task_with_engine(std::string_view dag_id,
                                 const TaskConfig &task) -> void;
  auto has_active_runs() const -> bool;
};

Application::Application() : impl_(std::make_unique<Impl>()) {}

Application::Application(std::string_view db_path)
    : impl_(std::make_unique<Impl>(db_path)) {}

Application::~Application() { stop(); }

auto Application::load_config(std::string_view path) -> Result<void> {
  auto cfg = ConfigLoader::load_from_file(path);
  if (!cfg) {
    return fail(cfg.error());
  }
  impl_->config = std::move(*cfg);
  return ok();
}

auto Application::load_config_string(std::string_view json_str)
    -> Result<void> {
  auto cfg = ConfigLoader::load_from_string(json_str);
  if (!cfg) {
    return fail(cfg.error());
  }
  impl_->config = std::move(*cfg);
  return ok();
}

auto Application::Impl::setup_dag() -> Result<void> {
  dag.clear();
  task_configs.clear();
  executor_configs.clear();

  size_t n = config.tasks.size();
  task_configs.resize(n);
  executor_configs.resize(n);

  for (const auto &task_cfg : config.tasks) {
    NodeIndex idx = dag.add_node(task_cfg.id);
    task_configs[idx] = task_cfg;

    ShellExecutorConfig exec;
    exec.command = task_cfg.command;
    exec.working_dir = task_cfg.working_dir;
    exec.timeout = task_cfg.timeout;
    executor_configs[idx] = exec;
  }

  for (const auto &task_cfg : config.tasks) {
    for (const auto &dep : task_cfg.deps) {
      if (auto r = dag.add_edge(dep, task_cfg.id); !r) {
        log::error("Failed to add edge {} -> {}", dep, task_cfg.id);
        return r;
      }
    }
  }

  if (auto r = dag.is_valid(); !r) {
    log::error("DAG contains a cycle!");
    return r;
  }

  if (!config.tasks.empty()) {
    if (auto r = dag_manager.load_from_config(config); !r) {
      log::warn("Failed to load config into DAGManager: {}",
                r.error().message());
    }
  }

  return ok();
}

auto Application::Impl::setup_engine() -> Result<void> {
  for (auto [idx, task_cfg] : std::views::enumerate(task_configs)) {
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
    def.executor = executor_configs[idx];
    def.retry.max_attempts = task_cfg.max_retries;
    def.enabled = task_cfg.enabled;

    engine.add_task(std::move(def));
  }

  engine.set_on_ready_callback(
      [this](const TaskInstance &inst) { on_task_ready(inst); });

  return ok();
}

auto Application::start() -> void {
  if (impl_->running.exchange(true))
    return;

  impl_->runtime.start();

  if (impl_->persistence && !impl_->persistence->is_open()) {
    if (auto r = impl_->persistence->open(); !r) {
      log::error("Failed to open database: {}", r.error().message());
      impl_->running.store(false);
      return;
    }
  }

  if (auto r = impl_->dag_manager.load_from_database(); !r) {
    log::warn("Failed to load DAGs from database: {}", r.error().message());
  }

  for (const auto &dag : impl_->dag_manager.list_dags()) {
    if (dag.from_config)
      continue;
    for (const auto &task : dag.tasks) {
      impl_->register_task_with_engine(dag.id, task);
    }
  }

  if (auto r = impl_->setup_dag(); !r) {
    log::error("Failed to setup DAG: {}", r.error().message());
    impl_->running.store(false);
    return;
  }

  if (auto r = impl_->setup_engine(); !r) {
    log::error("Failed to setup engine: {}", r.error().message());
    impl_->running.store(false);
    return;
  }

  impl_->executor = create_shell_executor(impl_->runtime);
  impl_->engine.start();

  if (impl_->config.api.enabled) {
    impl_->api_server = std::make_unique<ApiServer>(
        *this, impl_->config.api.port, impl_->config.api.host);
    impl_->api_server->start();
  }

  log::info("TaskMaster started with {} tasks", impl_->config.tasks.size());
}

auto Application::stop() -> void {
  if (!impl_->running.exchange(false))
    return;

  log::info("Stopping TaskMaster...");

  impl_->engine.stop();

  auto start = std::chrono::steady_clock::now();
  constexpr auto shutdown_timeout = std::chrono::seconds(5);

  while (impl_->active_coroutines.load() > 0 || impl_->has_active_runs()) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > shutdown_timeout) {
      log::warn("Shutdown timeout - {} active coroutines, {} active runs "
                "still pending",
                impl_->active_coroutines.load(), [this]() {
                  std::lock_guard lock(impl_->dag_runs_mu);
                  return std::ranges::count_if(
                      impl_->dag_runs | std::views::values,
                      [](const auto &run) { return !run->is_complete(); });
                }());
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  impl_->runtime.stop();

  if (impl_->api_server) {
    impl_->api_server->stop();
    impl_->api_server.reset();
  }

  impl_->executor.reset();

  if (impl_->persistence) {
    impl_->persistence->close();
  }

  log::info("TaskMaster stopped");
}

auto Application::is_running() const noexcept -> bool {
  return impl_->running.load();
}

auto Application::trigger_dag(std::string_view dag_id) -> void {
  std::string dag_run_id = std::string(dag_id) + "_" + generate_uuid();

  auto run = std::make_unique<DAGRun>(dag_run_id, impl_->dag);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (impl_->persistence) {
    if (auto r = impl_->persistence->save_dag_run(*run); !r) {
      log::warn("Failed to persist DAG run {}: {}", dag_run_id,
                r.error().message());
    }
  }

  impl_->restore_dag_run(dag_run_id, std::move(run));
  log::info("DAG run {} triggered", dag_run_id);
}

auto Application::Impl::restore_dag_run(std::string dag_run_id,
                                        std::unique_ptr<DAGRun> run) -> void {
  {
    std::lock_guard lock(dag_runs_mu);
    dag_runs[dag_run_id] = std::move(run);
  }
  execute_ready_tasks(dag_run_id);
}

auto Application::trigger_dag_by_id(std::string_view dag_id) -> void {
  auto dag_info = impl_->dag_manager.get_dag(dag_id);
  if (!dag_info) {
    log::error("DAG {} not found", dag_id);
    return;
  }

  if (dag_info->tasks.empty()) {
    log::warn("DAG {} has no tasks", dag_id);
    return;
  }

  auto dag_graph = impl_->dag_manager.build_dag_graph(dag_id);
  if (!dag_graph) {
    log::error("Failed to build DAG graph for {}", dag_id);
    return;
  }

  std::string dag_run_id = std::string(dag_id) + "_" + generate_uuid();

  std::vector<ExecutorConfig> run_exec_configs;
  run_exec_configs.resize(dag_info->tasks.size());

  for (const auto &task : dag_info->tasks) {
    NodeIndex idx = dag_graph->get_index(task.id);
    if (idx != INVALID_NODE && idx < run_exec_configs.size()) {
      ShellExecutorConfig exec;
      exec.command = task.command;
      exec.working_dir = task.working_dir;
      exec.timeout = task.timeout;
      run_exec_configs[idx] = exec;
    }
  }

  auto run = std::make_unique<DAGRun>(dag_run_id, *dag_graph);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (impl_->persistence) {
    if (auto r = impl_->persistence->save_dag_run(*run); !r) {
      log::warn("Failed to persist DAG run {}: {}", dag_run_id,
                r.error().message());
    }
  }

  {
    std::lock_guard lock(impl_->dag_runs_mu);
    impl_->dag_runs[dag_run_id] = std::move(run);
  }

  log::info("DAG run {} triggered for DAG {}", dag_run_id, dag_id);

  auto tasks_to_execute =
      impl_->collect_ready_tasks(dag_run_id, *dag_graph, run_exec_configs);
  impl_->execute_collected_tasks(dag_run_id, std::move(tasks_to_execute));
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  std::unique_lock lock(impl_->completion_mu);
  auto timeout = std::chrono::milliseconds(timeout_ms);

  bool completed = impl_->completion_cv.wait_for(
      lock, timeout, [this] { return !impl_->has_active_runs(); });

  if (!completed) {
    log::warn("wait_for_completion timed out after {} ms", timeout_ms);
  }
}

auto Application::Impl::has_active_runs() const -> bool {
  std::lock_guard lock(dag_runs_mu);
  return std::ranges::any_of(
      dag_runs | std::views::values,
      [](const auto &run) { return !run->is_complete(); });
}

auto Application::has_active_runs() const -> bool {
  return impl_->has_active_runs();
}

auto Application::Impl::collect_ready_tasks(
    const std::string &dag_run_id, const DAG &dag_ref,
    const std::vector<ExecutorConfig> &exec_configs)
    -> std::vector<TaskExecInfo> {
  std::vector<TaskExecInfo> tasks_to_execute;

  std::scoped_lock lock(dag_runs_mu, instance_map_mu);
  auto it = dag_runs.find(dag_run_id);
  if (it == dag_runs.end()) {
    return tasks_to_execute;
  }

  auto ready_tasks = it->second->get_ready_tasks();
  tasks_to_execute.reserve(ready_tasks.size());

  for (NodeIndex task_idx : ready_tasks) {
    if (task_idx >= exec_configs.size()) {
      log::error("Executor config not found for task index {}", task_idx);
      continue;
    }

    const std::string &task_name = dag_ref.get_key(task_idx);
    std::string instance_id = dag_run_id + "_" + task_name;

    it->second->mark_task_started(task_idx, instance_id);

    instance_to_dag_run[instance_id] = dag_run_id;
    instance_to_task[instance_id] = task_idx;

    tasks_to_execute.push_back({
        .task_idx = task_idx,
        .task_name = task_name,
        .instance_id = std::move(instance_id),
        .config = exec_configs[task_idx],
    });
  }

  return tasks_to_execute;
}

auto Application::Impl::execute_collected_tasks(const std::string &dag_run_id,
                                                std::vector<TaskExecInfo> tasks)
    -> void {
  for (auto &task : tasks) {
    log::info("Executing task {} (instance {})", task.task_name,
              task.instance_id);

    execute_task_coro(dag_run_id, task.task_idx, std::move(task.task_name),
                      std::move(task.instance_id), std::move(task.config));
  }
}

auto Application::Impl::execute_ready_tasks(const std::string &dag_run_id)
    -> void {
  auto tasks_to_execute =
      collect_ready_tasks(dag_run_id, dag, executor_configs);

  if (persistence && !tasks_to_execute.empty()) {
    std::lock_guard lock(dag_runs_mu);
    auto it = dag_runs.find(dag_run_id);
    if (it != dag_runs.end()) {
      for (const auto &task : tasks_to_execute) {
        if (auto info = it->second->get_task_info(task.task_idx)) {
          if (auto r = persistence->save_task_instance(dag_run_id, *info); !r) {
            log::warn("Failed to persist task instance: {}",
                      r.error().message());
          }
        }
      }
    }
  }

  execute_collected_tasks(dag_run_id, std::move(tasks_to_execute));
}

auto Application::Impl::execute_task_coro(
    std::string dag_run_id, NodeIndex task_idx, std::string task_name,
    std::string instance_id, ExecutorConfig exec_config) -> spawn_task {
  ++active_coroutines;
  struct CoroutineGuard {
    std::atomic<int> &counter;
    ~CoroutineGuard() { --counter; }
  } guard{active_coroutines};

  auto broadcast = [this, &dag_run_id, &task_name](std::string_view stream,
                                                   std::string content) {
    if (api_server) {
      api_server->hub().broadcast_log({format_timestamp(), dag_run_id,
                                       task_name, std::string(stream),
                                       std::move(content)});
    }
  };

  broadcast("stdout", "[INFO] Task started: " + task_name);

  auto result = co_await execute_async(*executor, instance_id, exec_config);

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

    std::lock_guard lock(dag_runs_mu);
    if (auto it = dag_runs.find(dag_run_id); it != dag_runs.end()) {
      int max_retries = task_idx < task_configs.size()
                            ? task_configs[task_idx].max_retries
                            : 3;
      it->second->mark_task_failed(task_idx, result.error, max_retries);

      if (persistence) {
        if (auto info = it->second->get_task_info(task_idx)) {
          if (auto r = persistence->update_task_instance(dag_run_id, *info);
              !r) {
            log::warn("Failed to update task instance: {}",
                      r.error().message());
          }
        }
      }

      if (it->second->is_complete()) {
        completion_cv.notify_all();
      }
    }
  }
}

auto Application::Impl::on_task_ready(const TaskInstance &inst) -> void {
  auto pos = inst.task_id.find(':');
  if (pos != std::string::npos) {
    std::string dag_id = inst.task_id.substr(0, pos);
    std::string task_id = inst.task_id.substr(pos + 1);

    engine.task_started(inst.instance_id);

    auto task_result = dag_manager.get_task(dag_id, task_id);
    if (!task_result) {
      log::error("Task {} not found in DAG {}", task_id, dag_id);
      engine.task_failed(inst.instance_id, "Task not found");
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

  NodeIndex task_idx = dag.get_index(inst.task_id);
  {
    std::lock_guard lock(instance_map_mu);
    instance_to_task[inst.instance_id] = task_idx;
  }

  engine.task_started(inst.instance_id);

  // Trigger DAG run - need to call through Application
  std::string dag_run_id = std::string(inst.task_id) + "_" + generate_uuid();
  auto run = std::make_unique<DAGRun>(dag_run_id, dag);
  run->set_scheduled_at(std::chrono::system_clock::now());

  if (persistence) {
    if (auto r = persistence->save_dag_run(*run); !r) {
      log::warn("Failed to persist DAG run {}: {}", dag_run_id,
                r.error().message());
    }
  }

  restore_dag_run(dag_run_id, std::move(run));
  log::info("DAG run {} triggered", dag_run_id);
}

auto Application::Impl::register_task_with_engine(std::string_view dag_id,
                                                  const TaskConfig &task)
    -> void {
  if (task.cron.empty() || !task.deps.empty()) {
    return;
  }

  auto cron = CronExpr::parse(task.cron);
  if (!cron) {
    log::error("Invalid cron expression for task {}: {}", task.id, task.cron);
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

  engine.add_task(std::move(def));
  log::info("Registered cron task: {} [{}]", engine_task_id, task.cron);
}

auto Application::register_task_with_engine(std::string_view dag_id,
                                            const TaskConfig &task) -> void {
  impl_->register_task_with_engine(dag_id, task);
}

auto Application::unregister_task_from_engine(std::string_view dag_id,
                                              std::string_view task_id)
    -> void {
  std::string engine_task_id = std::string(dag_id) + ":" + std::string(task_id);
  impl_->engine.remove_task(engine_task_id);
  log::info("Unregistered task {} from engine", engine_task_id);
}

auto Application::Impl::execute_single_task_coro(std::string dag_id,
                                                 std::string task_id,
                                                 std::string instance_id,
                                                 ExecutorConfig exec_config)
    -> spawn_task {
  ++active_coroutines;
  struct CoroutineGuard {
    std::atomic<int> &counter;
    ~CoroutineGuard() { --counter; }
  } guard{active_coroutines};

  auto broadcast = [this, &dag_id, &task_id](std::string_view stream,
                                             std::string content) {
    if (api_server) {
      api_server->hub().broadcast_log({format_timestamp(), dag_id, task_id,
                                       std::string(stream),
                                       std::move(content)});
    }
  };

  broadcast("stdout", "[INFO] Task started: " + task_id);

  auto result = co_await execute_async(*executor, instance_id, exec_config);

  if (!result.stdout_output.empty())
    broadcast("stdout", result.stdout_output);
  if (!result.stderr_output.empty())
    broadcast("stderr", result.stderr_output);

  if (result.exit_code == 0 && result.error.empty()) {
    log::info("Task {}:{} completed", dag_id, task_id);
    engine.task_completed(instance_id, 0);
    broadcast("stdout", "[SUCCESS] Task completed: " + task_id);
  } else {
    log::error("Task {}:{} failed: {}", dag_id, task_id, result.error);
    engine.task_failed(instance_id, result.error);
    broadcast("stderr",
              "[ERROR] Task failed: " + task_id + " - " + result.error);
  }
}

auto Application::Impl::on_dag_run_task_completed(std::string_view dag_run_id,
                                                  NodeIndex task_idx) -> void {
  std::string dag_run_id_str(dag_run_id);
  bool run_completed = false;
  {
    std::lock_guard lock(dag_runs_mu);
    auto it = dag_runs.find(dag_run_id_str);
    if (it != dag_runs.end()) {
      it->second->mark_task_completed(task_idx, 0);

      if (persistence) {
        auto info = it->second->get_task_info(task_idx);
        if (info) {
          if (auto r = persistence->update_task_instance(dag_run_id, *info);
              !r) {
            log::warn("Failed to update task instance: {}",
                      r.error().message());
          }
        }

        if (it->second->is_complete()) {
          if (auto r = persistence->update_dag_run_state(dag_run_id,
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
    completion_cv.notify_all();
    return;
  }

  execute_ready_tasks(dag_run_id_str);
}

auto Application::list_tasks() const -> void {
  std::cout << "Tasks:\n";

  for (const auto &task : impl_->config.tasks) {
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
  std::cout << "Status: " << (impl_->running.load() ? "running" : "stopped")
            << "\n";
  std::cout << "Tasks loaded: " << impl_->config.tasks.size() << "\n";
  std::cout << "Active DAG runs: " << impl_->dag_runs.size() << "\n";
}

auto Application::recover_from_crash() -> Result<void> {
  if (!impl_->persistence || !impl_->persistence->is_open()) {
    return fail(Error::DatabaseError);
  }

  Recovery recovery(*impl_->persistence);
  auto result = recovery.recover(
      [this](const std::string &) -> const DAG & { return impl_->dag; },
      [this](const std::string &dag_run_id, DAGRun &run) {
        impl_->restore_dag_run(dag_run_id,
                               std::make_unique<DAGRun>(std::move(run)));
      });

  if (!result) {
    return fail(result.error());
  }

  log::info("Recovery: {} runs recovered, {} tasks to retry",
            result->recovered_dag_runs.size(), result->tasks_to_retry);

  return ok();
}

auto Application::config() const noexcept -> const Config & {
  return impl_->config;
}

auto Application::config() noexcept -> Config & { return impl_->config; }

auto Application::dag_manager() -> DAGManager & { return impl_->dag_manager; }

auto Application::dag_manager() const -> const DAGManager & {
  return impl_->dag_manager;
}

auto Application::engine() -> Engine & { return impl_->engine; }

auto Application::api_server() -> ApiServer * {
  return impl_->api_server.get();
}

} // namespace taskmaster
