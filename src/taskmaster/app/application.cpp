#include "taskmaster/app/application.hpp"

#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/app/services/event_service.hpp"
#include "taskmaster/app/services/execution_service.hpp"
#include "taskmaster/app/services/persistence_service.hpp"
#include "taskmaster/app/services/scheduler_service.hpp"
#include "taskmaster/config/config_watcher.hpp"
#include "taskmaster/config/dag_definition.hpp"
#include "taskmaster/config/dag_file_loader.hpp"
#include "taskmaster/core/constants.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_validator.hpp"
#include "taskmaster/util/log.hpp"

#include <chrono>
#include <cstring>
#include <filesystem>
#include <sys/inotify.h>
#include <thread>
#include <unistd.h>

#include "taskmaster/executor/config_builder.hpp"
#include "taskmaster/storage/recovery.hpp"

namespace taskmaster {

Application::Application()
    : executor_(create_shell_executor(runtime_)),
      events_(std::make_unique<EventService>()),
      scheduler_(std::make_unique<SchedulerService>(runtime_)),
      execution_(std::make_unique<ExecutionService>(runtime_, *executor_)) {
  setup_callbacks();
}

Application::Application(std::string_view db_path)
    : executor_(create_shell_executor(runtime_)),
      persistence_(std::make_unique<PersistenceService>(db_path)),
      events_(std::make_unique<EventService>()),
      scheduler_(std::make_unique<SchedulerService>(runtime_)),
      execution_(std::make_unique<ExecutionService>(runtime_, *executor_)) {
  dag_manager_.set_persistence(persistence_->persistence());
  setup_callbacks();
}

Application::Application(Config config)
    : config_(std::move(config)),
      executor_(create_shell_executor(runtime_)),
      persistence_(std::make_unique<PersistenceService>(config_.storage.db_file)),
      events_(std::make_unique<EventService>()),
      scheduler_(std::make_unique<SchedulerService>(runtime_)),
      execution_(std::make_unique<ExecutionService>(runtime_, *executor_)) {
  dag_manager_.set_persistence(persistence_->persistence());
  setup_callbacks();
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

auto Application::config() const noexcept -> const Config& {
  return config_;
}

auto Application::config() noexcept -> Config& {
  return config_;
}

auto Application::init() -> Result<void> {
  if (persistence_ && !persistence_->is_open()) {
    if (auto r = persistence_->open(); !r) {
      return fail(r.error());
    }
  }
  auto config_loaded = load_dags_from_directory(config_.dag_source.directory);
  if (!config_loaded) {
    return fail(config_loaded.error());
  }
  if(*config_loaded) {
    return ok();
  }
  return dag_manager_.load_from_database();
}

auto Application::init_db_only() -> Result<void> {
  if (persistence_ && !persistence_->is_open()) {
    if (auto r = persistence_->open(); !r) {
      return fail(r.error());
    }
  }
  return dag_manager_.load_from_database();
}

auto Application::load_dags_from_directory(std::string_view dags_dir) -> Result<bool> {
  DAGFileLoader loader(dags_dir);
  auto dags_result = loader.load_all();
  if(!dags_result) {
    return fail(dags_result.error());
  }
  if (dags_result->empty()) {
    return ok(false);
  }

  if(persistence_) {
    if(auto r = persistence_->persistence()->clear_all_dag_data(); !r) {
      return fail(r.error());
    }
  }

  dag_manager_.clear_all();

  for (const auto& dag_file : *dags_result) {
    const auto& dag_id = dag_file.dag_id;
    const auto& def = dag_file.definition;

    DAGInfo info;
    info.dag_id = dag_id;
    info.name = def.name;
    info.description = def.description;
    info.cron = def.cron;
    info.tasks = def.tasks;
    info.created_at = std::chrono::system_clock::now();
    info.updated_at = info.created_at;
    info.rebuild_task_index();

    if(auto r = validate_dag_info(info); !r) {
      return fail(r.error());
    }

    auto create_result = create_dag_atomically(dag_id, info);
    if (!create_result) {
      log::error("Failed to create DAG {}: {}", dag_id, create_result.error().message());
      continue;
    }

    log::info("Loaded DAG {} with {} tasks from {}", dag_id, def.tasks.size(), dags_dir);
  }

  return ok(true);
}

auto Application::get_run_state(DAGRunId dag_run_id) const -> std::optional<DAGRunState> {
  if (!execution_) {
    return std::nullopt;
  }
  auto* run = execution_->get_run(dag_run_id);
  if (!run) {
    return std::nullopt;
  }
  return run->state();
}

auto Application::start() -> Result<void> {
  if (running_.exchange(true))
    return ok();

  if (persistence_ && !persistence_->is_open()) {
    if (auto r = persistence_->open(); !r) {
      running_ = false;
      return fail(r.error());
    }
  }

  runtime_.start();

  for (const auto& d : dag_manager_.list_dags() | std::views::filter([](const auto& d) { return !d.cron.empty(); })) {
    scheduler_->register_dag(d.dag_id, d);
  }

  scheduler_->start();

  if (config_.api.enabled) {
    api_ = std::make_unique<ApiServer>(*this);
    api_->start();
    log::info("API server started on {}:{}", config_.api.host, config_.api.port);
  }

  setup_config_watcher();

  return ok();
}

auto Application::stop() -> void {
  if (!running_.exchange(false))
    return;

  log::info("Stopping TaskMaster...");

  if (api_)
    api_->stop();

  scheduler_->stop();

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

  callbacks.on_task_status = [this](DAGRunId dag_run_id,
                                    TaskId task,
                                    std::string_view status) {
    events_->emit_task_status(dag_run_id, task, status);
  };

  callbacks.on_run_status = [this](DAGRunId dag_run_id,
                                   std::string_view status) {
    events_->emit_run_status(dag_run_id, status);
  };

  callbacks.on_log = [this](DAGRunId dag_run_id, TaskId task,
                            std::string_view stream, std::string msg) {
    events_->emit_log(dag_run_id, task, stream, std::move(msg));
  };

  callbacks.on_persist_run = [this](const DAGRun& run) {
    if (persistence_) {
      persistence_->save_run(run);
    }
  };

  callbacks.on_persist_task = [this](DAGRunId dag_run_id,
                                     const TaskInstanceInfo& info) {
    if (persistence_) {
      persistence_->save_task(dag_run_id, info);
    }
  };

  callbacks.on_persist_log =
      [this](DAGRunId dag_run_id, TaskId task, int attempt,
             std::string_view level, std::string_view msg) {
        if (persistence_) {
            persistence_->save_log(dag_run_id, task, attempt, level, msg);
        }
      };

  callbacks.on_persist_xcom =
      [this](DAGRunId dag_run_id, TaskId task,
             std::string_view key, const nlohmann::json& value) {
        if (persistence_) {
          persistence_->save_xcom(dag_run_id, task, key, value);
        }
      };

  callbacks.get_xcom =
      [this](DAGRunId dag_run_id, TaskId task,
             std::string_view key) -> Result<nlohmann::json> {
        if (persistence_) {
          return persistence_->get_xcom(dag_run_id, task, key);
        }
        return fail(Error::NotFound);
      };

  callbacks.get_max_retries = [this](DAGRunId dag_run_id, NodeIndex idx) {
    return get_max_retries(DAGRunId(dag_run_id), idx);
  };

  callbacks.get_retry_interval = [this](const DAGRunId& dag_run_id, NodeIndex idx) {
    return get_retry_interval(dag_run_id, idx);
  };

  execution_->set_callbacks(std::move(callbacks));

  scheduler_->set_on_dag_trigger([this](const DAGId& dag_id) {
    log::info("Cron triggered DAG: {}", dag_id);
    if (!trigger_dag_by_id(dag_id, TriggerType::Schedule)) {
      log::error("Failed to trigger scheduled DAG: {}", dag_id);
    }
  });
}

auto Application::trigger_dag_by_id(DAGId dag_id,
                                    TriggerType trigger) -> std::optional<DAGRunId> {
  auto info = dag_manager_.get_dag(dag_id);
  if (!info) {
    log::error("DAG {} not found", dag_id);
    return std::nullopt;
  }
  if (info->tasks.empty()) {
    log::warn("DAG {} has no tasks", dag_id);
    return std::nullopt;
  }

  auto graph = dag_manager_.build_dag_graph(dag_id);
  if (!graph) {
    log::error("Failed to build DAG graph for {}", dag_id);
    return std::nullopt;
  }

  auto dag_run_id = generate_dag_run_id(dag_id);

  auto cfgs = ExecutorConfigBuilder::build(*info, *graph);

  auto run_result = DAGRun::create(dag_run_id, *graph);
  if (!run_result) {
    log::error("Failed to create DAGRun for {}", dag_run_id);
    return std::nullopt;
  }
  auto run = std::make_unique<DAGRun>(std::move(*run_result));
  auto now = std::chrono::system_clock::now();
  run->set_scheduled_at(now);
  run->set_started_at(now);
  run->set_trigger_type(trigger);

  for (const auto& t : info->tasks) {
    NodeIndex idx = graph->get_index(t.task_id);
    if (idx != kInvalidNode && idx < graph->size()) {
      run->set_instance_id(idx, generate_instance_id(dag_run_id, t.task_id));
    }
  }

  if (persistence_) {
    persistence_->save_run(*run);
    for (const auto& ti : run->all_task_info()) {
      persistence_->save_task(dag_run_id, ti);
    }
  }

  execution_->start_run(dag_run_id, std::move(run), std::move(cfgs),
                        info->tasks);
  log::info("DAG run {} triggered for {} ({})", dag_run_id, dag_id,
            trigger == TriggerType::Schedule ? "schedule" : "manual");
  return dag_run_id;
}

auto Application::wait_for_completion(int timeout_ms) -> void {
  if (execution_)
    execution_->wait_for_completion(timeout_ms);
}

auto Application::has_active_runs() const -> bool {
  return execution_ && execution_->has_active_runs();
}

auto Application::get_max_retries(DAGRunId dag_run_id, NodeIndex idx)
    -> int {
  DAGId dag_id = extract_dag_id(dag_run_id);

  if (auto dag_info = dag_manager_.get_dag(dag_id)) {
    if (idx < dag_info->tasks.size()) {
      return dag_info->tasks[idx].max_retries;
    }
  }

  return 3;
}

auto Application::get_retry_interval(DAGRunId dag_run_id, NodeIndex idx)
    -> std::chrono::seconds {
  DAGId dag_id = extract_dag_id(dag_run_id);

  if (auto dag_info = dag_manager_.get_dag(dag_id)) {
    if (idx < dag_info->tasks.size()) {
      return dag_info->tasks[idx].retry_interval;
    }
  }

  return std::chrono::seconds(60);
}

auto Application::register_dag_cron(DAGId dag_id,
                                    std::string_view /*cron_expr*/) -> bool {
  auto dag = dag_manager_.get_dag(dag_id);
  if (!dag || dag->tasks.empty()) {
    log::error("Cannot register cron for DAG {}: not found or empty", dag_id);
    return false;
  }
  scheduler_->register_dag(dag_id, *dag);
  return true;
}

auto Application::unregister_dag_cron(DAGId dag_id) -> void {
  scheduler_->unregister_dag(dag_id);
}

auto Application::update_dag_cron(DAGId dag_id,
                                  std::string_view cron_expr, bool is_active)
    -> void {
  unregister_dag_cron(dag_id);

  if (!cron_expr.empty() && is_active) {
    register_dag_cron(dag_id, cron_expr);
  }
}

auto Application::recover_from_crash() -> Result<void> {
  if (!persistence_ || !persistence_->is_open()) {
    return fail(Error::DatabaseError);
  }

  Recovery recovery(*persistence_->persistence());
  auto result = recovery.recover(
      [this](DAGRunId dag_run_id) -> TaskDAG {
        auto run_result = persistence_->persistence()->get_run_history(dag_run_id);
        if (!run_result) {
          log::error("Failed to get run history for {}: {}", dag_run_id,
                     run_result.error().message());
          return DAG{};
        }

        const auto& run_entry = *run_result;
        auto dag_result = dag_manager_.build_dag_graph(run_entry.dag_id);
        if (!dag_result) {
          log::error("Failed to build DAG {}: {}", run_entry.dag_id,
                     dag_result.error().message());
          return DAG{};
        }

        return std::move(*dag_result);
      },
      [this](DAGRunId dag_run_id, DAGRun& run) {
        auto run_result = persistence_->persistence()->get_run_history(dag_run_id);
        if (!run_result) {
          log::error("Failed to get run history for {}: {}", dag_run_id,
                     run_result.error().message());
          return;
        }

        const auto& run_entry = *run_result;
        auto dag_info_result = dag_manager_.get_dag(run_entry.dag_id);
        if (!dag_info_result) {
          log::error("Failed to get DAG info for {}: {}", run_entry.dag_id,
                     dag_info_result.error().message());
          return;
        }

        const auto& dag_info = *dag_info_result;
        std::vector<ExecutorConfig> executor_cfgs;
        executor_cfgs.reserve(dag_info.tasks.size());
        for (const auto& task : dag_info.tasks) {
          ShellExecutorConfig exec;
          exec.command = task.command;
          exec.working_dir = task.working_dir;
          exec.timeout = task.timeout;
          executor_cfgs.push_back(exec);
        }

        execution_->start_run(dag_run_id, std::make_unique<DAGRun>(std::move(run)),
                              executor_cfgs);
      });

  if (!result)
    return fail(result.error());

  log::info("Recovery: {} runs, {} tasks to retry",
            result->recovered_dag_runs.size(), result->tasks_to_retry);
  return ok();
}

auto Application::list_tasks() const -> void {
  for (const auto& dag : dag_manager_.list_dags()) {
    std::println("DAG: {} ({} tasks)", dag.dag_id, dag.tasks.size());
    std::println("  {:<20} {:<10} {}", "TASK", "TYPE", "DEPS");
    
    for (const auto& task : dag.tasks) {
      std::string deps = "-";
      if (!task.dependencies.empty()) {
        deps.clear();
        for (auto [i, dep] : task.dependencies | std::views::enumerate) {
          if (i > 0) deps += ",";
          deps += dep.value();
        }
      }
      
      std::println("  {:<20} {:<10} {}", 
                   task.task_id,
                   executor_type_to_string(task.executor),
                   deps);
    }
    std::println("");
  }
}

auto Application::show_status() const -> void {
  std::println("Status: {}", running_.load() ? "running" : "stopped");
  std::println("DAGs: {}", dag_manager_.list_dags().size());
  std::println("Active runs: {}", execution_ ? "checking..." : "N/A");
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

auto Application::runtime() -> Runtime& {
  return runtime_;
}

auto Application::validate_dag_info(const DAGInfo& info) -> Result<void> {
  return DAGValidator::validate(info);
}

auto Application::create_dag_atomically(DAGId dag_id,
                                        const DAGInfo& info) -> Result<void> {
  if (auto result = dag_manager_.create_dag(dag_id, info); !result) {
    if(auto r = dag_manager_.delete_dag(dag_id); !r) {
      log::error("Failed to delete DAG: {}", r.error().message());
    }
    return fail(result.error());
  }
  return ok();
}

auto Application::setup_config_watcher() -> void {
  if (config_.dag_source.directory.empty()) {
    return;
  }

  inotify_fd_ = inotify_init1(IN_NONBLOCK);
  if (inotify_fd_ < 0) {
    log::error("Failed to initialize inotify: {}", strerror(errno));
    return;
  }

  watch_descriptor_ = inotify_add_watch(inotify_fd_, config_.dag_source.directory.c_str(),
                                        IN_CREATE | IN_MODIFY | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO);
  if (watch_descriptor_ < 0) {
    log::error("Failed to add watch on {}: {}", config_.dag_source.directory, strerror(errno));
    close(inotify_fd_);
    inotify_fd_ = -1;
    return;
  }

  watching_ = true;
  watcher_thread_ = std::jthread([this](std::stop_token stop) {
    config_watcher_loop(stop);
  });
}

auto Application::config_watcher_loop(std::stop_token stop) -> void {
  std::array<char, io::kEventBufferSize> buffer{};

  while (!stop.stop_requested() && watching_.load()) {
    ssize_t len = read(inotify_fd_, buffer.data(), buffer.size());
    if (len <= 0) {
      if (errno != EAGAIN) {
          std::this_thread::sleep_for(timing::kConfigWatchInterval);
      } else {
          std::this_thread::sleep_for(timing::kShutdownPollInterval);
      }
      continue;
    }

    ssize_t i = 0;
    while (i < len) {
      auto* event = reinterpret_cast<inotify_event*>(buffer.data() + i);

      if (event->len > 0) {
        std::filesystem::path file_path = std::filesystem::path(config_.dag_source.directory) / event->name;
        auto ext = file_path.extension().string();

        if (ext == ".yaml" || ext == ".yml") {
             handle_file_change(file_path.string());
        }
      }

      i += sizeof(inotify_event) + event->len;
    }
  }
}

auto Application::handle_file_change(const std::string& filename) -> void {
    if (!std::filesystem::exists(filename)) {
        std::filesystem::path p(filename);
        DAGId dag_id{p.stem().string()};
        
        log::info("DAG file removed: {}", filename);
        if (auto r = dag_manager_.delete_dag(dag_id); !r) {
             log::warn("Failed to delete DAG {}: {}", dag_id, r.error().message());
        }
        scheduler_->unregister_dag(dag_id);
        return;
    }

    log::info("DAG file changed: {}", filename);
    
    DAGFileLoader loader(config_.dag_source.directory);
    auto result = loader.load_file(filename);
    
    if (!result) {
        log::error("Failed to load DAG from {}: {}", filename, result.error().message());
        return;
    }

    if (auto r = reload_single_dag(result->definition); !r) {
        log::error("Failed to reload DAG from {}: {}", filename, r.error().message());
    } else {
        log::info("Successfully reloaded DAG from {}", filename);
    }
}

auto Application::reload_single_dag(const DAGDefinition& def) -> Result<void> {
    if (def.source_file.empty()) {
        return fail(Error::InvalidArgument);
    }
    
    std::filesystem::path p(def.source_file);
    DAGId dag_id{p.stem().string()};

    DAGInfo info;
    info.dag_id = dag_id;
    info.name = def.name;
    info.description = def.description;
    info.cron = def.cron;
    info.tasks = def.tasks;
    info.created_at = std::chrono::system_clock::now();
    info.updated_at = info.created_at;
    info.rebuild_task_index();

    if(auto r = validate_dag_info(info); !r) {
      return fail(r.error());
    }

    if (dag_manager_.has_dag(dag_id)) {
        if(auto r = dag_manager_.delete_dag(dag_id); !r) {
            log::warn("Failed to clean up old DAG version for reload: {}", r.error().message());
        }
    }
    
    if (auto r = dag_manager_.create_dag(dag_id, info); !r) {
        return fail(r.error());
    }
    
    update_dag_cron(dag_id, def.cron, true);
    
    return ok();
}



}  // namespace taskmaster
