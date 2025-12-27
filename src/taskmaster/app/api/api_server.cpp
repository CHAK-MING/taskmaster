#include "taskmaster/app/api/api_server.hpp"

#include "taskmaster/app/application.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <thread>

#include <crow.h>

namespace taskmaster {

using json = nlohmann::json;

namespace {

// Cached timestamp - regenerated at most once per second
thread_local std::string cached_timestamp;
thread_local std::chrono::seconds cached_timestamp_sec{0};

auto current_timestamp() -> const std::string& {
  auto now = std::chrono::system_clock::now();
  auto sec =
      std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch());

  if (sec != cached_timestamp_sec) {
    cached_timestamp_sec = sec;
    auto time = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    cached_timestamp = buf;
  }
  return cached_timestamp;
}

auto error_response(std::string_view code, std::string_view message)
    -> crow::response {
  json j = {{"error", {{"code", code}, {"message", message}}}};
  crow::response resp(j.dump());
  resp.set_header("Content-Type", "application/json");
  return resp;
}

auto json_response(const json& j, int status = 200) -> crow::response {
  crow::response resp(status, j.dump());
  resp.set_header("Content-Type", "application/json");
  return resp;
}

auto task_to_json(const TaskConfig& task) -> json {
  return {{"id", task.id},
          {"name", task.name},
          {"command", task.command},
          {"working_dir", task.working_dir},
          {"executor", std::string(executor_type_to_string(task.executor))},
          {"deps", task.deps},
          {"timeout", task.timeout.count()},
          {"retry_interval", task.retry_interval.count()},
          {"max_retries", task.max_retries},
          {"enabled", task.enabled}};
}

auto dag_to_json(const DAGInfo& dag) -> json {
  json tasks = json::array();
  for (const auto& task : dag.tasks) {
    tasks.push_back(task_to_json(task));
  }

  auto to_iso_string = [](std::chrono::system_clock::time_point tp) {
    auto time = std::chrono::system_clock::to_time_t(tp);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
  };

  return {{"id", dag.id},
          {"name", dag.name},
          {"description", dag.description},
          {"cron", dag.cron},
          {"max_concurrent_runs", dag.max_concurrent_runs},
          {"is_active", dag.is_active},
          {"created_at", to_iso_string(dag.created_at)},
          {"updated_at", to_iso_string(dag.updated_at)},
          {"tasks", tasks},
          {"task_count", dag.tasks.size()},
          {"from_config", dag.from_config}};
}

auto parse_task_config(const json& j) -> TaskConfig {
  TaskConfig task;
  task.id = j.value("id", "");
  task.name = j.value("name", task.id);
  task.command = j.value("command", "");
  task.working_dir = j.value("working_dir", "");
  task.deps = j.value("deps", std::vector<std::string>{});
  task.executor = string_to_executor_type(j.value("executor", "shell"));
  task.timeout = std::chrono::seconds(j.value("timeout", 300));
  task.retry_interval = std::chrono::seconds(j.value("retry_interval", 60));
  task.max_retries = j.value("max_retries", 3);
  task.enabled = j.value("enabled", true);
  return task;
}

}  // namespace

struct ApiServer::Impl {
  Application& app;
  uint16_t port;
  std::string host;
  WebSocketHub hub;

  std::unique_ptr<crow::SimpleApp> crow_app;
  std::thread server_thread;
  std::atomic<bool> running{false};

  Impl(Application& a, uint16_t p, const std::string& h)
      : app(a), port(p), host(h) {
  }

  auto setup_routes() -> void;
  auto setup_websocket() -> void;
};

ApiServer::ApiServer(Application& app, uint16_t port, const std::string& host)
    : impl_(std::make_unique<Impl>(app, port, host)) {
}

ApiServer::~ApiServer() {
  stop();
}

auto ApiServer::start() -> void {
  if (impl_->running.exchange(true)) {
    return;
  }

  impl_->crow_app = std::make_unique<crow::SimpleApp>();
  impl_->setup_routes();
  impl_->setup_websocket();

  impl_->crow_app->signal_clear();

  impl_->server_thread = std::thread([this]() {
    log::info("API server starting on {}:{}", impl_->host, impl_->port);
    impl_->crow_app->bindaddr(impl_->host)
        .port(impl_->port)
        .multithreaded()
        .run();
  });
}

auto ApiServer::stop() -> void {
  if (!impl_->running.exchange(false)) {
    return;
  }

  log::info("Stopping API server...");

  if (impl_->crow_app) {
    impl_->crow_app->stop();
  }

  if (impl_->server_thread.joinable()) {
    log::info("Waiting for API server thread to finish...");
    impl_->server_thread.join();
  }

  if (impl_->crow_app) {
    impl_->crow_app.reset();
    impl_->crow_app = nullptr;
  }

  log::info("API server stopped");
}

auto ApiServer::is_running() const noexcept -> bool {
  return impl_->running.load();
}

auto ApiServer::hub() -> WebSocketHub& {
  return impl_->hub;
}

auto ApiServer::Impl::setup_routes() -> void {
  CROW_ROUTE((*crow_app), "/api/health")
  ([this]() {
    json j = {{"status", app.is_running() ? "healthy" : "stopped"},
              {"timestamp", current_timestamp()}};
    return json_response(j);
  });

  CROW_ROUTE((*crow_app), "/api/status")
  ([this]() {
    json j = {{"running", app.is_running()},
              {"tasks", app.config().tasks.size()},
              {"dags", app.dag_manager().dag_count()},
              {"active_runs", app.has_active_runs() ? 1 : 0},
              {"timestamp", current_timestamp()}};
    return json_response(j);
  });

  // ========== DAG Management Routes ==========

  CROW_ROUTE((*crow_app), "/api/dags")
  ([this]() {
    auto dags = app.dag_manager().list_dags();
    json result = json::array();
    for (const auto& dag : dags) {
      result.push_back(dag_to_json(dag));
    }
    return json_response(result);
  });

  CROW_ROUTE((*crow_app), "/api/dags")
      .methods(crow::HTTPMethod::POST)([this](const crow::request& req) {
        try {
          auto body = json::parse(req.body);
          std::string name = body.value("name", "");
          std::string description = body.value("description", "");
          std::string cron = body.value("cron", "");
          int max_concurrent_runs = body.value("max_concurrent_runs", 1);
          bool is_active = body.value("is_active", true);

          if (name.empty()) {
            return error_response("INVALID_ARGUMENT", "DAG name is required");
          }

          auto result = app.dag_manager().create_dag(name, description);
          if (!result) {
            return error_response("CREATE_FAILED", result.error().message());
          }

          if (!cron.empty() || max_concurrent_runs != 1 || !is_active) {
            [[maybe_unused]] auto update_result = app.dag_manager().update_dag(
                *result, "", "", cron, max_concurrent_runs, is_active ? 1 : 0);
          }

          // Register cron schedule if provided and DAG is active
          if (!cron.empty() && is_active) {
            app.register_dag_cron(*result, cron);
          }

          auto dag = app.dag_manager().get_dag(*result);
          if (!dag) {
            return error_response("NOT_FOUND", "DAG not found after creation");
          }

          return json_response(dag_to_json(*dag), 201);
        } catch (const json::exception& e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>")
  ([this](const std::string& dag_id) {
    auto dag = app.dag_manager().get_dag(dag_id);
    if (!dag) {
      return error_response("NOT_FOUND", "DAG not found");
    }
    return json_response(dag_to_json(*dag));
  });

  CROW_ROUTE((*crow_app), "/api/dags/<string>")
      .methods(crow::HTTPMethod::PUT)([this](const crow::request& req,
                                             const std::string& dag_id) {
        try {
          auto body = json::parse(req.body);
          std::string name = body.value("name", "");
          std::string description = body.value("description", "");
          std::string cron = body.value("cron", "");
          int max_concurrent_runs = body.value("max_concurrent_runs", -1);
          int is_active = body.contains("is_active")
                              ? (body["is_active"].get<bool>() ? 1 : 0)
                              : -1;

          auto result = app.dag_manager().update_dag(
              dag_id, name, description, cron, max_concurrent_runs, is_active);
          if (!result) {
            if (result.error() == make_error_code(Error::NotFound)) {
              return error_response("NOT_FOUND", "DAG not found");
            }
            if (result.error() == make_error_code(Error::InvalidArgument)) {
              return error_response("READ_ONLY",
                                    "Cannot modify DAG loaded from config");
            }
            return error_response("UPDATE_FAILED", result.error().message());
          }

          // Update cron schedule if cron or is_active changed
          auto dag = app.dag_manager().get_dag(dag_id);
          if (dag) {
            app.update_dag_cron(dag_id, dag->cron, dag->is_active);
          }

          return json_response(dag_to_json(*dag));
        } catch (const json::exception& e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>")
      .methods(crow::HTTPMethod::DELETE)([this](const std::string& dag_id) {
        // Unregister cron schedule first
        app.unregister_dag_cron(dag_id);

        auto result = app.dag_manager().delete_dag(dag_id);
        if (!result) {
          if (result.error() == make_error_code(Error::NotFound)) {
            return error_response("NOT_FOUND", "DAG not found");
          }
          if (result.error() == make_error_code(Error::InvalidArgument)) {
            return error_response("READ_ONLY",
                                  "Cannot delete DAG loaded from config");
          }
          return error_response("DELETE_FAILED", result.error().message());
        }
        json j = {{"status", "deleted"}, {"dag_id", dag_id}};
        return json_response(j);
      });

  // ========== Task Management Routes ==========

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks")
  ([this](const std::string& dag_id) {
    auto dag = app.dag_manager().get_dag(dag_id);
    if (!dag) {
      return error_response("NOT_FOUND", "DAG not found");
    }
    json tasks = json::array();
    for (const auto& task : dag->tasks) {
      tasks.push_back(task_to_json(task));
    }
    return json_response(tasks);
  });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks")
      .methods(crow::HTTPMethod::POST)([this](const crow::request& req,
                                              const std::string& dag_id) {
        try {
          auto body = json::parse(req.body);
          auto task = parse_task_config(body);

          if (task.id.empty()) {
            return error_response("INVALID_ARGUMENT", "Task ID is required");
          }
          if (task.command.empty()) {
            return error_response("INVALID_ARGUMENT",
                                  "Task command is required");
          }

          if (app.dag_manager().would_create_cycle(dag_id, task.id,
                                                   task.deps)) {
            return error_response("CYCLE_DETECTED",
                                  "Adding this task would create a cycle");
          }

          auto result = app.dag_manager().add_task(dag_id, task);
          if (!result) {
            if (result.error() == make_error_code(Error::NotFound)) {
              return error_response("NOT_FOUND", "DAG or dependency not found");
            }
            if (result.error() == make_error_code(Error::AlreadyExists)) {
              return error_response("ALREADY_EXISTS", "Task ID already exists");
            }
            if (result.error() == make_error_code(Error::InvalidArgument)) {
              return error_response(
                  "INVALID_ARGUMENT",
                  "Cannot modify DAG loaded from config or cycle detected");
            }
            return error_response("ADD_FAILED", result.error().message());
          }

          app.register_task_with_engine(dag_id, task);
          return json_response(task_to_json(task), 201);
        } catch (const json::exception& e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>")
  ([this](const std::string& dag_id, const std::string& task_id) {
    auto task = app.dag_manager().get_task(dag_id, task_id);
    if (!task) {
      return error_response("NOT_FOUND", "Task not found");
    }
    return json_response(task_to_json(*task));
  });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>")
      .methods(crow::HTTPMethod::PUT)([this](const crow::request& req,
                                             const std::string& dag_id,
                                             const std::string& task_id) {
        try {
          auto body = json::parse(req.body);
          auto task = parse_task_config(body);
          task.id = task_id;

          if (app.dag_manager().would_create_cycle(dag_id, task_id,
                                                   task.deps)) {
            return error_response("CYCLE_DETECTED",
                                  "This update would create a cycle");
          }

          auto result = app.dag_manager().update_task(dag_id, task_id, task);
          if (!result) {
            if (result.error() == make_error_code(Error::NotFound)) {
              return error_response("NOT_FOUND", "DAG or task not found");
            }
            return error_response("UPDATE_FAILED", result.error().message());
          }

          return json_response(task_to_json(task));
        } catch (const json::exception& e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>")
      .methods(crow::HTTPMethod::DELETE)(
          [this](const std::string& dag_id, const std::string& task_id) {
            app.unregister_task_from_engine(dag_id, task_id);

            auto result = app.dag_manager().delete_task(dag_id, task_id);
            if (!result) {
              if (result.error() == make_error_code(Error::NotFound)) {
                return error_response("NOT_FOUND", "DAG or task not found");
              }
              if (result.error() == make_error_code(Error::InvalidArgument)) {
                return error_response(
                    "HAS_DEPENDENTS",
                    "Cannot delete task - other tasks depend on it");
              }
              return error_response("DELETE_FAILED", result.error().message());
            }
            json j = {{"status", "deleted"}, {"task_id", task_id}};
            return json_response(j);
          });

  // Enable/disable task
  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>/enable")
      .methods(crow::HTTPMethod::POST)([this](const std::string& dag_id,
                                              const std::string& task_id) {
        auto result = app.set_task_enabled(dag_id, task_id, true);
        if (!result) {
          return error_response("NOT_FOUND", "DAG or task not found");
        }
        json j = {
            {"status", "enabled"}, {"dag_id", dag_id}, {"task_id", task_id}};
        return json_response(j);
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>/disable")
      .methods(crow::HTTPMethod::POST)([this](const std::string& dag_id,
                                              const std::string& task_id) {
        auto result = app.set_task_enabled(dag_id, task_id, false);
        if (!result) {
          return error_response("NOT_FOUND", "DAG or task not found");
        }
        json j = {
            {"status", "disabled"}, {"dag_id", dag_id}, {"task_id", task_id}};
        return json_response(j);
      });

  // Trigger single task manually
  CROW_ROUTE((*crow_app), "/api/dags/<string>/tasks/<string>/trigger")
      .methods(crow::HTTPMethod::POST)([this](const std::string& dag_id,
                                              const std::string& task_id) {
        auto result = app.trigger_task(dag_id, task_id);
        if (!result) {
          return error_response("NOT_FOUND", "DAG or task not found");
        }
        json j = {
            {"status", "triggered"}, {"dag_id", dag_id}, {"task_id", task_id}};
        return json_response(j, 202);
      });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/trigger")
      .methods(crow::HTTPMethod::POST)([this](const std::string& dag_id) {
        auto dag = app.dag_manager().get_dag(dag_id);
        if (!dag) {
          return error_response("NOT_FOUND", "DAG not found");
        }

        if (dag->tasks.empty()) {
          return error_response("NO_TASKS", "DAG has no tasks to execute");
        }

        auto validate_result = app.dag_manager().validate_dag(dag_id);
        if (!validate_result) {
          return error_response("INVALID_DAG",
                                "DAG validation failed - may contain cycles");
        }

        app.trigger_dag_by_id(dag_id);
        json j = {{"status", "triggered"}, {"dag_id", dag_id}};
        return json_response(j, 202);
      });

  // ========== Run History Routes ==========

  auto dag_run_state_to_string = [](DAGRunState state) -> std::string {
    switch (state) {
      case DAGRunState::Running:
        return "running";
      case DAGRunState::Success:
        return "success";
      case DAGRunState::Failed:
        return "failed";
    }
    return "unknown";
  };

  auto trigger_type_to_string = [](TriggerType type) -> std::string {
    switch (type) {
      case TriggerType::Manual:
        return "manual";
      case TriggerType::Schedule:
        return "schedule";
      case TriggerType::Api:
        return "api";
    }
    return "manual";
  };

  auto timestamp_to_iso = [](std::int64_t ts_ms) -> std::string {
    if (ts_ms == 0)
      return "-";
    // Convert from milliseconds to seconds
    auto time = static_cast<std::time_t>(ts_ms / 1000);
    std::tm tm{};
    gmtime_r(&time, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
  };

  CROW_ROUTE((*crow_app), "/api/history")
  ([this, dag_run_state_to_string, trigger_type_to_string, timestamp_to_iso]() {
    auto* persistence = app.persistence();
    if (!persistence) {
      return json_response(json::array());
    }

    auto runs_result = persistence->list_run_history("", 50);
    if (!runs_result) {
      return json_response(json::array());
    }

    json result = json::array();
    for (const auto& rec : *runs_result) {
      // Timestamps are already in milliseconds
      auto duration_ms =
          rec.finished_at > 0 ? (rec.finished_at - rec.started_at) : 0;

      // Get task instances to calculate counts
      int total_tasks = 0;
      int completed_tasks = 0;
      int failed_tasks = 0;
      auto tasks_result = persistence->get_task_instances(rec.run_id);
      if (tasks_result) {
        total_tasks = static_cast<int>(tasks_result->size());
        for (const auto& task : *tasks_result) {
          if (task.state == TaskState::Success) {
            completed_tasks++;
          } else if (task.state == TaskState::Failed) {
            failed_tasks++;
          }
        }
      }

      // Get real DAG name from DAG manager
      std::string dag_name = rec.dag_id;
      if (auto dag = app.dag_manager().get_dag(rec.dag_id)) {
        dag_name = dag->name;
      }

      result.push_back({{"run_id", rec.run_id},
                        {"dag_id", rec.dag_id},
                        {"dag_name", dag_name},
                        {"status", dag_run_state_to_string(rec.state)},
                        {"trigger_type", trigger_type_to_string(rec.trigger_type)},
                        {"start_time", timestamp_to_iso(rec.started_at)},
                        {"end_time", timestamp_to_iso(rec.finished_at)},
                        {"duration_ms", duration_ms},
                        {"total_tasks", total_tasks},
                        {"completed_tasks", completed_tasks},
                        {"failed_tasks", failed_tasks}});
    }
    return json_response(result);
  });

  CROW_ROUTE((*crow_app), "/api/history/<string>")
  ([this, dag_run_state_to_string, trigger_type_to_string,
    timestamp_to_iso](const std::string& run_id) {
    auto* persistence = app.persistence();
    if (!persistence) {
      return error_response("NOT_FOUND", "Run not found");
    }

    auto run_result = persistence->get_run_history(run_id);
    if (!run_result) {
      return error_response("NOT_FOUND", "Run not found");
    }

    json task_runs = json::array();
    std::vector<TaskInstanceInfo> task_instances;

    auto active_run = app.get_active_dag_run(run_id);
    if (active_run) {
      task_instances = active_run->all_task_info();
    } else {
      auto tasks_result = persistence->get_task_instances(run_id);
      if (tasks_result) {
        task_instances = *tasks_result;
      }
    }

    for (const auto& task : task_instances) {
      std::string task_state;
      switch (task.state) {
        case TaskState::Pending:
          task_state = "pending";
          break;
        case TaskState::Running:
          task_state = "running";
          break;
        case TaskState::Success:
          task_state = "success";
          break;
        case TaskState::Failed:
          task_state = "failed";
          break;
        case TaskState::UpstreamFailed:
          task_state = "upstream_failed";
          break;
        case TaskState::Retrying:
          task_state = "retrying";
          break;
        default:
          task_state = "unknown";
      }
      task_runs.push_back(
          {{"task_id", task.instance_id},
           {"status", task_state},
           {"attempt", task.attempt},
           {"start_time",
            timestamp_to_iso(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    task.started_at.time_since_epoch())
                    .count())},
           {"end_time",
            timestamp_to_iso(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    task.finished_at.time_since_epoch())
                    .count())},
           {"exit_code", task.exit_code},
           {"error", task.error_message}});
    }

    int completed_tasks = 0;
    int failed_tasks = 0;
    for (const auto& task : task_instances) {
      if (task.state == TaskState::Success) {
        completed_tasks++;
      } else if (task.state == TaskState::Failed ||
                 task.state == TaskState::UpstreamFailed) {
        failed_tasks++;
      }
    }

    const auto& rec = *run_result;
    std::string dag_name = rec.dag_id;
    if (auto dag = app.dag_manager().get_dag(rec.dag_id)) {
      dag_name = dag->name;
    }

    DAGRunState current_state = rec.state;
    if (active_run) {
      current_state = active_run->state();
    }

    json j = {{"run_id", rec.run_id},
              {"dag_id", rec.dag_id},
              {"dag_name", dag_name},
              {"status", dag_run_state_to_string(current_state)},
              {"start_time", timestamp_to_iso(rec.started_at)},
              {"end_time", timestamp_to_iso(rec.finished_at)},
              {"total_tasks", task_runs.size()},
              {"completed_tasks", completed_tasks},
              {"failed_tasks", failed_tasks},
              {"task_runs", task_runs}};
    return json_response(j);
  });

  CROW_ROUTE((*crow_app), "/api/dags/<string>/history")
  ([this, dag_run_state_to_string, trigger_type_to_string,
    timestamp_to_iso](const std::string& dag_id) {
    auto* persistence = app.persistence();
    if (!persistence) {
      return json_response(json::array());
    }

    auto runs_result = persistence->list_run_history(dag_id, 50);
    if (!runs_result) {
      return json_response(json::array());
    }

    json result = json::array();
    for (const auto& rec : *runs_result) {
      // Timestamps are already in milliseconds
      auto duration_ms =
          rec.finished_at > 0 ? (rec.finished_at - rec.started_at) : 0;

      // Get task instances to calculate counts
      int total_tasks = 0;
      int completed_tasks = 0;
      int failed_tasks = 0;
      auto tasks_result = persistence->get_task_instances(rec.run_id);
      if (tasks_result) {
        total_tasks = static_cast<int>(tasks_result->size());
        for (const auto& task : *tasks_result) {
          if (task.state == TaskState::Success) {
            completed_tasks++;
          } else if (task.state == TaskState::Failed) {
            failed_tasks++;
          }
        }
      }

      std::string dag_name = rec.dag_id;
      if (auto dag = app.dag_manager().get_dag(rec.dag_id)) {
        dag_name = dag->name;
      }

      result.push_back({{"run_id", rec.run_id},
                        {"dag_id", rec.dag_id},
                        {"dag_name", dag_name},
                        {"status", dag_run_state_to_string(rec.state)},
                        {"trigger_type", trigger_type_to_string(rec.trigger_type)},
                        {"start_time", timestamp_to_iso(rec.started_at)},
                        {"end_time", timestamp_to_iso(rec.finished_at)},
                        {"duration_ms", duration_ms},
                        {"total_tasks", total_tasks},
                        {"completed_tasks", completed_tasks},
                        {"failed_tasks", failed_tasks}});
    }
    return json_response(result);
  });

  // Task Logs API
  CROW_ROUTE((*crow_app), "/api/runs/<string>/logs")
  ([this](const std::string& run_id) {
    auto* persistence = app.persistence();
    if (!persistence) {
      return json_response(json::array());
    }

    auto logs_result = persistence->get_task_logs(run_id);
    if (!logs_result) {
      return json_response(json::array());
    }

    json result = json::array();
    for (const auto& log : *logs_result) {
      result.push_back({{"id", log.id},
                        {"run_id", log.run_id},
                        {"task_id", log.task_id},
                        {"attempt", log.attempt},
                        {"timestamp", log.timestamp},
                        {"level", log.level},
                        {"stream", log.stream},
                        {"message", log.message}});
    }
    return json_response(result);
  });

  CROW_ROUTE((*crow_app), "/api/runs/<string>/tasks/<string>/logs")
  ([this](const crow::request& req, const std::string& run_id,
          const std::string& task_id) {
    auto* persistence = app.persistence();
    if (!persistence) {
      return json_response(json::array());
    }

    // Parse optional attempt query parameter
    int attempt = -1;
    auto attempt_param = req.url_params.get("attempt");
    if (attempt_param) {
      try {
        attempt = std::stoi(attempt_param);
      } catch (...) {
        // Invalid attempt parameter, ignore
      }
    }

    auto logs_result = persistence->get_task_logs(run_id, task_id, attempt);
    if (!logs_result) {
      return json_response(json::array());
    }

    json result = json::array();
    for (const auto& log : *logs_result) {
      result.push_back({{"id", log.id},
                        {"run_id", log.run_id},
                        {"task_id", log.task_id},
                        {"attempt", log.attempt},
                        {"timestamp", log.timestamp},
                        {"level", log.level},
                        {"stream", log.stream},
                        {"message", log.message}});
    }
    return json_response(result);
  });
}

auto ApiServer::Impl::setup_websocket() -> void {
  CROW_WEBSOCKET_ROUTE((*crow_app), "/ws/logs")
      .onopen([this](crow::websocket::connection& conn) {
        log::debug("WebSocket connection opened");
        hub.add_connection(&conn);
        json welcome = {{"type", "connected"},
                        {"timestamp", current_timestamp()}};
        conn.send_text(welcome.dump());
      })
      .onclose(
          [this](crow::websocket::connection& conn, const std::string& reason) {
            log::debug("WebSocket connection closed: {}", reason);
            hub.remove_connection(&conn);
          })
      .onmessage([](crow::websocket::connection&, const std::string& data,
                    bool is_binary) {
        if (is_binary)
          return;
        log::debug("WebSocket message: {}", data);
      });
}

}  // namespace taskmaster
