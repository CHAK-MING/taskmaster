#include "taskmaster/api/api_server.hpp"
#include "taskmaster/application.hpp"
#include "taskmaster/dag_manager.hpp"

#include <chrono>
#include <future>

#include "taskmaster/log.hpp"
#include <nlohmann/json.hpp>

namespace taskmaster {

using json = nlohmann::json;

namespace {

// Cached timestamp - regenerated at most once per second
thread_local std::string cached_timestamp;
thread_local std::chrono::seconds cached_timestamp_sec{0};

auto current_timestamp() -> const std::string & {
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

auto json_response(const json &j, int status = 200) -> crow::response {
  crow::response resp(status, j.dump());
  resp.set_header("Content-Type", "application/json");
  return resp;
}

auto task_to_json(const TaskConfig &task) -> json {
  return {{"id", task.id},
          {"name", task.name},
          {"command", task.command},
          {"cron", task.cron},
          {"deps", task.deps},
          {"timeout", task.timeout.count()},
          {"max_retries", task.max_retries},
          {"enabled", task.enabled}};
}

auto dag_to_json(const DAGInfo &dag) -> json {
  json tasks = json::array();
  for (const auto &task : dag.tasks) {
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
          {"created_at", to_iso_string(dag.created_at)},
          {"updated_at", to_iso_string(dag.updated_at)},
          {"tasks", tasks},
          {"task_count", dag.tasks.size()},
          {"from_config", dag.from_config}};
}

auto parse_task_config(const json &j) -> TaskConfig {
  TaskConfig task;
  task.id = j.value("id", "");
  task.name = j.value("name", task.id);
  task.command = j.value("command", "");
  task.cron = j.value("cron", "");
  task.working_dir = j.value("working_dir", "");
  task.deps = j.value("deps", std::vector<std::string>{});
  task.timeout = std::chrono::seconds(j.value("timeout", 300));
  task.max_retries = j.value("max_retries", 3);
  task.enabled = j.value("enabled", true);
  return task;
}

} // namespace

ApiServer::ApiServer(Application &app, uint16_t port, const std::string &host)
    : app_(app), port_(port), host_(host) {}

ApiServer::~ApiServer() { stop(); }

auto ApiServer::start() -> void {
  if (running_.exchange(true)) {
    return;
  }

  crow_app_ = std::make_unique<crow::SimpleApp>();
  setup_routes();
  setup_websocket();

  crow_app_->signal_clear();

  server_thread_ = std::thread([this]() {
    log::info("API server starting on {}:{}", host_, port_);
    crow_app_->bindaddr(host_).port(port_).multithreaded().run();
  });
}

auto ApiServer::stop() -> void {
  if (!running_.exchange(false)) {
    return;
  }

  log::info("Stopping API server...");

  if (crow_app_) {
    crow_app_->stop();
  }

  if (server_thread_.joinable()) {
    // 等待线程结束，最多等待 3 秒
    auto future =
        std::async(std::launch::async, [this]() { server_thread_.join(); });

    if (future.wait_for(std::chrono::seconds(3)) ==
        std::future_status::timeout) {
      log::warn("API server thread did not stop in time, detaching...");
      server_thread_.detach();
    }
  }

  log::info("API server stopped");
}

auto ApiServer::is_running() const noexcept -> bool { return running_.load(); }

auto ApiServer::setup_routes() -> void {
  CROW_ROUTE((*crow_app_), "/api/health")
  ([this]() {
    json j = {{"status", app_.is_running() ? "healthy" : "stopped"},
              {"timestamp", current_timestamp()}};
    return json_response(j);
  });

  CROW_ROUTE((*crow_app_), "/api/status")
  ([this]() {
    json j = {{"running", app_.is_running()},
              {"tasks", app_.config().tasks.size()},
              {"dags", app_.dag_manager().dag_count()},
              {"active_runs", app_.has_active_runs() ? 1 : 0},
              {"timestamp", current_timestamp()}};
    return json_response(j);
  });

  CROW_ROUTE((*crow_app_), "/api/tasks")
  ([this]() {
    json tasks = json::array();
    for (const auto &task : app_.config().tasks) {
      tasks.push_back({{"id", task.id},
                       {"name", task.name},
                       {"command", task.command},
                       {"cron", task.cron},
                       {"deps", task.deps},
                       {"enabled", task.enabled}});
    }
    return json_response(tasks);
  });

  CROW_ROUTE((*crow_app_), "/api/tasks/<string>")
  ([this](const std::string &task_id) {
    const auto *task = app_.config().find_task(task_id);
    if (!task) {
      return error_response("NOT_FOUND", "Task not found");
    }
    json j = {{"id", task->id},
              {"name", task->name},
              {"command", task->command},
              {"cron", task->cron},
              {"deps", task->deps},
              {"timeout", task->timeout.count()},
              {"max_retries", task->max_retries},
              {"enabled", task->enabled}};
    return json_response(j);
  });

  CROW_ROUTE((*crow_app_), "/api/trigger/<string>")
      .methods(crow::HTTPMethod::POST)([this](const std::string &task_id) {
        if (!app_.config().find_task(task_id)) {
          return error_response("NOT_FOUND", "Task not found");
        }

        app_.trigger_dag(task_id);
        json j = {{"status", "triggered"}, {"task_id", task_id}};
        return json_response(j, 202);
      });

  // ========== DAG Management Routes ==========

  // List all DAGs
  CROW_ROUTE((*crow_app_), "/api/dags")
  ([this]() {
    auto dags = app_.dag_manager().list_dags();
    json result = json::array();
    for (const auto &dag : dags) {
      result.push_back(dag_to_json(dag));
    }
    return json_response(result);
  });

  // Create new DAG
  CROW_ROUTE((*crow_app_), "/api/dags")
      .methods(crow::HTTPMethod::POST)([this](const crow::request &req) {
        try {
          auto body = json::parse(req.body);
          std::string name = body.value("name", "");
          std::string description = body.value("description", "");

          if (name.empty()) {
            return error_response("INVALID_ARGUMENT", "DAG name is required");
          }

          auto result = app_.dag_manager().create_dag(name, description);
          if (!result) {
            return error_response("CREATE_FAILED", result.error().message());
          }

          auto dag = app_.dag_manager().get_dag(*result);
          if (!dag) {
            return error_response("NOT_FOUND", "DAG not found after creation");
          }

          return json_response(dag_to_json(*dag), 201);
        } catch (const json::exception &e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  // Get DAG details
  CROW_ROUTE((*crow_app_), "/api/dags/<string>")
  ([this](const std::string &dag_id) {
    auto dag = app_.dag_manager().get_dag(dag_id);
    if (!dag) {
      return error_response("NOT_FOUND", "DAG not found");
    }
    return json_response(dag_to_json(*dag));
  });

  // Update DAG
  CROW_ROUTE((*crow_app_), "/api/dags/<string>")
      .methods(crow::HTTPMethod::PUT)([this](const crow::request &req,
                                             const std::string &dag_id) {
        try {
          auto body = json::parse(req.body);
          std::string name = body.value("name", "");
          std::string description = body.value("description", "");

          auto result =
              app_.dag_manager().update_dag(dag_id, name, description);
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

          auto dag = app_.dag_manager().get_dag(dag_id);
          return json_response(dag_to_json(*dag));
        } catch (const json::exception &e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  // Delete DAG
  CROW_ROUTE((*crow_app_), "/api/dags/<string>")
      .methods(crow::HTTPMethod::DELETE)([this](const std::string &dag_id) {
        auto result = app_.dag_manager().delete_dag(dag_id);
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

  // ========== Task Management Routes (within DAG) ==========

  // List tasks in DAG
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/tasks")
  ([this](const std::string &dag_id) {
    auto dag = app_.dag_manager().get_dag(dag_id);
    if (!dag) {
      return error_response("NOT_FOUND", "DAG not found");
    }
    json tasks = json::array();
    for (const auto &task : dag->tasks) {
      tasks.push_back(task_to_json(task));
    }
    return json_response(tasks);
  });

  // Add task to DAG
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/tasks")
      .methods(crow::HTTPMethod::POST)([this](const crow::request &req,
                                              const std::string &dag_id) {
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

          // Check for cycle before adding
          if (app_.dag_manager().would_create_cycle(dag_id, task.id,
                                                    task.deps)) {
            return error_response("CYCLE_DETECTED",
                                  "Adding this task would create a cycle");
          }

          auto result = app_.dag_manager().add_task(dag_id, task);
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

          // Register task with engine for cron scheduling
          app_.register_task_with_engine(dag_id, task);

          return json_response(task_to_json(task), 201);
        } catch (const json::exception &e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  // Get task details
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/tasks/<string>")
  ([this](const std::string &dag_id, const std::string &task_id) {
    auto task = app_.dag_manager().get_task(dag_id, task_id);
    if (!task) {
      return error_response("NOT_FOUND", "Task not found");
    }
    return json_response(task_to_json(*task));
  });

  // Update task
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/tasks/<string>")
      .methods(crow::HTTPMethod::PUT)([this](const crow::request &req,
                                             const std::string &dag_id,
                                             const std::string &task_id) {
        try {
          auto body = json::parse(req.body);
          auto task = parse_task_config(body);
          task.id = task_id; // Preserve original ID

          // Check for cycle with new dependencies
          if (app_.dag_manager().would_create_cycle(dag_id, task_id,
                                                    task.deps)) {
            return error_response("CYCLE_DETECTED",
                                  "This update would create a cycle");
          }

          auto result = app_.dag_manager().update_task(dag_id, task_id, task);
          if (!result) {
            if (result.error() == make_error_code(Error::NotFound)) {
              return error_response("NOT_FOUND", "DAG or task not found");
            }
            return error_response("UPDATE_FAILED", result.error().message());
          }

          return json_response(task_to_json(task));
        } catch (const json::exception &e) {
          return error_response("PARSE_ERROR", e.what());
        }
      });

  // Delete task
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/tasks/<string>")
      .methods(crow::HTTPMethod::DELETE)(
          [this](const std::string &dag_id, const std::string &task_id) {
            // Unregister from engine first
            app_.unregister_task_from_engine(dag_id, task_id);

            auto result = app_.dag_manager().delete_task(dag_id, task_id);
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

  // Trigger DAG run
  CROW_ROUTE((*crow_app_), "/api/dags/<string>/trigger")
      .methods(crow::HTTPMethod::POST)([this](const std::string &dag_id) {
        if (!app_.dag_manager().has_dag(dag_id)) {
          return error_response("NOT_FOUND", "DAG not found");
        }

        // Validate DAG before triggering
        auto validate_result = app_.dag_manager().validate_dag(dag_id);
        if (!validate_result) {
          return error_response("INVALID_DAG",
                                "DAG validation failed - may contain cycles");
        }

        app_.trigger_dag_by_id(dag_id);
        json j = {{"status", "triggered"}, {"dag_id", dag_id}};
        return json_response(j, 202);
      });
}

auto ApiServer::setup_websocket() -> void {
  CROW_WEBSOCKET_ROUTE((*crow_app_), "/ws/logs")
      .onopen([this](crow::websocket::connection &conn) {
        log::debug("WebSocket connection opened");
        hub_.add_connection(&conn);
        json welcome = {{"type", "connected"},
                        {"timestamp", current_timestamp()}};
        conn.send_text(welcome.dump());
      })
      .onclose(
          [this](crow::websocket::connection &conn, const std::string &reason) {
            log::debug("WebSocket connection closed: {}", reason);
            hub_.remove_connection(&conn);
          })
      .onmessage([](crow::websocket::connection &, const std::string &data,
                    bool is_binary) {
        if (is_binary)
          return;
        log::debug("WebSocket message: {}", data);
      });
}

} // namespace taskmaster
