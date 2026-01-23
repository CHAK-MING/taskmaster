#include "taskmaster/app/api/api_server.hpp"

#include "taskmaster/app/application.hpp"
#include "taskmaster/app/http/http_parser.hpp"
#include "taskmaster/app/http/http_server.hpp"
#include "taskmaster/app/http/http_types.hpp"
#include "taskmaster/app/http/router.hpp"
#include "taskmaster/app/http/websocket.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/storage/state_strings.hpp"
#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>

#include <array>
#include <chrono>
#include <string>

namespace taskmaster {

using json = nlohmann::json;
using namespace http;

namespace {

auto current_timestamp() -> std::string {
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&time_t, &tm);
  std::array<char, 32> buf{};
  std::strftime(buf.data(), buf.size(), "%Y-%m-%d %H:%M:%S", &tm);
  return buf.data();
}

auto format_iso(int64_t timestamp_ms) -> std::string {
  if (timestamp_ms == 0)
    return "";
  auto tp =
      std::chrono::system_clock::time_point(std::chrono::milliseconds(timestamp_ms));
  auto time_t = std::chrono::system_clock::to_time_t(tp);
  std::tm tm{};
  gmtime_r(&time_t, &tm);
  std::array<char, 32> buf{};
  std::strftime(buf.data(), buf.size(), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buf.data();
}

auto to_json(const DAGInfo& dag_info) -> json {
  auto tasks = dag_info.tasks 
      | std::views::transform([](const auto& t) { return t.task_id.str(); })
      | std::ranges::to<std::vector<std::string>>();
  return {
      {"dag_id", dag_info.dag_id.str()},
      {"name", dag_info.name},
      {"description", dag_info.description},
      {"cron", dag_info.cron},
      {"max_concurrent_runs", dag_info.max_concurrent_runs},
      {"tasks", tasks},
  };
}

auto to_json(const Persistence::RunHistoryEntry& run) -> json {
  return {
      {"dag_run_id", run.dag_run_id.str()},
      {"dag_id", run.dag_id.str()},
      {"state", dag_run_state_name(run.state)},
      {"trigger_type", to_string_view(run.trigger_type)},
      {"started_at", format_iso(run.started_at)},
      {"finished_at", format_iso(run.finished_at)},
      {"execution_date", format_iso(run.execution_date)},
  };
}

auto error_response(int code, std::string_view message) -> HttpResponse {
  HttpResponse resp;
  resp.status = static_cast<HttpStatus>(code);
  resp.set_header("Content-Type", "application/json");
  resp.set_body(json{{"error", message}}.dump());
  return resp;
}

auto json_response(const json& j, HttpStatus status = HttpStatus::Ok)
    -> HttpResponse {
  HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", "application/json");
  resp.set_body(j.dump());
  return resp;
}

}  // namespace

struct ApiServer::Impl {
  Application& app_;
  std::unique_ptr<http::HttpServer> server_;
  std::unique_ptr<http::WebSocketHub> ws_hub_;

  explicit Impl(Application& app) : app_(app) {
    server_ = std::make_unique<HttpServer>(app_.runtime());
    ws_hub_ = std::make_unique<WebSocketHub>(app_.runtime());
    setup_routes();
    setup_websocket();
  }

  void setup_websocket() {
    server_->set_websocket_handler(
        [this](io::AsyncFd fd, std::string_view path, std::string sec_key) -> task<void> {
          if (path != "/ws/logs") {
            co_return;
          }
          
          co_await http::perform_websocket_handshake(fd, std::move(sec_key));
          
          int fd_num = fd.fd();
          auto conn = std::make_shared<WebSocketConnection>(std::move(fd), app_.runtime());
          ws_hub_->add_connection(conn);
          
          taskmaster::log::debug("WebSocket client connected: fd={}", fd_num);
          
          co_await conn->handle_frames([this, fd_num](http::WebSocketOpCode opcode, 
                                                   std::span<const uint8_t>) {
            if (opcode == http::WebSocketOpCode::Close) {
              ws_hub_->remove_connection(fd_num);
              taskmaster::log::info("WebSocket client disconnected: fd={}", fd_num);
            }
          });
          
          ws_hub_->remove_connection(fd_num);
        });
  }

  void setup_routes() {
    auto& router = server_->router();

    router.get("/api/health", [](const HttpRequest&) -> task<HttpResponse> {
      co_return json_response({{"status", "healthy"}});
    });

    router.get("/api/status", [this](const HttpRequest&) -> task<HttpResponse> {
      co_return json_response({
          {"dag_count", app_.dag_manager().dag_count()},
          {"active_runs", app_.has_active_runs()},
          {"timestamp", current_timestamp()},
      });
    });

    router.get("/api/dags", [this](const HttpRequest&) -> task<HttpResponse> {
      json dags = json::array();
      for (const auto& dag_info : app_.dag_manager().list_dags()) {
        dags.push_back(to_json(dag_info));
      }
      co_return json_response({{"dags", dags}});
    });

    router.get("/api/dags/{dag_id}",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id) {
                   co_return error_response(400, "Missing dag_id");
                 }
                 auto dag_result = app_.dag_manager().get_dag(DAGId{*dag_id});
                 if (!dag_result) {
                   co_return error_response(404, "DAG not found");
                 }
                 co_return json_response(to_json(*dag_result));
               });

    router.get("/api/dags/{dag_id}/tasks",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id) {
                   co_return error_response(400, "Missing dag_id");
                 }
                 auto dag_result = app_.dag_manager().get_dag(DAGId{*dag_id});
                 if (!dag_result) {
                   co_return error_response(404, "DAG not found");
                 }
                 json tasks = json::array();
                 for (const auto& task : dag_result->tasks) {
                   tasks.push_back(task.task_id.str());
                 }
                 co_return json_response({{"tasks", tasks}});
               });

    router.get("/api/dags/{dag_id}/tasks/{task_id}",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_id = req.path_param("dag_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_id || !task_id) {
                   co_return error_response(400, "Missing dag_id or task_id");
                 }
                 auto dag_result = app_.dag_manager().get_dag(DAGId{*dag_id});
                 if (!dag_result) {
                   co_return error_response(404, "DAG not found");
                 }
                 auto* task = dag_result->find_task(TaskId{*task_id});
                 if (!task) {
                   co_return error_response(404, "Task not found");
                 }
                 json deps = json::array();
                 for (const auto& dep : task->dependencies) {
                   if (dep.label.empty()) {
                     deps.push_back(dep.task_id.str());
                   } else {
                     deps.push_back({{"task", dep.task_id.str()}, {"label", dep.label}});
                   }
                 }
                 co_return json_response({
                     {"task_id", task->task_id.str()},
                     {"name", task->name},
                     {"command", task->command},
                     {"dependencies", deps},
                 });
               });

    router.post("/api/dags/{dag_id}/trigger",
                [this](const HttpRequest& req) -> task<HttpResponse> {
                  auto dag_id = req.path_param("dag_id");
                  if (!dag_id) {
                    co_return error_response(400, "Missing dag_id");
                  }
                  auto dag_run_id =
                      app_.trigger_dag_by_id(DAGId{*dag_id}, TriggerType::Manual);
                  if (!dag_run_id) {
                    co_return error_response(500, "Failed to trigger DAG");
                  }
                  co_return json_response(
                      {{"dag_run_id", dag_run_id->str()}, {"status", "triggered"}},
                      HttpStatus::Created);
                });

    router.get("/api/history", [this](const HttpRequest&) -> task<HttpResponse> {
      auto* persistence = app_.persistence();
      if (!persistence) {
        co_return error_response(500, "Persistence not available");
      }
      auto runs_result = persistence->list_run_history(std::nullopt, 50);
      if (!runs_result) {
        co_return error_response(500, "Failed to retrieve run history");
      }
      json runs = json::array();
      for (const auto& run : *runs_result) {
        runs.push_back(to_json(run));
      }
      co_return json_response({{"runs", runs}});
    });

    router.get("/api/history/{dag_run_id}",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id) {
                   co_return error_response(400, "Missing dag_run_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto run_result =
                     persistence->get_run_history(DAGRunId{*dag_run_id});
                 if (!run_result) {
                   co_return error_response(404, "Run not found");
                 }
                 co_return json_response(to_json(*run_result));
               });

    router.get("/api/dags/{dag_id}/history",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_id = req.path_param("dag_id");
                 if (!dag_id) {
                   co_return error_response(400, "Missing dag_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto runs_result =
                     persistence->list_run_history(DAGId{*dag_id}, 50);
                 if (!runs_result) {
                   co_return error_response(500, "Failed to retrieve run history");
                 }
                 json runs = json::array();
                 for (const auto& run : *runs_result) {
                   runs.push_back({
                       {"dag_run_id", run.dag_run_id.str()},
                       {"state", dag_run_state_name(run.state)},
                       {"started_at", format_iso(run.started_at)},
                       {"finished_at", format_iso(run.finished_at)},
                   });
                 }
                 co_return json_response({{"runs", runs}});
               });

    router.get("/api/runs/{dag_run_id}/tasks/{task_id}/logs",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_run_id = req.path_param("dag_run_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_run_id || !task_id) {
                   co_return error_response(400, "Missing dag_run_id or task_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto logs_result = persistence->get_task_logs(
                     DAGRunId{*dag_run_id}, TaskId{*task_id});
                 if (!logs_result) {
                   co_return error_response(500, "Failed to retrieve logs");
                 }
                 json logs = json::array();
                 for (const auto& log : *logs_result) {
                   logs.push_back({
                       {"timestamp", format_iso(log.timestamp)},
                       {"level", log.level},
                       {"stream", log.stream},
                       {"message", log.message},
                   });
                 }
                  co_return json_response({{"logs", logs}});
                });

    router.get("/api/runs/{dag_run_id}/tasks/{task_id}/xcom",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_run_id = req.path_param("dag_run_id");
                 auto task_id = req.path_param("task_id");
                 if (!dag_run_id || !task_id) {
                   co_return error_response(400, "Missing dag_run_id or task_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto xcoms_result = persistence->get_task_xcoms(
                     DAGRunId{*dag_run_id}, TaskId{*task_id});
                 if (!xcoms_result) {
                   co_return error_response(500, "Failed to retrieve XCom values");
                 }
                 json xcoms = json::object();
                 for (const auto& [key, value] : *xcoms_result) {
                   xcoms[key] = value;
                 }
                 co_return json_response({{"task_id", *task_id}, {"xcom", xcoms}});
               });

    router.get("/api/runs/{dag_run_id}/xcom",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id) {
                   co_return error_response(400, "Missing dag_run_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto run_result = persistence->get_run_history(DAGRunId{*dag_run_id});
                 if (!run_result) {
                   co_return error_response(404, "Run not found");
                 }
                 auto dag_info = app_.dag_manager().get_dag(run_result->dag_id);
                 if (!dag_info) {
                   co_return error_response(404, "DAG not found");
                 }
                 json all_xcoms = json::object();
                 for (const auto& task : dag_info->tasks) {
                   auto xcoms_result = persistence->get_task_xcoms(
                       DAGRunId{*dag_run_id}, task.task_id);
                   if (xcoms_result && !xcoms_result->empty()) {
                     json task_xcoms = json::object();
                     for (const auto& [key, value] : *xcoms_result) {
                       task_xcoms[key] = value;
                     }
                     all_xcoms[task.task_id.str()] = task_xcoms;
                   }
                 }
                 co_return json_response({{"dag_run_id", *dag_run_id}, {"xcom", all_xcoms}});
               });

    router.get("/api/runs/{dag_run_id}/tasks",
               [this](const HttpRequest& req) -> task<HttpResponse> {
                 auto dag_run_id = req.path_param("dag_run_id");
                 if (!dag_run_id) {
                   co_return error_response(400, "Missing dag_run_id");
                 }
                 auto* persistence = app_.persistence();
                 if (!persistence) {
                   co_return error_response(500, "Persistence not available");
                 }
                 auto run_result = persistence->get_run_history(DAGRunId{*dag_run_id});
                 if (!run_result) {
                   co_return error_response(404, "Run not found");
                 }
                 auto dag_info = app_.dag_manager().get_dag(run_result->dag_id);
                 if (!dag_info) {
                   co_return error_response(404, "DAG not found");
                 }
                 auto tasks_result = persistence->get_task_instances(DAGRunId{*dag_run_id});
                 if (!tasks_result) {
                   co_return error_response(500, "Failed to retrieve task instances");
                 }
                 json tasks = json::array();
                 for (const auto& t : *tasks_result) {
                   std::string task_id_str = (t.task_idx < dag_info->tasks.size())
                       ? dag_info->tasks[t.task_idx].task_id.str()
                       : std::format("task_{}", t.task_idx);
                   tasks.push_back({
                       {"task_id", task_id_str},
                       {"state", task_state_name(t.state)},
                       {"attempt", t.attempt},
                       {"exit_code", t.exit_code},
                       {"started_at", format_iso(
                           std::chrono::duration_cast<std::chrono::milliseconds>(
                               t.started_at.time_since_epoch()).count())},
                       {"finished_at", format_iso(
                           std::chrono::duration_cast<std::chrono::milliseconds>(
                               t.finished_at.time_since_epoch()).count())},
                       {"error", t.error_message},
                   });
                 }
                 co_return json_response({{"dag_run_id", *dag_run_id}, {"tasks", tasks}});
               });
  }

  void start() {
    const auto& api_cfg = app_.config().api;
    auto t = server_->start(api_cfg.host, api_cfg.port);
    app_.runtime().schedule(t.take());
  }

  void stop() { server_->stop(); }

  [[nodiscard]] auto is_running() const -> bool { return server_->is_running(); }

  [[nodiscard]] auto websocket_hub() -> http::WebSocketHub& { return *ws_hub_; }
};

ApiServer::ApiServer(Application& app) : impl_(std::make_unique<Impl>(app)) {}
ApiServer::~ApiServer() = default;

void ApiServer::start() { impl_->start(); }

void ApiServer::stop() { impl_->stop(); }

auto ApiServer::is_running() const -> bool { return impl_->is_running(); }

auto ApiServer::websocket_hub() -> http::WebSocketHub& {
  return impl_->websocket_hub();
}

}  // namespace taskmaster
