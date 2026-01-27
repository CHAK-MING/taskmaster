#include "taskmaster/app/services/event_service.hpp"

#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/util/util.hpp"

#include <nlohmann/json.hpp>

#include <charconv>
#include <chrono>

namespace taskmaster {

using EventMessage = http::WebSocketHub::EventMessage;

namespace {
auto format_timestamp_ms(std::int64_t ms) -> std::string {
  std::array<char, 24> buf;
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), ms);
  return std::string(buf.data(), ptr);
}
}  // namespace

auto EventService::set_api_server(ApiServer* api) -> void {
  api_ = api;
}

auto EventService::emit_task_status(const DAGRunId& dag_run_id, const TaskId& task,
                                    std::string_view status) -> void {
  if (!api_)
    return;

  auto dag_id = extract_dag_id(dag_run_id);
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  nlohmann::json j = {{"type", "task_status_changed"},
                      {"dag_id", dag_id.value()},
                      {"dag_run_id", dag_run_id.value()},
                      {"task_id", task.value()},
                      {"status", status},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = format_timestamp_ms(now);
  ev.event = "task_status_changed";
  ev.dag_run_id = std::string(dag_run_id.value());
  ev.task_id = std::string(task.value());
  ev.data = j.dump();

  api_->websocket_hub().broadcast_event(ev);
}

auto EventService::emit_run_status(const DAGRunId& dag_run_id,
                                   std::string_view status) -> void {
  if (!api_)
    return;

  auto dag_id = extract_dag_id(dag_run_id);
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  nlohmann::json j = {{"type", "dag_run_completed"},
                      {"dag_id", dag_id.value()},
                      {"dag_run_id", dag_run_id.value()},
                      {"status", status},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = format_timestamp_ms(now);
  ev.event = "dag_run_completed";
  ev.dag_run_id = std::string(dag_run_id.value());
  ev.data = j.dump();

  api_->websocket_hub().broadcast_event(ev);
}

auto EventService::emit_log(const DAGRunId& dag_run_id, const TaskId& task,
                            std::string_view stream, std::string msg) -> void {
  if (!api_)
    return;

  api_->websocket_hub().broadcast_log({format_timestamp(), std::string(dag_run_id.value()),
                             std::string(task.value()), std::string(stream),
                             std::move(msg)});
}

}  // namespace taskmaster
