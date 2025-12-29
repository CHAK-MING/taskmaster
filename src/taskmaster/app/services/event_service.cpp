#include "taskmaster/app/services/event_service.hpp"

#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/util/util.hpp"

#include <nlohmann/json.hpp>

#include <charconv>
#include <chrono>

namespace taskmaster {

namespace {
// Extract dag_id from run_id (format: "dag_id_timestamp")
// Returns a view into the original string, no allocation
auto extract_dag_id(std::string_view run_id) -> std::string_view {
  auto pos = run_id.rfind('_');
  return pos != std::string_view::npos ? run_id.substr(0, pos) : run_id;
}

// Format timestamp to string using stack buffer
auto format_timestamp_ms(std::int64_t ms) -> std::string {
  std::array<char, 24> buf;
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), ms);
  return std::string(buf.data(), ptr);
}
}  // namespace

auto EventService::set_api_server(ApiServer* api) -> void {
  api_ = api;
}

auto EventService::emit_task_status(std::string_view run_id,
                                    std::string_view task,
                                    std::string_view status) -> void {
  if (!api_)
    return;

  auto dag_id = extract_dag_id(run_id);
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  // Build JSON with string_view where possible
  nlohmann::json j = {{"type", "task_status_changed"},
                      {"dag_id", dag_id},
                      {"run_id", run_id},
                      {"task_id", task},
                      {"status", status},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = format_timestamp_ms(now);
  ev.event = "task_status_changed";
  ev.run_id = std::string(run_id);
  ev.task_id = std::string(task);
  ev.data = j.dump();

  api_->hub().broadcast_event(ev);
}

auto EventService::emit_run_status(std::string_view run_id,
                                   std::string_view status) -> void {
  if (!api_)
    return;

  auto dag_id = extract_dag_id(run_id);
  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  nlohmann::json j = {{"type", "dag_run_completed"},
                      {"dag_id", dag_id},
                      {"run_id", run_id},
                      {"status", status},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = format_timestamp_ms(now);
  ev.event = "dag_run_completed";
  ev.run_id = std::string(run_id);
  ev.data = j.dump();

  api_->hub().broadcast_event(ev);
}

auto EventService::emit_log(std::string_view run_id, std::string_view task,
                            std::string_view stream, std::string msg) -> void {
  if (!api_)
    return;

  api_->hub().broadcast_log({format_timestamp(), std::string(run_id),
                             std::string(task), std::string(stream),
                             std::move(msg)});
}

}  // namespace taskmaster
