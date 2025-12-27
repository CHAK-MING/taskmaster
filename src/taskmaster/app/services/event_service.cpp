#include "taskmaster/app/services/event_service.hpp"

#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/util/util.hpp"

#include <nlohmann/json.hpp>

#include <chrono>

namespace taskmaster {

auto EventService::set_api_server(ApiServer* api) -> void {
  api_ = api;
}

auto EventService::emit_task_status(std::string_view run_id,
                                    std::string_view task,
                                    std::string_view status) -> void {
  if (!api_)
    return;

  std::string rid(run_id);
  std::string dag_id = rid.substr(0, rid.rfind('_'));

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  nlohmann::json j = {{"type", "task_status_changed"},
                      {"dag_id", dag_id},
                      {"run_id", rid},
                      {"task_id", std::string(task)},
                      {"status", std::string(status)},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = std::to_string(now);
  ev.event = "task_status_changed";
  ev.run_id = rid;
  ev.task_id = std::string(task);
  ev.data = j.dump();

  api_->hub().broadcast_event(ev);
}

auto EventService::emit_run_status(std::string_view run_id,
                                   std::string_view status) -> void {
  if (!api_)
    return;

  std::string rid(run_id);
  std::string dag_id = rid.substr(0, rid.rfind('_'));

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  nlohmann::json j = {{"type", "dag_run_completed"},
                      {"dag_id", dag_id},
                      {"run_id", rid},
                      {"status", std::string(status)},
                      {"timestamp", now}};

  EventMessage ev;
  ev.timestamp = std::to_string(now);
  ev.event = "dag_run_completed";
  ev.run_id = rid;
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
