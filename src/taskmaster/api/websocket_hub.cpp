#include "taskmaster/api/websocket_hub.hpp"

#include <nlohmann/json.hpp>

namespace taskmaster {

using json = nlohmann::json;

auto WebSocketHub::add_connection(crow::websocket::connection *conn,
                                  std::optional<std::string> run_filter)
    -> void {
  std::lock_guard lock(mu_);
  connections_.push_back({conn, std::move(run_filter)});
}

auto WebSocketHub::remove_connection(crow::websocket::connection *conn)
    -> void {
  std::lock_guard lock(mu_);
  std::erase_if(connections_,
                [conn](const Connection &c) { return c.conn == conn; });
}

auto WebSocketHub::broadcast_log(const LogMessage &msg) -> void {
  json j = {{"type", "log"},
            {"timestamp", msg.timestamp},
            {"run_id", msg.run_id},
            {"task_id", msg.task_id},
            {"stream", msg.stream},
            {"content", msg.content}};
  broadcast_json(j.dump(), msg.run_id);
}

auto WebSocketHub::broadcast_event(const EventMessage &event) -> void {
  json j = {{"type", "event"},
            {"timestamp", event.timestamp},
            {"event", event.event},
            {"run_id", event.run_id},
            {"task_id", event.task_id}};
  if (!event.data.empty()) {
    j["data"] = json::parse(event.data, nullptr, false);
  }
  broadcast_json(j.dump(), event.run_id);
}

auto WebSocketHub::connection_count() const -> size_t {
  std::lock_guard lock(mu_);
  return connections_.size();
}

auto WebSocketHub::broadcast_json(const std::string &json_str,
                                  const std::optional<std::string> &run_id)
    -> void {
  std::lock_guard lock(mu_);
  for (const auto &conn : connections_) {
    if (!conn.run_filter || !run_id || *conn.run_filter == *run_id) {
      conn.conn->send_text(json_str);
    }
  }
}

} // namespace taskmaster
