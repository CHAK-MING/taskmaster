#include "taskmaster/api/websocket_hub.hpp"

#include <mutex>
#include <vector>

#include <crow.h>
#include <nlohmann/json.hpp>

namespace taskmaster {

using json = nlohmann::json;

struct WebSocketHub::Impl {
  struct Connection {
    crow::websocket::connection *conn;
    std::optional<std::string> run_filter;
  };

  std::vector<Connection> connections;
  mutable std::mutex mu;

  auto broadcast_json(const std::string &json_str,
                      const std::optional<std::string> &run_id) -> void {
    std::lock_guard lock(mu);
    for (auto &c : connections) {
      if (!c.run_filter || !run_id || *c.run_filter == *run_id) {
        c.conn->send_text(json_str);
      }
    }
  }
};

WebSocketHub::WebSocketHub() : impl_(std::make_unique<Impl>()) {}

WebSocketHub::~WebSocketHub() = default;

WebSocketHub::WebSocketHub(WebSocketHub &&) noexcept = default;

auto WebSocketHub::operator=(WebSocketHub &&) noexcept -> WebSocketHub & = default;

auto WebSocketHub::add_connection(ConnectionHandle conn,
                                  std::optional<std::string> run_filter)
    -> void {
  auto *crow_conn = static_cast<crow::websocket::connection *>(conn);
  std::lock_guard lock(impl_->mu);
  impl_->connections.push_back({crow_conn, std::move(run_filter)});
}

auto WebSocketHub::remove_connection(ConnectionHandle conn) -> void {
  auto *crow_conn = static_cast<crow::websocket::connection *>(conn);
  std::lock_guard lock(impl_->mu);
  std::erase_if(impl_->connections,
                [crow_conn](const Impl::Connection &c) { return c.conn == crow_conn; });
}

auto WebSocketHub::broadcast_log(const LogMessage &msg) -> void {
  json j = {{"type", "log"},
            {"timestamp", msg.timestamp},
            {"run_id", msg.run_id},
            {"task_id", msg.task_id},
            {"stream", msg.stream},
            {"content", msg.content}};
  impl_->broadcast_json(j.dump(), msg.run_id);
}

auto WebSocketHub::broadcast_event(const EventMessage &event) -> void {
  json j = {{"type", "event"},
            {"timestamp", event.timestamp},
            {"event", event.event},
            {"run_id", event.run_id},
            {"task_id", event.task_id},
            {"data", event.data}};
  impl_->broadcast_json(j.dump(), event.run_id);
}

auto WebSocketHub::connection_count() const -> size_t {
  std::lock_guard lock(impl_->mu);
  return impl_->connections.size();
}

} // namespace taskmaster
