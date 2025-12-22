#pragma once

#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <crow.h>

namespace taskmaster {

struct LogMessage {
  std::string timestamp;
  std::string run_id;
  std::string task_id;
  std::string stream;
  std::string content;
};

struct EventMessage {
  std::string timestamp;
  std::string event;
  std::string run_id;
  std::string task_id;
  std::string data;
};

class WebSocketHub {
public:
  auto add_connection(crow::websocket::connection *conn,
                      std::optional<std::string> run_filter = std::nullopt)
      -> void;
  auto remove_connection(crow::websocket::connection *conn) -> void;

  auto broadcast_log(const LogMessage &msg) -> void;
  auto broadcast_event(const EventMessage &event) -> void;

  [[nodiscard]] auto connection_count() const -> size_t;

private:
  struct Connection {
    crow::websocket::connection *conn;
    std::optional<std::string> run_filter;
  };

  auto broadcast_json(const std::string &json,
                      const std::optional<std::string> &run_id) -> void;

  std::vector<Connection> connections_;
  mutable std::mutex mu_;
};

} // namespace taskmaster
