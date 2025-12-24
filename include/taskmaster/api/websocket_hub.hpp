#pragma once

#include <memory>
#include <optional>
#include <string>

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
  WebSocketHub();
  ~WebSocketHub();

  WebSocketHub(const WebSocketHub &) = delete;
  auto operator=(const WebSocketHub &) -> WebSocketHub & = delete;
  WebSocketHub(WebSocketHub &&) noexcept;
  auto operator=(WebSocketHub &&) noexcept -> WebSocketHub &;

  // Opaque connection handle - actual type is crow::websocket::connection*
  using ConnectionHandle = void *;

  auto add_connection(ConnectionHandle conn,
                      std::optional<std::string> run_filter = std::nullopt)
      -> void;
  auto remove_connection(ConnectionHandle conn) -> void;

  auto broadcast_log(const LogMessage &msg) -> void;
  auto broadcast_event(const EventMessage &event) -> void;

  [[nodiscard]] auto connection_count() const -> size_t;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace taskmaster
