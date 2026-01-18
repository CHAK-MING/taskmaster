#pragma once

#include "taskmaster/core/runtime.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace taskmaster::http {

enum class WebSocketOpCode : uint8_t {
  Continuation = 0x0,
  Text = 0x1,
  Binary = 0x2,
  Close = 0x8,
  Ping = 0x9,
  Pong = 0xA
};

struct WebSocketFrame {
  bool fin;
  WebSocketOpCode opcode;
  bool masked;
  uint64_t payload_length;
  uint8_t mask_key[4];
  std::vector<uint8_t> payload;
};

class WebSocketConnection {
public:
  explicit WebSocketConnection(int fd, Runtime& runtime);
  ~WebSocketConnection();

  WebSocketConnection(const WebSocketConnection&) = delete;
  auto operator=(const WebSocketConnection&) -> WebSocketConnection& = delete;

  auto send_text(std::string text) -> task<void>;
  auto send_binary(std::vector<uint8_t> data) -> task<void>;
  auto send_close() -> task<void>;
  auto send_pong(std::vector<uint8_t> data) -> task<void>;

  auto handle_frames(
      std::move_only_function<void(WebSocketOpCode, std::span<const uint8_t>)> on_message)
      -> task<void>;

  [[nodiscard]] auto fd() const -> int;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

class WebSocketHub {
public:
  explicit WebSocketHub(Runtime& runtime);
  ~WebSocketHub();

  WebSocketHub(const WebSocketHub&) = delete;
  auto operator=(const WebSocketHub&) -> WebSocketHub& = delete;

  struct LogMessage {
    std::string timestamp;
    std::string dag_run_id;
    std::string task_id;
    std::string stream;
    std::string content;
  };

  struct EventMessage {
    std::string timestamp;
    std::string event;
    std::string dag_run_id;
    std::string task_id;
    std::string data;
  };

  auto add_connection(std::shared_ptr<WebSocketConnection> conn,
                      std::optional<std::string> run_filter = std::nullopt)
      -> void;
  auto remove_connection(int fd) -> void;

  auto broadcast_log(const LogMessage& msg) -> void;
  auto broadcast_event(const EventMessage& event) -> void;

  [[nodiscard]] auto connection_count() const -> size_t;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

auto perform_websocket_handshake(int client_fd, std::string sec_key)
    -> task<void>;

}  // namespace taskmaster::http
