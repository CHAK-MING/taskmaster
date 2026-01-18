#include "taskmaster/app/http/websocket.hpp"

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>

#include <openssl/evp.h>
#include <openssl/sha.h>

#include <array>
#include <cstring>
#include <format>
#include <mutex>
#include <vector>

namespace taskmaster::http {

using json = nlohmann::json;

namespace {

auto base64_encode(std::span<const uint8_t> data) -> std::string {
  static constexpr char kBase64Chars[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  std::string result;
  result.reserve(((data.size() + 2) / 3) * 4);

  for (size_t i = 0; i < data.size(); i += 3) {
    uint32_t triple = static_cast<uint32_t>(data[i]) << 16;
    if (i + 1 < data.size())
      triple |= static_cast<uint32_t>(data[i + 1]) << 8;
    if (i + 2 < data.size())
      triple |= static_cast<uint32_t>(data[i + 2]);

    result += kBase64Chars[(triple >> 18) & 0x3F];
    result += kBase64Chars[(triple >> 12) & 0x3F];
    result += (i + 1 < data.size()) ? kBase64Chars[(triple >> 6) & 0x3F] : '=';
    result += (i + 2 < data.size()) ? kBase64Chars[triple & 0x3F] : '=';
  }

  return result;
}

auto create_websocket_accept_key(std::string_view sec_key) -> std::string {
  static constexpr auto kMagicGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  std::string combined = std::string(sec_key) + kMagicGUID;

  std::array<uint8_t, SHA_DIGEST_LENGTH> hash{};
  SHA1(reinterpret_cast<const uint8_t*>(combined.data()), combined.size(),
       hash.data());

  return base64_encode(hash);
}

auto encode_websocket_frame(WebSocketOpCode opcode,
                             std::span<const uint8_t> payload, bool fin = true)
    -> std::vector<uint8_t> {
  std::vector<uint8_t> frame;

  frame.push_back((fin ? 0x80 : 0x00) | static_cast<uint8_t>(opcode));

  if (payload.size() < 126) {
    frame.push_back(static_cast<uint8_t>(payload.size()));
  } else if (payload.size() < 65536) {
    frame.push_back(126);
    frame.push_back((payload.size() >> 8) & 0xFF);
    frame.push_back(payload.size() & 0xFF);
  } else {
    frame.push_back(127);
    for (int i = 7; i >= 0; --i) {
      frame.push_back((payload.size() >> (i * 8)) & 0xFF);
    }
  }

  frame.insert(frame.end(), payload.begin(), payload.end());
  return frame;
}

auto parse_websocket_frame(std::span<const uint8_t> data)
    -> std::optional<WebSocketFrame> {
  if (data.size() < 2) {
    return std::nullopt;
  }

  WebSocketFrame frame;
  frame.fin = (data[0] & 0x80) != 0;
  frame.opcode = static_cast<WebSocketOpCode>(data[0] & 0x0F);
  frame.masked = (data[1] & 0x80) != 0;

  size_t pos = 2;
  uint64_t payload_len = data[1] & 0x7F;

  if (payload_len == 126) {
    if (data.size() < 4)
      return std::nullopt;
    payload_len = (static_cast<uint64_t>(data[2]) << 8) | data[3];
    pos = 4;
  } else if (payload_len == 127) {
    if (data.size() < 10)
      return std::nullopt;
    payload_len = 0;
    for (int i = 0; i < 8; ++i) {
      payload_len = (payload_len << 8) | data[2 + i];
    }
    pos = 10;
  }

  frame.payload_length = payload_len;

  if (frame.masked) {
    if (data.size() < pos + 4)
      return std::nullopt;
    std::memcpy(frame.mask_key, data.data() + pos, 4);
    pos += 4;
  }

  if (data.size() < pos + payload_len) {
    return std::nullopt;
  }

  frame.payload.assign(data.begin() + pos, data.begin() + pos + payload_len);

  if (frame.masked) {
    for (size_t i = 0; i < frame.payload.size(); ++i) {
      frame.payload[i] ^= frame.mask_key[i % 4];
    }
  }

  return frame;
}

}  // namespace

struct WebSocketConnection::Impl {
  int fd;
  Runtime& runtime;

  Impl(int fd_, Runtime& rt) : fd(fd_), runtime(rt) {
  }
};

WebSocketConnection::WebSocketConnection(int fd, Runtime& runtime)
    : impl_(std::make_unique<Impl>(fd, runtime)) {
}

WebSocketConnection::~WebSocketConnection() = default;

auto WebSocketConnection::send_text(std::string text) -> task<void> {
  auto& io_ctx = current_io_context();
  auto frame = encode_websocket_frame(
      WebSocketOpCode::Text,
      std::span{reinterpret_cast<const uint8_t*>(text.data()), text.size()});

  auto result = co_await io_ctx.async_write(impl_->fd, io::buffer(frame));
  if (!result) {
    log::debug("WebSocket send_text failed");
  }
}

auto WebSocketConnection::send_binary(std::vector<uint8_t> data)
    -> task<void> {
  auto& io_ctx = current_io_context();
  auto frame = encode_websocket_frame(WebSocketOpCode::Binary, data);

  auto result = co_await io_ctx.async_write(impl_->fd, io::buffer(frame));
  if (!result) {
    log::debug("WebSocket send_binary failed");
  }
}

auto WebSocketConnection::send_close() -> task<void> {
  auto& io_ctx = current_io_context();
  auto frame = encode_websocket_frame(WebSocketOpCode::Close, {});

  auto result = co_await io_ctx.async_write(impl_->fd, io::buffer(frame));
  if (!result) {
    log::debug("WebSocket send_close failed");
  }
}

auto WebSocketConnection::send_pong(std::vector<uint8_t> data)
    -> task<void> {
  auto& io_ctx = current_io_context();
  auto frame = encode_websocket_frame(WebSocketOpCode::Pong, data);

  auto result = co_await io_ctx.async_write(impl_->fd, io::buffer(frame));
  if (!result) {
    log::debug("WebSocket send_pong failed");
  }
}

auto WebSocketConnection::handle_frames(
    std::move_only_function<void(WebSocketOpCode, std::span<const uint8_t>)> on_message)
    -> task<void> {
  auto& io_ctx = current_io_context();
  std::vector<uint8_t> buffer(8192);

  while (true) {
    auto read_result =
        co_await io_ctx.async_read(impl_->fd, io::buffer(buffer));

    if (!read_result || read_result.bytes_transferred == 0) {
      break;
    }

    auto frame_opt = parse_websocket_frame(
        std::span{buffer.data(), read_result.bytes_transferred});
    if (!frame_opt) {
      log::debug("Failed to parse WebSocket frame");
      continue;
    }

    auto& frame = *frame_opt;

    switch (frame.opcode) {
      case WebSocketOpCode::Text:
      case WebSocketOpCode::Binary:
        on_message(frame.opcode, frame.payload);
        break;

      case WebSocketOpCode::Ping:
        co_await send_pong(frame.payload);
        break;

      case WebSocketOpCode::Close:
        co_await send_close();
        co_return;

      default:
        break;
    }
  }
}

auto WebSocketConnection::fd() const -> int {
  return impl_->fd;
}

struct WebSocketHub::Impl {
  struct Connection {
    std::shared_ptr<WebSocketConnection> conn;
    std::optional<std::string> run_filter;
  };

  Runtime& runtime;
  std::vector<Connection> connections;
  mutable std::mutex mu;

  explicit Impl(Runtime& rt) : runtime(rt) {
  }

  auto broadcast_json(const std::string& json_str,
                      const std::optional<std::string>& dag_run_id) -> void {
    std::scoped_lock lock(mu);
    for (auto& c : connections) {
      if (!c.run_filter || !dag_run_id || *c.run_filter == *dag_run_id) {
        auto send_task = [](std::shared_ptr<WebSocketConnection> conn,
                            std::string msg) -> task<void> {
          co_await conn->send_text(msg);
        };
        runtime.schedule(send_task(c.conn, json_str).take());
      }
    }
  }
};

WebSocketHub::WebSocketHub(Runtime& runtime)
    : impl_(std::make_unique<Impl>(runtime)) {
}

WebSocketHub::~WebSocketHub() = default;

auto WebSocketHub::add_connection(std::shared_ptr<WebSocketConnection> conn,
                                   std::optional<std::string> run_filter)
    -> void {
  std::scoped_lock lock(impl_->mu);
  impl_->connections.push_back({std::move(conn), std::move(run_filter)});
}

auto WebSocketHub::remove_connection(int fd) -> void {
  std::scoped_lock lock(impl_->mu);
  std::erase_if(impl_->connections,
                [fd](const Impl::Connection& c) { return c.conn->fd() == fd; });
}

auto WebSocketHub::broadcast_log(const LogMessage& msg) -> void {
  json j = {{"type", "log"},        {"timestamp", msg.timestamp},
            {"dag_run_id", msg.dag_run_id}, {"task_id", msg.task_id},
            {"stream", msg.stream}, {"content", msg.content}};
  impl_->broadcast_json(j.dump(), msg.dag_run_id);
}

auto WebSocketHub::broadcast_event(const EventMessage& event) -> void {
  json j = {{"type", "event"},          {"timestamp", event.timestamp},
            {"event", event.event},     {"dag_run_id", event.dag_run_id},
            {"task_id", event.task_id}, {"data", event.data}};
  impl_->broadcast_json(j.dump(), event.dag_run_id);
}

auto WebSocketHub::connection_count() const -> size_t {
  std::scoped_lock lock(impl_->mu);
  return impl_->connections.size();
}

auto perform_websocket_handshake(int client_fd, std::string sec_key)
    -> task<void> {
  auto& io_ctx = current_io_context();

  auto accept_key = create_websocket_accept_key(sec_key);

  std::string response = std::format(
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Accept: {}\r\n"
      "\r\n",
      accept_key);

  std::vector<uint8_t> response_data(response.begin(), response.end());
  auto result = co_await io_ctx.async_write(client_fd, io::buffer(response_data));

  if (!result) {
    log::error("WebSocket handshake write failed");
  }
}

}  // namespace taskmaster::http
