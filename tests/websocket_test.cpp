#include "dagforge/app/http/websocket.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/json.hpp"

#include <array>
#include <atomic>
#include <boost/asio/generic/stream_protocol.hpp>
#include <cstddef>
#include <future>
#include <optional>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "test_utils.hpp"
#include "gtest/gtest.h"

using namespace dagforge;
using namespace dagforge::http;

namespace {

struct ConnPair {
  int client_fd{-1};
  std::shared_ptr<WebSocketConnection> server_conn;
};

[[nodiscard]] auto make_masked_frame(WebSocketOpCode opcode,
                                     std::string_view payload)
    -> std::vector<std::byte> {
  std::vector<std::byte> frame;
  frame.reserve(2 + 4 + payload.size());

  frame.push_back(static_cast<std::byte>(0x80 | std::to_underlying(opcode)));

  std::uint64_t len = payload.size();
  if (len < 126) {
    frame.push_back(
        static_cast<std::byte>(0x80 | static_cast<std::uint8_t>(len)));
  } else if (len < 65536) {
    frame.push_back(static_cast<std::byte>(0x80 | 126));
    frame.push_back(static_cast<std::byte>((len >> 8) & 0xFF));
    frame.push_back(static_cast<std::byte>(len & 0xFF));
  } else {
    frame.push_back(static_cast<std::byte>(0x80 | 127));
    for (int i = 7; i >= 0; --i) {
      frame.push_back(static_cast<std::byte>((len >> (i * 8)) & 0xFF));
    }
  }

  std::array<std::byte, 4> mask{std::byte{0x12}, std::byte{0x34},
                                std::byte{0x56}, std::byte{0x78}};
  frame.insert(frame.end(), mask.begin(), mask.end());

  for (std::size_t i = 0; i < payload.size(); ++i) {
    frame.push_back(static_cast<std::byte>(payload[i]) ^ mask[i % 4]);
  }

  return frame;
}

[[nodiscard]] auto read_exact(int fd, std::span<std::byte> buf) -> bool {
  std::size_t off = 0;
  while (off < buf.size()) {
    auto n = ::recv(fd, buf.data() + off, buf.size() - off, 0);
    if (n <= 0) {
      return false;
    }
    off += static_cast<std::size_t>(n);
  }
  return true;
}

struct DecodedFrame {
  WebSocketOpCode opcode{WebSocketOpCode::Continuation};
  std::string payload;
};

[[nodiscard]] auto read_frame(int fd) -> std::optional<DecodedFrame> {
  std::array<std::byte, 2> hdr{};
  if (!read_exact(fd, hdr)) {
    return std::nullopt;
  }

  WebSocketOpCode opcode =
      static_cast<WebSocketOpCode>(std::to_integer<uint8_t>(hdr[0]) & 0x0F);
  bool masked = (std::to_integer<uint8_t>(hdr[1]) & 0x80) != 0;
  std::uint64_t len = (std::to_integer<uint8_t>(hdr[1]) & 0x7F);

  if (len == 126) {
    std::array<std::byte, 2> ext{};
    if (!read_exact(fd, ext)) {
      return std::nullopt;
    }
    len = (static_cast<std::uint64_t>(std::to_integer<uint8_t>(ext[0])) << 8) |
          static_cast<std::uint64_t>(std::to_integer<uint8_t>(ext[1]));
  } else if (len == 127) {
    std::array<std::byte, 8> ext{};
    if (!read_exact(fd, ext)) {
      return std::nullopt;
    }
    len = 0;
    for (int i = 0; i < 8; ++i) {
      len = (len << 8) | static_cast<std::uint64_t>(std::to_integer<uint8_t>(
                             ext[static_cast<std::size_t>(i)]));
    }
  }

  std::array<std::byte, 4> mask{};
  if (masked) {
    if (!read_exact(fd, mask)) {
      return std::nullopt;
    }
  }

  std::vector<std::byte> payload(len);
  if (len > 0 && !read_exact(fd, payload)) {
    return std::nullopt;
  }

  if (masked) {
    for (std::size_t i = 0; i < payload.size(); ++i) {
      payload[i] ^= mask[i % 4];
    }
  }

  std::string payload_str;
  payload_str.reserve(payload.size());
  for (auto b : payload) {
    payload_str += static_cast<char>(b);
  }

  return DecodedFrame{.opcode = opcode, .payload = std::move(payload_str)};
}

[[nodiscard]] auto poll_readable(int fd, std::chrono::milliseconds timeout)
    -> bool {
  pollfd pfd{.fd = fd, .events = POLLIN, .revents = 0};
  int rc = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
  return rc > 0 && (pfd.revents & POLLIN) != 0;
}

// Perform WebSocket handshake from client side (for socketpair tests)
auto perform_client_handshake(int fd) -> bool {
  // Send HTTP Upgrade request
  const std::string request = "GET /ws HTTP/1.1\r\n"
                              "Host: localhost\r\n"
                              "Upgrade: websocket\r\n"
                              "Connection: Upgrade\r\n"
                              "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                              "Sec-WebSocket-Version: 13\r\n"
                              "\r\n";

  if (::send(fd, request.data(), request.size(), 0) !=
      static_cast<ssize_t>(request.size())) {
    return false;
  }

  // Read HTTP 101 response
  std::array<char, 1024> response{};
  auto n = ::recv(fd, response.data(), response.size(), 0);
  if (n <= 0) {
    return false;
  }

  // Verify it's a 101 Switching Protocols response
  std::string response_str(response.data(), static_cast<size_t>(n));
  return response_str.find("HTTP/1.1 101") != std::string::npos;
}

} // namespace

class WebSocketTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    ASSERT_TRUE(runtime_->start().has_value());
    hub_ = std::make_unique<WebSocketHub>(*runtime_);
  }

  void TearDown() override {
    hub_.reset();
    if (runtime_) {
      runtime_->stop();
    }
    runtime_.reset();
  }

  [[nodiscard]] auto make_connection_pair() -> ConnPair {
    int fds[2]{};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
      return {};
    }

    std::promise<ConnPair> p;
    auto fut = p.get_future();

    auto task = [&]() -> spawn_task {
      boost::asio::generic::stream_protocol::socket server_socket(
          current_io_context());
      boost::system::error_code ec;
      server_socket.assign(
          boost::asio::generic::stream_protocol(AF_UNIX, SOCK_STREAM), fds[0],
          ec);
      if (ec) {
        p.set_value(ConnPair{.client_fd = -1, .server_conn = nullptr});
        co_return;
      }
      auto conn =
          std::make_shared<WebSocketConnection>(std::move(server_socket));
      p.set_value(
          ConnPair{.client_fd = fds[1], .server_conn = std::move(conn)});
      co_return;
    };

    auto t = task();
    runtime_->spawn_external(std::move(t));
    return fut.get();
  }

  auto run_on_runtime(std::move_only_function<void()> fn) -> void {
    std::promise<void> p;
    auto fut = p.get_future();
    auto t = [f = std::move(fn), &p]() mutable -> spawn_task {
      f();
      p.set_value();
      co_return;
    };
    runtime_->spawn_external(t());
    fut.get();
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<WebSocketHub> hub_;
};

TEST_F(WebSocketTest, BasicCreation) { EXPECT_EQ(hub_->connection_count(), 0); }

TEST_F(WebSocketTest, ConnectionCountStartsAtZero) {
  EXPECT_EQ(hub_->connection_count(), 0);
}

TEST_F(WebSocketTest, LogMessageStructure) {
  WebSocketHub::LogMessage msg{.timestamp = "2024-01-01T00:00:00Z",
                               .dag_run_id = "run_123",
                               .task_id = "task_456",
                               .stream = "stdout",
                               .content = "Test log message"};

  EXPECT_EQ(msg.timestamp, "2024-01-01T00:00:00Z");
  EXPECT_EQ(msg.dag_run_id, "run_123");
  EXPECT_EQ(msg.task_id, "task_456");
  EXPECT_EQ(msg.stream, "stdout");
  EXPECT_EQ(msg.content, "Test log message");
}

TEST_F(WebSocketTest, EventMessageStructure) {
  WebSocketHub::EventMessage event{.timestamp = "2024-01-01T00:00:00Z",
                                   .event = "task_started",
                                   .dag_run_id = "run_789",
                                   .task_id = "task_abc",
                                   .data = R"({"status":"running"})"};

  EXPECT_EQ(event.timestamp, "2024-01-01T00:00:00Z");
  EXPECT_EQ(event.event, "task_started");
  EXPECT_EQ(event.dag_run_id, "run_789");
  EXPECT_EQ(event.task_id, "task_abc");
  EXPECT_EQ(event.data, R"({"status":"running"})");
}

TEST_F(WebSocketTest, WebSocketOpCodeValues) {
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Continuation), 0x0);
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Text), 0x1);
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Binary), 0x2);
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Close), 0x8);
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Ping), 0x9);
  EXPECT_EQ(static_cast<uint8_t>(WebSocketOpCode::Pong), 0xA);
}

TEST_F(WebSocketTest, RemoveNonExistentConnection) {
  EXPECT_EQ(hub_->connection_count(), 0);
  run_on_runtime([this] { hub_->remove_connection(999); });
  EXPECT_EQ(hub_->connection_count(), 0);
}

TEST_F(WebSocketTest, BroadcastWithoutConnectionsIsNoOp) {
  EXPECT_EQ(hub_->connection_count(), 0U);
  EXPECT_EQ(hub_->messages_sent_total(), 0U);

  hub_->broadcast_log(WebSocketHub::LogMessage{
      .timestamp = "1",
      .dag_run_id = "run1",
      .task_id = "task1",
      .stream = "stdout",
      .content = "hello",
  });
  hub_->broadcast_event(WebSocketHub::EventMessage{
      .timestamp = "2",
      .event = "task_status_changed",
      .dag_run_id = "run1",
      .task_id = "task1",
      .data = R"({"status":"running"})",
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(hub_->connection_count(), 0U);
  EXPECT_EQ(hub_->messages_sent_total(), 0U);
}

TEST_F(WebSocketTest, HandleFrames_PingRespondsWithPong) {
  auto pair = make_connection_pair();
  ASSERT_GE(pair.client_fd, 0);
  ASSERT_TRUE(pair.server_conn != nullptr);

  std::atomic<bool> done{false};
  auto t = [&]() -> spawn_task {
    co_await pair.server_conn->handle_frames(
        [](WebSocketOpCode, std::span<const std::byte>) {});
    done = true;
  };
  runtime_->spawn_external(t());

  // Perform WebSocket handshake before sending frames
  ASSERT_TRUE(perform_client_handshake(pair.client_fd));

  auto ping = make_masked_frame(WebSocketOpCode::Ping, "hi");
  ASSERT_EQ(::send(pair.client_fd, ping.data(), ping.size(), 0),
            static_cast<ssize_t>(ping.size()));

  ASSERT_TRUE(poll_readable(pair.client_fd, std::chrono::milliseconds(500)));
  auto pong = read_frame(pair.client_fd);
  ASSERT_TRUE(pong.has_value());
  EXPECT_EQ(pong->opcode, WebSocketOpCode::Pong);
  EXPECT_EQ(pong->payload, "hi");

  auto close = make_masked_frame(WebSocketOpCode::Close, "");
  ASSERT_EQ(::send(pair.client_fd, close.data(), close.size(), 0),
            static_cast<ssize_t>(close.size()));

  ASSERT_TRUE(poll_readable(pair.client_fd, std::chrono::milliseconds(500)));
  auto close_resp = read_frame(pair.client_fd);
  ASSERT_TRUE(close_resp.has_value());
  EXPECT_EQ(close_resp->opcode, WebSocketOpCode::Close);

  // Shutdown write to signal EOF
  ::shutdown(pair.client_fd, SHUT_WR);

  ASSERT_TRUE(test::poll_until([&] { return done.load(); },
                               std::chrono::milliseconds(2000),
                               std::chrono::milliseconds(10)));
  ::close(pair.client_fd);
}

TEST_F(WebSocketTest, HandleFrames_UnmaskedFrameClosesConnection) {
  auto pair = make_connection_pair();
  ASSERT_GE(pair.client_fd, 0);
  ASSERT_TRUE(pair.server_conn != nullptr);

  std::atomic<bool> done{false};
  auto t = [&]() -> spawn_task {
    co_await pair.server_conn->handle_frames(
        [](WebSocketOpCode, std::span<const std::byte>) {
          FAIL() << "Callback should not be invoked for protocol error";
        });
    done = true;
  };
  runtime_->spawn_external(t());

  // Send unmasked frame (Protocol Error)
  std::vector<std::byte> frame;
  frame.push_back(std::byte{0x81}); // Fin, Text
  frame.push_back(std::byte{0x05}); // Len 5, Unmasked (0x80 bit 0)
  std::string payload = "hello";
  for (char c : payload) {
    frame.push_back(static_cast<std::byte>(c));
  }

  ASSERT_EQ(::send(pair.client_fd, frame.data(), frame.size(), 0),
            static_cast<ssize_t>(frame.size()));

  // Expect handle_frames to exit (connection closed)
  ASSERT_TRUE(test::poll_until([&] { return done.load(); },
                               std::chrono::milliseconds(2000),
                               std::chrono::milliseconds(10)));

  ::close(pair.client_fd);
}

TEST_F(WebSocketTest, HandleFrames_TextInvokesCallback) {
  auto pair = make_connection_pair();
  ASSERT_GE(pair.client_fd, 0);
  ASSERT_TRUE(pair.server_conn != nullptr);

  std::atomic<bool> got_msg{false};
  std::string received;

  std::atomic<bool> done{false};
  auto t = [&]() -> spawn_task {
    co_await pair.server_conn->handle_frames(
        [&](WebSocketOpCode opcode, std::span<const std::byte> payload) {
          if (opcode == WebSocketOpCode::Text) {
            received.assign(reinterpret_cast<const char *>(payload.data()),
                            payload.size());
            got_msg = true;
          }
        });
    done = true;
  };
  runtime_->spawn_external(t());

  // Perform WebSocket handshake before sending frames
  ASSERT_TRUE(perform_client_handshake(pair.client_fd));

  auto text = make_masked_frame(WebSocketOpCode::Text, "hello");
  ASSERT_EQ(::send(pair.client_fd, text.data(), text.size(), 0),
            static_cast<ssize_t>(text.size()));

  ASSERT_TRUE(test::poll_until([&] { return got_msg.load(); },
                               std::chrono::milliseconds(2000),
                               std::chrono::milliseconds(10)));
  EXPECT_EQ(received, "hello");

  auto close = make_masked_frame(WebSocketOpCode::Close, "");
  ASSERT_EQ(::send(pair.client_fd, close.data(), close.size(), 0),
            static_cast<ssize_t>(close.size()));
  ASSERT_TRUE(poll_readable(pair.client_fd, std::chrono::milliseconds(500)));
  auto close_resp = read_frame(pair.client_fd);
  ASSERT_TRUE(close_resp.has_value());
  EXPECT_EQ(close_resp->opcode, WebSocketOpCode::Close);

  // Shutdown write to signal EOF
  ::shutdown(pair.client_fd, SHUT_WR);

  ASSERT_TRUE(test::poll_until([&] { return done.load(); },
                               std::chrono::milliseconds(2000),
                               std::chrono::milliseconds(10)));
  ::close(pair.client_fd);
}

TEST_F(WebSocketTest, HubBroadcastLog_SendsJson) {
  auto pair = make_connection_pair();
  ASSERT_GE(pair.client_fd, 0);
  ASSERT_TRUE(pair.server_conn != nullptr);

  // Start handle_frames to accept the connection
  auto t = [conn = pair.server_conn]() -> spawn_task {
    co_await conn->handle_frames(
        [](WebSocketOpCode, std::span<const std::byte>) {});
  };
  runtime_->spawn_external(t());

  // Perform WebSocket handshake
  ASSERT_TRUE(perform_client_handshake(pair.client_fd));

  run_on_runtime(
      [this, conn = pair.server_conn] { hub_->add_connection(conn); });

  WebSocketHub::LogMessage msg{.timestamp = "1",
                               .dag_run_id = "run1",
                               .task_id = "t1",
                               .stream = "stdout",
                               .content = "x"};
  hub_->broadcast_log(msg);

  ASSERT_TRUE(poll_readable(pair.client_fd, std::chrono::milliseconds(2000)));
  auto frame = read_frame(pair.client_fd);
  ASSERT_TRUE(frame.has_value());
  ASSERT_EQ(frame->opcode, WebSocketOpCode::Text);

  auto parsed = parse_json(frame->payload);
  ASSERT_TRUE(parsed.has_value());
  auto &j = *parsed;
  ASSERT_TRUE(j["type"].is_string());
  EXPECT_EQ(j["type"].as<std::string>(), "log");
  EXPECT_EQ(j["timestamp"].as<std::string>(), "1");
  EXPECT_EQ(j["dag_run_id"].as<std::string>(), "run1");
  EXPECT_EQ(j["task_id"].as<std::string>(), "t1");
  EXPECT_EQ(j["stream"].as<std::string>(), "stdout");
  EXPECT_EQ(j["content"].as<std::string>(), "x");

  ::close(pair.client_fd);
}

TEST_F(WebSocketTest, HubBroadcast_RespectsRunFilter) {
  auto pair = make_connection_pair();
  ASSERT_GE(pair.client_fd, 0);
  ASSERT_TRUE(pair.server_conn != nullptr);

  // Start handle_frames to accept the connection
  auto t = [conn = pair.server_conn]() -> spawn_task {
    co_await conn->handle_frames(
        [](WebSocketOpCode, std::span<const std::byte>) {});
  };
  runtime_->spawn_external(t());

  // Perform WebSocket handshake
  ASSERT_TRUE(perform_client_handshake(pair.client_fd));

  run_on_runtime(
      [this, conn = pair.server_conn] { hub_->add_connection(conn, "run1"); });

  WebSocketHub::LogMessage msg_other{.timestamp = "1",
                                     .dag_run_id = "run2",
                                     .task_id = "t1",
                                     .stream = "stdout",
                                     .content = "x"};
  hub_->broadcast_log(msg_other);
  EXPECT_FALSE(poll_readable(pair.client_fd, std::chrono::milliseconds(200)));

  WebSocketHub::LogMessage msg_ok{.timestamp = "1",
                                  .dag_run_id = "run1",
                                  .task_id = "t1",
                                  .stream = "stdout",
                                  .content = "x"};
  hub_->broadcast_log(msg_ok);
  ASSERT_TRUE(poll_readable(pair.client_fd, std::chrono::milliseconds(2000)));
  auto frame = read_frame(pair.client_fd);
  ASSERT_TRUE(frame.has_value());
  EXPECT_EQ(frame->opcode, WebSocketOpCode::Text);

  ::close(pair.client_fd);
}

TEST_F(WebSocketTest, HubStress_RepeatedConnectAndCloseAllWhileBroadcasting) {
  constexpr int kCycles = 20;
  constexpr int kConnectionsPerCycle = 3;

  std::atomic<bool> stop_broadcast{false};
  std::thread broadcaster([this, &stop_broadcast] {
    int seq = 0;
    while (!stop_broadcast.load(std::memory_order_acquire)) {
      hub_->broadcast_log(WebSocketHub::LogMessage{
          .timestamp = std::to_string(seq),
          .dag_run_id = "stress_run",
          .task_id = "stress_task",
          .stream = "stdout",
          .content = "msg-" + std::to_string(seq),
      });
      hub_->broadcast_event(WebSocketHub::EventMessage{
          .timestamp = std::to_string(seq),
          .event = "stress_event",
          .dag_run_id = "stress_run",
          .task_id = "stress_task",
          .data = R"({"status":"running"})",
      });
      ++seq;
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  for (int cycle = 0; cycle < kCycles; ++cycle) {
    std::vector<ConnPair> pairs;
    pairs.reserve(kConnectionsPerCycle);

    for (int i = 0; i < kConnectionsPerCycle; ++i) {
      auto pair = make_connection_pair();
      ASSERT_GE(pair.client_fd, 0);
      ASSERT_TRUE(pair.server_conn != nullptr);

      auto t = [conn = pair.server_conn]() -> spawn_task {
        co_await conn->handle_frames(
            [](WebSocketOpCode, std::span<const std::byte>) {});
      };
      runtime_->spawn_external(t());

      ASSERT_TRUE(perform_client_handshake(pair.client_fd));
      run_on_runtime([this, conn = pair.server_conn] { hub_->add_connection(conn); });
      pairs.push_back(std::move(pair));
    }

    ASSERT_TRUE(test::poll_until(
        [this] { return hub_->connection_count() >= kConnectionsPerCycle; },
        std::chrono::milliseconds(2000), std::chrono::milliseconds(10)));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    hub_->close_all();

    ASSERT_TRUE(test::poll_until(
        [this] { return hub_->connection_count() == 0; },
        std::chrono::milliseconds(2000), std::chrono::milliseconds(10)));

    for (auto &pair : pairs) {
      ASSERT_TRUE(test::poll_until(
          [&pair] { return pair.server_conn->is_closed(); },
          std::chrono::milliseconds(2000), std::chrono::milliseconds(10)));
      ::shutdown(pair.client_fd, SHUT_RDWR);
      ::close(pair.client_fd);
    }
  }

  stop_broadcast.store(true, std::memory_order_release);
  broadcaster.join();
}
