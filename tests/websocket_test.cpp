#include "taskmaster/app/http/websocket.hpp"
#include "taskmaster/core/runtime.hpp"

#include <thread>

#include "gtest/gtest.h"
#include "test_utils.hpp"

using namespace taskmaster;
using namespace taskmaster::http;

class WebSocketTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    hub_ = std::make_unique<WebSocketHub>(*runtime_);
  }

  void TearDown() override {
    hub_.reset();
    runtime_.reset();
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<WebSocketHub> hub_;
};

TEST_F(WebSocketTest, BasicCreation) {
  EXPECT_EQ(hub_->connection_count(), 0);
}

TEST_F(WebSocketTest, ConnectionCountStartsAtZero) {
  EXPECT_EQ(hub_->connection_count(), 0);
}

TEST_F(WebSocketTest, LogMessageStructure) {
  WebSocketHub::LogMessage msg{
    .timestamp = "2024-01-01T00:00:00Z",
    .dag_run_id = "run_123",
    .task_id = "task_456",
    .stream = "stdout",
    .content = "Test log message"
  };

  EXPECT_EQ(msg.timestamp, "2024-01-01T00:00:00Z");
  EXPECT_EQ(msg.dag_run_id, "run_123");
  EXPECT_EQ(msg.task_id, "task_456");
  EXPECT_EQ(msg.stream, "stdout");
  EXPECT_EQ(msg.content, "Test log message");
}

TEST_F(WebSocketTest, EventMessageStructure) {
  WebSocketHub::EventMessage event{
    .timestamp = "2024-01-01T00:00:00Z",
    .event = "task_started",
    .dag_run_id = "run_789",
    .task_id = "task_abc",
    .data = R"({"status":"running"})"
  };

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

TEST_F(WebSocketTest, WebSocketFrameDefaultValues) {
  WebSocketFrame frame{};
  frame.fin = true;
  frame.opcode = WebSocketOpCode::Text;
  frame.masked = false;
  frame.payload_length = 0;

  EXPECT_TRUE(frame.fin);
  EXPECT_EQ(frame.opcode, WebSocketOpCode::Text);
  EXPECT_FALSE(frame.masked);
  EXPECT_EQ(frame.payload_length, 0);
}

TEST_F(WebSocketTest, RemoveNonExistentConnection) {
  EXPECT_EQ(hub_->connection_count(), 0);
  hub_->remove_connection(999);
  EXPECT_EQ(hub_->connection_count(), 0);
}
