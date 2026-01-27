#include "taskmaster/client/http/http_client.hpp"
#include "taskmaster/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

namespace taskmaster::http::test {

class HttpClientTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>();
    runtime_->start();
  }

  void TearDown() override {
    if (runtime_) {
      runtime_->stop();
    }
  }

  std::unique_ptr<Runtime> runtime_;
};

TEST_F(HttpClientTest, ConnectUnixFailsForNonExistentSocket) {
  std::atomic<bool> completed{false};
  std::unique_ptr<HttpClient> result_client;

  auto test_task = [&]() -> spawn_task {
    auto client = co_await HttpClient::connect_unix(
        current_io_context(), "/tmp/nonexistent_socket_12345.sock");

    result_client = std::move(client);
    completed = true;
  };

  auto t = test_task();
  runtime_->schedule_external(t.take());

  taskmaster::test::busy_wait_for(std::chrono::milliseconds(100));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_EQ(result_client, nullptr);
}

TEST_F(HttpClientTest, HttpClientConfigDefaults) {
  HttpClientConfig config;

  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.max_response_size, 10 * 1024 * 1024);
  EXPECT_TRUE(config.keep_alive);
}

TEST_F(HttpClientTest, HttpClientConfigCustomValues) {
  HttpClientConfig config{
      .connect_timeout = std::chrono::milliseconds(5000),
      .read_timeout = std::chrono::milliseconds(10000),
      .max_response_size = 1024,
      .keep_alive = false,
  };

  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(5000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(10000));
  EXPECT_EQ(config.max_response_size, 1024u);
  EXPECT_FALSE(config.keep_alive);
}

}  // namespace taskmaster::http::test
