#include "dagforge/http/http_client.hpp"
#include "dagforge/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>

namespace dagforge::http::test {
namespace {

auto connect_unix_after_yield(std::shared_ptr<std::atomic_bool> completed,
                              std::shared_ptr<std::atomic_bool> failed)
    -> spawn_task {
  auto connect_op = HttpClient::connect_unix(
      current_io_context(), std::string{"/tmp/nonexistent_deferred.sock"});
  co_await async_yield();
  auto client_res = co_await std::move(connect_op);
  failed->store(!client_res.has_value(), std::memory_order_release);
  completed->store(true, std::memory_order_release);
}

} // namespace

class HttpClientTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>();
    ASSERT_TRUE(runtime_->start().has_value());
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
    auto client_res = co_await HttpClient::connect_unix(
        current_io_context(), "/tmp/nonexistent_socket_12345.sock");

    if (client_res) {
      result_client = std::move(*client_res);
    }
    completed = true;
  };

  auto t = test_task();
  runtime_->spawn_external(std::move(t));

  dagforge::test::busy_wait_for(std::chrono::milliseconds(100));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_EQ(result_client, nullptr);
}

TEST_F(HttpClientTest, ConnectUnixOwnsSocketPathAcrossDeferredStart) {
  auto completed = std::make_shared<std::atomic_bool>(false);
  auto failed = std::make_shared<std::atomic_bool>(false);
  runtime_->spawn_external(connect_unix_after_yield(completed, failed));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(completed->load(std::memory_order_acquire));
  EXPECT_TRUE(failed->load(std::memory_order_acquire));
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
  EXPECT_EQ(config.max_response_size, 1024U);
  EXPECT_FALSE(config.keep_alive);
}

} // namespace dagforge::http::test
