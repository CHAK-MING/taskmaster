#include "dagforge/client/docker/docker_client.hpp"
#include "dagforge/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <utility>

namespace dagforge::docker::test {

using TestDockerClient = DockerClient<http::HttpClient>;

namespace {

auto connect_docker_after_yield(std::shared_ptr<std::atomic_bool> completed,
                                std::shared_ptr<std::atomic_bool> failed)
    -> spawn_task {
  auto connect_op = TestDockerClient::connect(
      current_io_context(),
      std::string{"/tmp/nonexistent_deferred_docker.sock"});
  co_await async_yield();
  auto result = co_await std::move(connect_op);
  failed->store(!result.has_value(), std::memory_order_release);
  completed->store(true, std::memory_order_release);
}

} // namespace

class DockerClientTest : public ::testing::Test {
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

TEST_F(DockerClientTest, ConnectFailsForNonExistentSocket) {
  std::atomic<bool> completed{false};
  Result<std::unique_ptr<TestDockerClient>> result;

  auto test_task = [&]() -> spawn_task {
    result = co_await TestDockerClient::connect(
        current_io_context(), "/tmp/nonexistent_docker_socket_12345.sock");

    completed = true;
    co_return;
  };

  auto t = test_task();
  runtime_->spawn_external(std::move(t));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(DockerError::ConnectionFailed));
}

TEST_F(DockerClientTest, ConnectOwnsSocketPathAcrossDeferredStart) {
  auto completed = std::make_shared<std::atomic_bool>(false);
  auto failed = std::make_shared<std::atomic_bool>(false);
  runtime_->spawn_external(connect_docker_after_yield(completed, failed));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(completed->load(std::memory_order_acquire));
  EXPECT_TRUE(failed->load(std::memory_order_acquire));
}

TEST_F(DockerClientTest, DockerClientConfigDefaults) {
  DockerClientConfig config;

  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(300000));
  EXPECT_EQ(config.api_version, "v1.43");
}

TEST_F(DockerClientTest, DockerClientConfigCustomValues) {
  DockerClientConfig config{
      .connect_timeout = std::chrono::milliseconds(5000),
      .read_timeout = std::chrono::milliseconds(60000),
      .api_version = "v1.44",
  };

  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(5000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(60000));
  EXPECT_EQ(config.api_version, "v1.44");
}

TEST_F(DockerClientTest, ContainerConfigConstruction) {
  ContainerConfig config{
      .image = "alpine:latest",
      .command = "echo hello",
      .working_dir = "/app",
      .env = {{"KEY1", "value1"}, {"KEY2", "value2"}},
  };

  EXPECT_EQ(config.image, "alpine:latest");
  EXPECT_EQ(config.command, "echo hello");
  EXPECT_EQ(config.working_dir, "/app");
  EXPECT_EQ(config.env.size(), 2U);
  EXPECT_EQ(config.env["KEY1"], "value1");
  EXPECT_EQ(config.env["KEY2"], "value2");
}

TEST_F(DockerClientTest, DockerErrorConvertsToErrorCode) {
  const auto ec = make_error_code(DockerError::Timeout);

  EXPECT_EQ(ec.category(), docker_error_category());
  EXPECT_EQ(ec.message(), "timeout");
  EXPECT_EQ(ec, std::errc::timed_out);
}

} // namespace dagforge::docker::test
