#include "taskmaster/client/docker/docker_client.hpp"
#include "taskmaster/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

namespace taskmaster::docker::test {

class DockerClientTest : public ::testing::Test {
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

TEST_F(DockerClientTest, ConnectFailsForNonExistentSocket) {
  std::atomic<bool> completed{false};
  DockerResult<std::unique_ptr<DockerClient>> result;

  auto test_task = [&]() -> spawn_task {
    result = co_await DockerClient::connect(
        current_io_context(), "/tmp/nonexistent_docker_socket_12345.sock");

    completed = true;
  };

  auto t = test_task();
  runtime_->schedule_external(t.take());

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), DockerError::ConnectionFailed);
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
  EXPECT_EQ(config.env.size(), 2u);
  EXPECT_EQ(config.env["KEY1"], "value1");
  EXPECT_EQ(config.env["KEY2"], "value2");
}

}  // namespace taskmaster::docker::test
