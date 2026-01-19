#include "taskmaster/executor/docker_executor.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/core/runtime.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>

namespace taskmaster::test {

class DockerExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>();
    runtime_->start();
    executor_ = create_docker_executor(*runtime_);
  }

  void TearDown() override {
    executor_.reset();
    if (runtime_) {
      runtime_->stop();
    }
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<IExecutor> executor_;
};

TEST_F(DockerExecutorTest, DockerExecutorConfigDefaults) {
  DockerExecutorConfig config;

  EXPECT_TRUE(config.image.empty());
  EXPECT_TRUE(config.command.empty());
  EXPECT_TRUE(config.working_dir.empty());
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(300));
  EXPECT_TRUE(config.env.empty());
  EXPECT_EQ(config.docker_socket, "/var/run/docker.sock");
}

TEST_F(DockerExecutorTest, DockerExecutorConfigCustomValues) {
  DockerExecutorConfig config{
      .image = "alpine:latest",
      .command = "echo hello",
      .working_dir = "/app",
      .execution_timeout = std::chrono::seconds(60),
      .env = {{"KEY", "value"}},
      .docker_socket = "/custom/docker.sock",
  };

  EXPECT_EQ(config.image, "alpine:latest");
  EXPECT_EQ(config.command, "echo hello");
  EXPECT_EQ(config.working_dir, "/app");
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(60));
  EXPECT_EQ(config.env.size(), 1u);
  EXPECT_EQ(config.env["KEY"], "value");
  EXPECT_EQ(config.docker_socket, "/custom/docker.sock");
}

TEST_F(DockerExecutorTest, ExecutorHandlesInvalidConfig) {
  std::atomic<bool> completed{false};
  ExecutorResult final_result;

  ShellExecutorConfig wrong_config;
  wrong_config.command = "echo test";

  ExecutorRequest req{
      .instance_id = InstanceId{"test-instance-1"},
      .config = wrong_config,
  };

  ExecutionSink sink{
      .on_state = nullptr,
      .on_stdout = nullptr,
      .on_stderr = nullptr,
      .on_complete = [&](InstanceId, ExecutorResult result) {
        final_result = std::move(result);
        completed = true;
      },
  };

  ExecutorContext ctx{.runtime = *runtime_};
  executor_->start(ctx, std::move(req), std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_EQ(final_result.exit_code, -1);
  EXPECT_FALSE(final_result.error.empty());
}

TEST_F(DockerExecutorTest, ExecutorFailsWithNonExistentSocket) {
  std::atomic<bool> completed{false};
  ExecutorResult final_result;

  DockerExecutorConfig config{
      .image = "alpine:latest",
      .command = "echo hello",
      .working_dir = "",
      .execution_timeout = std::chrono::seconds(300),
      .env = {},
      .docker_socket = "/tmp/nonexistent_docker_socket_test.sock",
  };

  ExecutorRequest req{
      .instance_id = InstanceId{"test-instance-2"},
      .config = config,
  };

  ExecutionSink sink{
      .on_state = nullptr,
      .on_stdout = nullptr,
      .on_stderr = nullptr,
      .on_complete = [&](InstanceId, ExecutorResult result) {
        final_result = std::move(result);
        completed = true;
      },
  };

  ExecutorContext ctx{.runtime = *runtime_};
  executor_->start(ctx, std::move(req), std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_EQ(final_result.exit_code, -1);
  EXPECT_FALSE(final_result.error.empty());
}

TEST_F(DockerExecutorTest, ExecutorTypeRegistration) {
  auto& registry = ExecutorTypeRegistry::instance();
  
  auto type = registry.from_string("docker");
  EXPECT_EQ(type, ExecutorType::Docker);
  
  auto str = registry.to_string(ExecutorType::Docker);
  EXPECT_EQ(str, "docker");
}

}  // namespace taskmaster::test
