#include "taskmaster/executor/executor.hpp"
#include "taskmaster/core/runtime.hpp"

#include <chrono>
#include <string_view>
#include <thread>

#include "gtest/gtest.h"

using namespace taskmaster;

TEST(ExecutorTypeRegistryTest, Shell_StringToType) {
  auto& registry = ExecutorTypeRegistry::instance();
  EXPECT_EQ(registry.from_string("shell"), ExecutorType::Shell);
}

TEST(ExecutorTypeRegistryTest, Shell_TypeToString) {
  auto& registry = ExecutorTypeRegistry::instance();
  EXPECT_EQ(registry.to_string(ExecutorType::Shell), "shell");
}

TEST(ExecutorTypeRegistryTest, UnknownTypeNumber_ReturnsUnknownString) {
  auto& registry = ExecutorTypeRegistry::instance();
  EXPECT_EQ(registry.to_string(static_cast<ExecutorType>(99)), "unknown");
}

TEST(ExecutorTypeRegistryTest, UnknownString_ReturnsDefaultShell) {
  auto& registry = ExecutorTypeRegistry::instance();
  EXPECT_EQ(registry.from_string("nonexistent"), ExecutorType::Shell);
}

TEST(ExecutorTypeConversionTest, HelperFunctions_MatchRegistry) {
  EXPECT_EQ(executor_type_to_string(ExecutorType::Shell), "shell");
  EXPECT_EQ(string_to_executor_type("shell"), ExecutorType::Shell);
}

TEST(ExecutorResultTest, DefaultConstruction_HasZeroValues) {
  ExecutorResult result;

  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.stdout_output.empty());
  EXPECT_TRUE(result.stderr_output.empty());
  EXPECT_TRUE(result.error.empty());
  EXPECT_FALSE(result.timed_out);
}

TEST(ShellExecutorConfigTest, DefaultConstruction_HasExpectedDefaults) {
  ShellExecutorConfig config;

  EXPECT_TRUE(config.command.empty());
  EXPECT_TRUE(config.working_dir.empty());
  EXPECT_EQ(config.timeout, std::chrono::seconds(300));
}

TEST(ShellExecutorConfigTest, CustomValues_ArePreserved) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.working_dir = "/tmp";
  config.timeout = std::chrono::seconds(60);

  EXPECT_EQ(config.command, "echo hello");
  EXPECT_EQ(config.working_dir, "/tmp");
  EXPECT_EQ(config.timeout, std::chrono::seconds(60));
}

class ShellExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    runtime_->start();
    executor_ = create_shell_executor(*runtime_);
  }

  void TearDown() override {
    executor_.reset();
    if (runtime_) {
      runtime_->stop();
      runtime_.reset();
    }
  }

  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<IExecutor> executor_;
};

TEST_F(ShellExecutorTest, ExecuteValidCommand_ReturnsZeroExitCode) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorContext ctx{*runtime_};
  ExecutorRequest req;
  req.instance_id = InstanceId("test_instance");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId&, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(ctx, req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(ShellExecutorTest, ExecuteFailingCommand_ReturnsNonZeroExitCode) {
  ShellExecutorConfig config;
  config.command = "exit 42";
  config.timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorContext ctx{*runtime_};
  ExecutorRequest req;
  req.instance_id = InstanceId("test_failing");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId&, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(ctx, req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 42);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(ShellExecutorTest, ExecuteInvalidCommand_ReturnsError) {
  ShellExecutorConfig config;
  config.command = "nonexistent_command_12345";
  config.timeout = std::chrono::seconds(5);

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorContext ctx{*runtime_};
  ExecutorRequest req;
  req.instance_id = InstanceId("test_invalid");
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId&, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(ctx, req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_NE(result.exit_code, 0);
}
