#include "taskmaster/executor/executor.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/app/config_builder.hpp"

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

TEST(ExecutorTypeRegistryTest, Noop_StringToType_RoundTripsToNoop) {
  EXPECT_EQ(to_string_view(parse<ExecutorType>("noop")), "noop");
}

TEST(ExecutorTypeConversionTest, HelperFunctions_MatchRegistry) {
  EXPECT_EQ(to_string_view(ExecutorType::Shell), "shell");
  EXPECT_EQ(parse<ExecutorType>("shell"), ExecutorType::Shell);
}

TEST(ExecutorConfigBuilderTest, NoopTask_BuildsNoopExecutorConfig) {
  DAGInfo info;
  info.dag_id = DAGId("test");
  info.name = "test";
  info.created_at = std::chrono::system_clock::now();
  info.updated_at = info.created_at;

  TaskConfig task;
  task.task_id = TaskId("t1");
  task.command = "ignored";
  task.executor = ExecutorType::Noop;
  task.executor_config = NoopTaskConfig{};
  info.tasks.push_back(task);
  info.rebuild_task_index();

  DAG graph;
  graph.add_node(TaskId("t1"));

  auto cfgs = ExecutorConfigBuilder::build(info, graph);
  ASSERT_EQ(cfgs.size(), 1);
  EXPECT_TRUE(std::holds_alternative<NoopExecutorConfig>(cfgs[0]));
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
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(300));
}

TEST(ShellExecutorConfigTest, CustomValues_ArePreserved) {
  ShellExecutorConfig config;
  config.command = "echo hello";
  config.working_dir = "/tmp";
  config.execution_timeout = std::chrono::seconds(60);

  EXPECT_EQ(config.command, "echo hello");
  EXPECT_EQ(config.working_dir, "/tmp");
  EXPECT_EQ(config.execution_timeout, std::chrono::seconds(60));
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
  config.execution_timeout = std::chrono::seconds(5);

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
  config.execution_timeout = std::chrono::seconds(5);

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
  config.execution_timeout = std::chrono::seconds(5);

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
