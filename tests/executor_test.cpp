#include "dagforge/app/config_builder.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/executor/executor.hpp"

#include <chrono>
#include <future>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace dagforge;

TEST(ExecutorTypeRegistryTest, Shell_StringToType) {
  EXPECT_EQ(parse<ExecutorType>("shell"), ExecutorType::Shell);
}

TEST(ExecutorTypeRegistryTest, Shell_TypeToString) {
  EXPECT_EQ(to_string_view(ExecutorType::Shell), "shell");
}

TEST(ExecutorTypeRegistryTest, UnknownTypeNumber_ReturnsUnknownString) {
  EXPECT_EQ(to_string_view(static_cast<ExecutorType>(99)), "unknown");
}

TEST(ExecutorTypeRegistryTest, UnknownString_ReturnsDefaultShell) {
  EXPECT_EQ(parse<ExecutorType>("nonexistent"), ExecutorType::Shell);
}

TEST(ExecutorTypeRegistryTest, ParseNormalizesCaseAndSeparators) {
  EXPECT_EQ(parse<ExecutorType>("DOCKER"), ExecutorType::Docker);
  EXPECT_EQ(parse<ExecutorType>("do-ck_er"), ExecutorType::Docker);
  EXPECT_EQ(dagforge::util::normalize_enum_token("Do-Ck_Er!"),
            "docker");
}

TEST(ExecutorTypeConversionTest, HelperFunctions_MatchRegistry) {
  EXPECT_EQ(to_string_view(ExecutorType::Shell), "shell");
  EXPECT_EQ(parse<ExecutorType>("shell"), ExecutorType::Shell);
}

TEST(ExecutorRegistryTest, PersistedDockerConfigRoundTripsThroughRegistry) {
  DockerExecutorConfig docker;
  docker.image = "alpine:latest";
  docker.docker_socket = "/tmp/docker.sock";
  docker.pull_policy = ImagePullPolicy::Always;

  const auto json =
      ExecutorRegistry::instance().serialize_config(ExecutorConfig{docker});
  const auto reparsed = ExecutorRegistry::instance().parse_persisted_config(
      ExecutorType::Docker, json);

  ASSERT_TRUE(reparsed);
  const auto *cfg = reparsed->as<DockerExecutorConfig>();
  ASSERT_NE(cfg, nullptr);
  EXPECT_EQ(cfg->image, "alpine:latest");
  EXPECT_EQ(cfg->docker_socket, "/tmp/docker.sock");
  EXPECT_EQ(cfg->pull_policy, ImagePullPolicy::Always);
}

TEST(ExecutorRegistryTest, ValidateTaskUsesExecutorSpecificRules) {
  TaskConfig task{};
  task.task_id = TaskId{"docker_task"};
  task.executor = ExecutorType::Docker;
  task.executor_config = DockerExecutorConfig{};

  std::vector<std::string> errors;
  ExecutorRegistry::instance().validate_task(task, errors);

  ASSERT_EQ(errors.size(), 1U);
  EXPECT_NE(errors.front().find("docker image cannot be empty"),
            std::string::npos);
}

TEST(ExecutorRegistryTest, ParsePersistedDockerConfigRejectsMalformedJson) {
  auto reparsed = ExecutorRegistry::instance().parse_persisted_config(
      ExecutorType::Docker, "{not-json");

  ASSERT_FALSE(reparsed);
  EXPECT_EQ(reparsed.error(), make_error_code(Error::ParseError));
}

TEST(ExecutorRegistryTest, ParsePersistedSensorConfigRejectsMalformedJson) {
  auto reparsed = ExecutorRegistry::instance().parse_persisted_config(
      ExecutorType::Sensor, "[");

  ASSERT_FALSE(reparsed);
  EXPECT_EQ(reparsed.error(), make_error_code(Error::ParseError));
}

TEST(ExecutorRegistryTest, PersistedSensorConfigRoundTripsThroughRegistry) {
  SensorExecutorConfig sensor;
  sensor.type = SensorType::Http;
  sensor.target = "http://127.0.0.1:8080/health";
  sensor.poke_interval = std::chrono::seconds(7);
  sensor.soft_fail = true;
  sensor.expected_status = 204;
  sensor.http_method = "POST";

  const auto json =
      ExecutorRegistry::instance().serialize_config(ExecutorConfig{sensor});
  const auto reparsed = ExecutorRegistry::instance().parse_persisted_config(
      ExecutorType::Sensor, json);

  ASSERT_TRUE(reparsed);
  const auto *cfg = reparsed->as<SensorExecutorConfig>();
  ASSERT_NE(cfg, nullptr);
  EXPECT_EQ(cfg->type, SensorType::Http);
  EXPECT_EQ(cfg->target, "http://127.0.0.1:8080/health");
  EXPECT_EQ(cfg->poke_interval, std::chrono::seconds(7));
  EXPECT_TRUE(cfg->soft_fail);
  EXPECT_EQ(cfg->expected_status, 204);
  EXPECT_EQ(cfg->http_method, "POST");
}

TEST(ExecutorRegistryTest, ValidateSensorTaskRejectsCommandAndMissingTarget) {
  TaskConfig task{};
  task.task_id = TaskId{"sensor_task"};
  task.executor = ExecutorType::Sensor;
  task.command = "echo should_not_exist";
  task.executor_config =
      SensorExecutorConfig{.type = SensorType::File, .target = ""};

  std::vector<std::string> errors;
  ExecutorRegistry::instance().validate_task(task, errors);

  ASSERT_EQ(errors.size(), 2U);
  EXPECT_NE(errors[0].find("command is not allowed for sensor tasks"),
            std::string::npos);
  EXPECT_NE(errors[1].find("sensor target cannot be empty"),
            std::string::npos);
}

TEST(ExecutorConfigBuilderTest, BuildsConfigsInGraphNodeOrder) {
  DAGInfo info;
  info.dag_id = DAGId{"builder_test_dag"};
  info.name = "builder_test_dag";

  TaskConfig shell_task;
  shell_task.task_id = TaskId{"shell_task"};
  shell_task.name = "shell_task";
  shell_task.command = "echo ok";
  shell_task.executor = ExecutorType::Shell;

  TaskConfig sensor_task;
  sensor_task.task_id = TaskId{"sensor_task"};
  sensor_task.name = "sensor_task";
  sensor_task.executor = ExecutorType::Sensor;
  sensor_task.executor_config =
      SensorExecutorConfig{.type = SensorType::File, .target = "/tmp/probe"};

  info.tasks = {shell_task, sensor_task};

  DAG graph;
  auto sensor_idx = graph.add_node(sensor_task.task_id);
  auto shell_idx = graph.add_node(shell_task.task_id);
  ASSERT_TRUE(sensor_idx.has_value());
  ASSERT_TRUE(shell_idx.has_value());

  auto configs = ExecutorConfigBuilder::build(info, graph);
  ASSERT_EQ(configs.size(), 2U);
  EXPECT_EQ(configs[*sensor_idx].type(), ExecutorType::Sensor);
  EXPECT_EQ(configs[*shell_idx].type(), ExecutorType::Shell);
}

namespace {
class CountingExecutor final : public IExecutor {
public:
  auto start(ExecutorRequest, ExecutionSink) -> Result<void> override {
    return ok();
  }
  auto cancel(const InstanceId &) -> void override { ++cancel_count_; }

  int cancel_count_{0};
};

auto run_shell_affinity_probe(
    Runtime &runtime, IExecutor &executor,
    std::shared_ptr<std::promise<shard_id>> completion_shard_promise)
    -> spawn_task {
  ExecutionSink sink;
  sink.on_complete = [&runtime, completion_shard_promise](const InstanceId &,
                                                          ExecutorResult) mutable {
    completion_shard_promise->set_value(runtime.current_shard());
  };

  auto start = executor.start(
      ExecutorRequest{
          .instance_id = InstanceId{"affinity_instance"},
          .command = "echo hello",
          .execution_timeout = std::chrono::seconds(5),
          .config = ShellExecutorConfig{.env = {}},
          .memory_resource = {}},
      std::move(sink));
  EXPECT_TRUE(start.has_value());
  co_return;
}
} // namespace

TEST(CompositeExecutorTest, CancelWithoutMappingBroadcastsToAllExecutors) {
  CompositeExecutor composite;

  auto shell = std::make_unique<CountingExecutor>();
  auto docker = std::make_unique<CountingExecutor>();
  auto *shell_raw = shell.get();
  auto *docker_raw = docker.get();

  composite.register_executor(ExecutorType::Shell, std::move(shell));
  composite.register_executor(ExecutorType::Docker, std::move(docker));

  composite.cancel(InstanceId{"recovered_run_task"});

  EXPECT_EQ(shell_raw->cancel_count_, 1);
  EXPECT_EQ(docker_raw->cancel_count_, 1);
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

  EXPECT_TRUE(config.env.empty());
}

TEST(ShellExecutorConfigTest, CustomValues_ArePreserved) {
  ShellExecutorConfig config;
  config.env["KEY"] = "VALUE";

  EXPECT_EQ(config.env["KEY"], "VALUE");
}

class ShellExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    ASSERT_TRUE(runtime_->start().has_value());
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

class NoopExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>(1);
    ASSERT_TRUE(runtime_->start().has_value());
    executor_ = create_noop_executor(*runtime_);
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

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_instance");
  req.command = "echo hello";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

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

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_failing");
  req.command = "exit 42";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

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

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_invalid");
  req.command = "nonexistent_command_12345";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_NE(result.exit_code, 0);
}

TEST_F(ShellExecutorTest, ExecuteTimedOutCommand_ReturnsTimeoutResult) {
  ShellExecutorConfig config;

  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("test_timeout");
  req.command = "sleep 2";
  req.execution_timeout = std::chrono::seconds(1);
  req.config = config;

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, kExitCodeTimeout);
  EXPECT_TRUE(result.timed_out);
  EXPECT_NE(result.error.find("Execution timeout"), std::string::npos);
}

TEST_F(ShellExecutorTest, StreamsStdoutLineByLineBeforeCompletion) {
  ShellExecutorConfig config;

  ExecutorResult result;
  std::atomic<bool> completed{false};
  std::vector<std::string> streamed_lines;

  ExecutorRequest req;
  req.instance_id = InstanceId("test_stream_lines");
  req.command = "printf 'line1\\nline2\\nline3\\n'";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = config;

  ExecutionSink sink;
  sink.on_stdout = [&](const InstanceId &, std::string_view data) {
    streamed_lines.emplace_back(data);
  };
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  executor_->start(req, std::move(sink));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.stdout_streamed);
  ASSERT_EQ(streamed_lines.size(), 3U);
  EXPECT_EQ(streamed_lines[0], "line1");
  EXPECT_EQ(streamed_lines[1], "line2");
  EXPECT_EQ(streamed_lines[2], "line3");
  EXPECT_EQ(std::string(result.stdout_output), "line1\nline2\nline3\n");
}

TEST_F(ShellExecutorTest, ExecuteWithMaliciousEnvKey_ReturnsError) {
  ShellExecutorConfig config;
  config.env["VALID_KEY"] = "value";
  config.env["MALICIOUS_KEY; rm -rf /"] = "value";

  ExecutorRequest req;
  req.instance_id = InstanceId("test_malicious");
  req.command = "echo hello";
  req.execution_timeout = std::chrono::seconds(5);
  req.config = config;

  ExecutionSink sink;

  auto result = executor_->start(req, std::move(sink));
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error(), Error::InvalidArgument);
}

TEST_F(NoopExecutorTest, CompletesImmediatelyWithConfiguredExitCode) {
  ExecutorResult result;
  std::atomic<bool> completed{false};

  ExecutorRequest req;
  req.instance_id = InstanceId("noop_ok");
  req.config = NoopExecutorConfig{.exit_code = 17};

  ExecutionSink sink;
  sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
    result = std::move(r);
    completed = true;
  };

  ASSERT_TRUE(executor_->start(req, std::move(sink)).has_value());

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!completed && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(completed);
  EXPECT_EQ(result.exit_code, 17);
  EXPECT_FALSE(result.timed_out);
}

TEST_F(NoopExecutorTest, RejectsMismatchedExecutorConfigType) {
  ExecutorRequest req;
  req.instance_id = InstanceId("noop_bad");
  req.config = ShellExecutorConfig{};

  auto start_res = executor_->start(req, ExecutionSink{});
  ASSERT_FALSE(start_res.has_value());
  EXPECT_EQ(start_res.error(), make_error_code(Error::InvalidArgument));
}

TEST(ShellExecutorAffinityTest, ExecutorRunsOnCallingShard) {
  Runtime runtime(4);
  ASSERT_TRUE(runtime.start().has_value());

  auto executor = create_shell_executor(runtime);

  auto completion_shard_promise = std::make_shared<std::promise<shard_id>>();
  auto completion_shard = completion_shard_promise->get_future();
  runtime.spawn_on(shard_id{1}, run_shell_affinity_probe(
                                    runtime, *executor,
                                    std::move(completion_shard_promise)));

  ASSERT_EQ(completion_shard.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  EXPECT_EQ(completion_shard.get(), shard_id{1});

  executor.reset();
  runtime.stop();
}
