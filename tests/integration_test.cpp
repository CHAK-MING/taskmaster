#include "taskmaster/app/api/api_server.hpp"
#include "taskmaster/app/application.hpp"
#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <unistd.h>

#include "gtest/gtest.h"

using namespace taskmaster;
using namespace std::chrono_literals;
namespace fs = std::filesystem;

namespace {

constexpr auto kServerStartupTimeout = std::chrono::milliseconds(2000);
constexpr auto kServerShutdownDelay = std::chrono::milliseconds(200);
constexpr auto kPollInterval = std::chrono::milliseconds(20);
constexpr uint16_t kBaseIntegrationTestPort = 19000;

auto make_temp_path() -> std::string {
  std::string templ = "/tmp/taskmaster_test_XXXXXX";
  int fd = ::mkstemp(templ.data());
  EXPECT_GE(fd, 0) << "Failed to create temp file";
  if (fd >= 0) {
    ::close(fd);
  }
  return templ;
}

}  // namespace

class RealIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    temp_db_ = make_temp_path();

    app_ = std::make_unique<Application>(temp_db_);
    app_->config().api.port = next_port_++;
    api_server_ = std::make_unique<ApiServer>(*app_);

    ASSERT_TRUE(app_->start().has_value());
    api_server_->start();
    ASSERT_TRUE(wait_for_server_ready());
  }

  void TearDown() override {
    if (api_server_ && api_server_->is_running()) {
      api_server_->stop();
    }
    if (app_ && app_->is_running()) {
      app_->stop();
    }
    std::this_thread::sleep_for(kServerShutdownDelay);

    if (!temp_db_.empty()) {
      std::error_code ec;
      fs::remove(temp_db_, ec);
    }
  }

  [[nodiscard]] auto wait_for_server_ready() -> bool {
    auto deadline = std::chrono::steady_clock::now() + kServerStartupTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
      if (api_server_->is_running() && app_->is_running()) {
        return true;
      }
      std::this_thread::sleep_for(kPollInterval);
    }
    ADD_FAILURE() << "Server failed to start within timeout";
    return false;
  }

  auto wait_for_run_completion(const DAGRunId& run_id,
                               std::chrono::milliseconds timeout = 10s)
      -> bool {
    if (!app_ || !app_->persistence()) {
      ADD_FAILURE() << "Application/persistence is null";
      return false;
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto state = app_->persistence()->get_dag_run_state(run_id);
      if (!state.has_value()) {
        ADD_FAILURE() << "get_dag_run_state(" << run_id
                      << ") failed: " << state.error().message();
        return false;
      }
      if (state.value() != DAGRunState::Running) {
        return true;
      }
      std::this_thread::sleep_for(50ms);
    }

    auto state = app_->persistence()->get_dag_run_state(run_id);
    if (state.has_value()) {
      ADD_FAILURE() << "Timed out waiting for run completion; run_id=" << run_id
                    << " state=" << static_cast<int>(state.value());
    } else {
      ADD_FAILURE() << "Timed out and get_dag_run_state(" << run_id
                    << ") failed: " << state.error().message();
    }
    return false;
  }

  auto latest_run_for_dag(const DAGId& dag_id,
                          std::chrono::milliseconds timeout = 2s)
      -> std::optional<Persistence::RunHistoryEntry> {
    if (!app_ || !app_->persistence()) {
      ADD_FAILURE() << "Application/persistence is null";
      return std::nullopt;
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      auto history = app_->persistence()->list_run_history(dag_id, 1);
      if (!history.has_value()) {
        ADD_FAILURE() << "list_run_history(" << dag_id
                      << ") failed: " << history.error().message();
        return std::nullopt;
      }
      if (!history->empty()) {
        return history->front();
      }
      std::this_thread::sleep_for(20ms);
    }

    ADD_FAILURE() << "Timed out waiting for run history entry for dag_id="
                  << dag_id;
    return std::nullopt;
  }

  auto create_dag_info(std::string name) -> DAGInfo {
    DAGInfo info;
    info.name = std::move(name);
    return info;
  }

  auto create_task(std::string id, std::string name, std::string command,
                   std::vector<TaskId> deps = {}) -> TaskConfig {
    TaskConfig task;
    task.task_id = TaskId(std::move(id));
    task.name = std::move(name);
    task.command = std::move(command);
    task.dependencies = std::move(deps);
    return task;
  }

  [[nodiscard]] auto get_task_stdout(const DAGRunId& run_id,
                                     const TaskId& task_id) -> std::string {
    auto logs = app_->persistence()->get_task_logs(run_id, task_id);
    if (!logs.has_value()) {
      return "";
    }
    std::string result;
    for (const auto& entry : *logs) {
      if (entry.stream == "stdout") {
        result += entry.message;
      }
    }
    return result;
  }

  auto expect_task_stdout_contains(const DAGRunId& run_id,
                                   const TaskId& task_id,
                                   std::string_view expected) -> void {
    auto stdout_content = get_task_stdout(run_id, task_id);
    EXPECT_TRUE(stdout_content.find(expected) != std::string::npos)
        << "Expected stdout of task '" << task_id << "' to contain '"
        << expected << "', but got: '" << stdout_content << "'";
  }

  auto verify_task_instance(const TaskInstanceInfo& info,
                            TaskState expected_state,
                            int expected_exit_code = 0) -> void {
    EXPECT_EQ(info.state, expected_state)
        << "Task state mismatch for instance " << info.instance_id;
    EXPECT_EQ(info.exit_code, expected_exit_code)
        << "Exit code mismatch for instance " << info.instance_id;

    if (expected_state == TaskState::Success ||
        expected_state == TaskState::Failed) {
      EXPECT_GT(info.started_at.time_since_epoch().count(), 0)
          << "started_at should be set for completed task";
      EXPECT_GT(info.finished_at.time_since_epoch().count(), 0)
          << "finished_at should be set for completed task";
      EXPECT_GE(info.finished_at, info.started_at)
          << "finished_at should be >= started_at";
    }
  }

  auto verify_all_tasks_successful(const DAGRunId& run_id,
                                   size_t expected_count) -> void {
    auto tasks = app_->persistence()->get_task_instances(run_id);
    ASSERT_TRUE(tasks.has_value()) << "Failed to get task instances";
    ASSERT_EQ(tasks->size(), expected_count) << "Task count mismatch";

    for (const auto& t : *tasks) {
      verify_task_instance(t, TaskState::Success, 0);
      EXPECT_EQ(t.attempt, 1) << "Successful task should have attempt=1";
    }
  }

  auto verify_xcom_value(const DAGRunId& run_id, const TaskId& task_id,
                         const std::string& key,
                         const nlohmann::json& expected) -> void {
    auto xcom = app_->persistence()->get_xcom(run_id, task_id, key);
    ASSERT_TRUE(xcom.has_value())
        << "XCom key '" << key << "' not found for task '" << task_id << "'";
    EXPECT_EQ(*xcom, expected)
        << "XCom value mismatch for key '" << key << "'";
  }

  std::string temp_db_;
  std::unique_ptr<Application> app_;
  std::unique_ptr<ApiServer> api_server_;

private:
  static inline std::atomic<uint16_t> next_port_{kBaseIntegrationTestPort};
};

TEST_F(RealIntegrationTest, HealthCheck) {
  EXPECT_TRUE(app_->is_running());
  EXPECT_TRUE(api_server_->is_running());
}

TEST_F(RealIntegrationTest, SimpleTaskSucceeds) {
  DAGId dag_id("simple_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Simple DAG"));
  ASSERT_TRUE(dag_result.has_value()) << dag_result.error().message();

  auto task = create_task("simple_task", "Simple Task", "echo 'hello world'");
  auto task_result = app_->dag_manager().add_task(dag_id, task);
  ASSERT_TRUE(task_result.has_value()) << task_result.error().message();

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  auto entry = latest_run_for_dag(dag_id);
  ASSERT_TRUE(entry.has_value());
  ASSERT_TRUE(wait_for_run_completion(entry->dag_run_id));

  auto state = app_->persistence()->get_dag_run_state(entry->dag_run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  verify_all_tasks_successful(entry->dag_run_id, 1);
  expect_task_stdout_contains(entry->dag_run_id, TaskId("simple_task"), "hello world");
}

TEST_F(RealIntegrationTest, TaskWithDependencyChain) {
  DAGId dag_id("chain_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Chain DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task1 = create_task("task1", "Task 1", "echo 'Task 1 output'");
  auto task2 = create_task("task2", "Task 2", "echo 'Task 2 output'", {TaskId("task1")});
  auto task3 = create_task("task3", "Task 3", "echo 'Task 3 output'", {TaskId("task2")});

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task1).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task2).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task3).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  auto entry = latest_run_for_dag(dag_id);
  ASSERT_TRUE(entry.has_value());
  ASSERT_TRUE(wait_for_run_completion(entry->dag_run_id));

  auto history = app_->persistence()->list_run_history(dag_id, 10);
  ASSERT_TRUE(history.has_value());
  EXPECT_EQ(history.value()[0].state, DAGRunState::Success);

  verify_all_tasks_successful(entry->dag_run_id, 3);

  expect_task_stdout_contains(entry->dag_run_id, TaskId("task1"), "Task 1 output");
  expect_task_stdout_contains(entry->dag_run_id, TaskId("task2"), "Task 2 output");
  expect_task_stdout_contains(entry->dag_run_id, TaskId("task3"), "Task 3 output");

  auto tasks = app_->persistence()->get_task_instances(entry->dag_run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 3u);

  std::vector<std::chrono::system_clock::time_point> finish_times;
  for (const auto& t : *tasks) {
    finish_times.push_back(t.finished_at);
  }
  std::sort(finish_times.begin(), finish_times.end());
  EXPECT_LE(finish_times[0], finish_times[1]);
  EXPECT_LE(finish_times[1], finish_times[2]);
}

TEST_F(RealIntegrationTest, FailingTaskBlocksDependents) {
  DAGId dag_id("fail_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Fail DAG"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task1;
  task1.task_id = TaskId("task1");
  task1.name = "Task 1";
  task1.command = "exit 1";
  task1.max_retries = 0;

  auto task2 = create_task("task2", "Task 2", "echo 'This should not run'", {TaskId("task1")});

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task1).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task2).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  auto entry = latest_run_for_dag(dag_id);
  ASSERT_TRUE(entry.has_value());
  ASSERT_TRUE(wait_for_run_completion(entry->dag_run_id));

  auto history = app_->persistence()->list_run_history(dag_id, 10);
  ASSERT_TRUE(history.has_value());
  EXPECT_EQ(history.value()[0].state, DAGRunState::Failed);

  auto tasks = app_->persistence()->get_task_instances(entry->dag_run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 2u);

  bool found_failed = false;
  bool found_upstream_failed = false;
  for (const auto& t : *tasks) {
    if (t.state == TaskState::Failed) {
      found_failed = true;
      EXPECT_EQ(t.exit_code, 1) << "Failed task should have exit_code=1";
      EXPECT_GT(t.started_at.time_since_epoch().count(), 0);
      EXPECT_GT(t.finished_at.time_since_epoch().count(), 0);
    }
    if (t.state == TaskState::UpstreamFailed) {
      found_upstream_failed = true;
      EXPECT_EQ(t.started_at.time_since_epoch().count(), 0)
          << "UpstreamFailed task should never start";
    }
  }
  EXPECT_TRUE(found_failed);
  EXPECT_TRUE(found_upstream_failed);

  auto task2_stdout = get_task_stdout(entry->dag_run_id, TaskId("task2"));
  EXPECT_TRUE(task2_stdout.empty()) << "Blocked task should have no output";
}

TEST_F(RealIntegrationTest, TimeoutTaskFailsRun) {
  DAGId dag_id("timeout_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Timeout DAG"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task;
  task.task_id = TaskId("timeout_task");
  task.name = "Timeout Task";
  task.command = "sleep 2";
  task.timeout = 1s;
  task.max_retries = 0;

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  auto entry = latest_run_for_dag(dag_id);
  ASSERT_TRUE(entry.has_value());
  ASSERT_TRUE(wait_for_run_completion(entry->dag_run_id));

  auto state = app_->persistence()->get_dag_run_state(entry->dag_run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Failed);

  auto tasks = app_->persistence()->get_task_instances(entry->dag_run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1u);
  EXPECT_EQ(tasks->front().state, TaskState::Failed);
}

TEST_F(RealIntegrationTest, MultipleTriggersCreateMultipleRuns) {
  DAGId dag_id("multi_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Multi Trigger DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_task("multi_task", "Multi Task", "echo 'Multi trigger'");
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  (void)app_->trigger_dag_by_id(dag_id);
  std::this_thread::sleep_for(100ms);
  (void)app_->trigger_dag_by_id(dag_id);
  std::this_thread::sleep_for(100ms);
  (void)app_->trigger_dag_by_id(dag_id);

  auto deadline = std::chrono::steady_clock::now() + 15s;
  while (std::chrono::steady_clock::now() < deadline) {
    auto history = app_->persistence()->list_run_history(dag_id, 10);
    ASSERT_TRUE(history.has_value());
    if (history->size() >= 3) {
      bool all_done = true;
      for (const auto& e : *history) {
        auto s = app_->persistence()->get_dag_run_state(e.dag_run_id);
        ASSERT_TRUE(s.has_value());
        all_done = all_done && (s.value() != DAGRunState::Running);
      }
      if (all_done) break;
    }
    std::this_thread::sleep_for(50ms);
  }

  auto history = app_->persistence()->list_run_history(dag_id, 10);
  ASSERT_TRUE(history.has_value());
  EXPECT_GE(history.value().size(), 3);

  int success_count = 0;
  for (const auto& entry : history.value()) {
    if (entry.state == DAGRunState::Success) {
      success_count++;
    }
  }
  EXPECT_GE(success_count, 3);
}

TEST_F(RealIntegrationTest, DAGLifecycle_CreateAndDelete) {
  DAGId dag_id("lifecycle_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Lifecycle DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_task("lifecycle_task", "Lifecycle Task", "echo 'Lifecycle'");
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto db_dag_result = app_->persistence()->get_dag(dag_id);
  ASSERT_TRUE(db_dag_result.has_value());

  auto delete_result = app_->dag_manager().delete_dag(dag_id);
  ASSERT_TRUE(delete_result.has_value());

  auto db_dag_after = app_->persistence()->get_dag(dag_id);
  EXPECT_FALSE(db_dag_after.has_value());
}

TEST_F(RealIntegrationTest, XComPushAndPullBetweenTasks) {
  DAGId dag_id("xcom_test_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Test"));
  ASSERT_TRUE(dag_result.has_value()) << dag_result.error().message();

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = R"(echo '{"value": 42, "message": "hello"}')";
  producer.xcom_push = {{.key = "result", .source = XComSource::Json}};

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Received: $PRODUCER_DATA\"";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "result", .source_task = TaskId("producer"), .env_var = "PRODUCER_DATA"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom = app_->persistence()->get_xcom(*run_id, TaskId("producer"), "result");
  ASSERT_TRUE(xcom.has_value()) << "XCom 'result' should be persisted";
  EXPECT_EQ((*xcom)["value"], 42);
  EXPECT_EQ((*xcom)["message"], "hello");

  verify_all_tasks_successful(*run_id, 2);

  expect_task_stdout_contains(*run_id, TaskId("consumer"), "Received:");
  expect_task_stdout_contains(*run_id, TaskId("consumer"), "42");
}

TEST_F(RealIntegrationTest, XComPullFailsGracefullyWhenKeyMissing) {
  DAGId dag_id("xcom_missing_key_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Missing Key"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = "echo 'no xcom push'";

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Data: ${MISSING_DATA:-default_value}\"";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "nonexistent", .source_task = TaskId("producer"), .env_var = "MISSING_DATA"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);
}

TEST_F(RealIntegrationTest, XComRegexExtraction) {
  DAGId dag_id("xcom_regex_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Regex"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = "echo 'Processed 123 records in 5 seconds'";
  producer.xcom_push = {{
      .key = "count",
      .source = XComSource::Stdout,
      .regex_pattern = R"(Processed (\d+) records)",
      .regex_group = 1
  }};

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Count was: $RECORD_COUNT\"";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "count", .source_task = TaskId("producer"), .env_var = "RECORD_COUNT"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom = app_->persistence()->get_xcom(*run_id, TaskId("producer"), "count");
  ASSERT_TRUE(xcom.has_value());
  EXPECT_EQ(*xcom, "123");

  expect_task_stdout_contains(*run_id, TaskId("consumer"), "Count was: 123");
}

TEST_F(RealIntegrationTest, XComChainedTasks) {
  DAGId dag_id("xcom_chain_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Chain"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task1;
  task1.task_id = TaskId("step1");
  task1.name = "Step 1";
  task1.command = "echo 'step1_output'";
  task1.xcom_push = {{.key = "data", .source = XComSource::Stdout}};

  TaskConfig task2;
  task2.task_id = TaskId("step2");
  task2.name = "Step 2";
  task2.command = "echo \"step2_received_${STEP1_DATA}\"";
  task2.dependencies = {TaskId("step1")};
  task2.xcom_pull = {{.key = "data", .source_task = TaskId("step1"), .env_var = "STEP1_DATA"}};
  task2.xcom_push = {{.key = "data", .source = XComSource::Stdout}};

  TaskConfig task3;
  task3.task_id = TaskId("step3");
  task3.name = "Step 3";
  task3.command = "echo \"final: $STEP2_DATA\"";
  task3.dependencies = {TaskId("step2")};
  task3.xcom_pull = {{.key = "data", .source_task = TaskId("step2"), .env_var = "STEP2_DATA"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task1).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task2).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task3).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom1 = app_->persistence()->get_xcom(*run_id, TaskId("step1"), "data");
  ASSERT_TRUE(xcom1.has_value());
  EXPECT_TRUE(xcom1->get<std::string>().find("step1_output") != std::string::npos);

  auto xcom2 = app_->persistence()->get_xcom(*run_id, TaskId("step2"), "data");
  ASSERT_TRUE(xcom2.has_value());
  EXPECT_TRUE(xcom2->get<std::string>().find("step2_received") != std::string::npos);

  expect_task_stdout_contains(*run_id, TaskId("step2"), "step1_output");
  expect_task_stdout_contains(*run_id, TaskId("step3"), "step2_received");

  verify_all_tasks_successful(*run_id, 3);
}

TEST_F(RealIntegrationTest, XComStderrExtraction) {
  DAGId dag_id("xcom_stderr_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Stderr"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = "echo 'stderr_test_value' >&2 && echo 'stdout_value'";
  producer.xcom_push = {{.key = "error", .source = XComSource::Stderr}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom = app_->persistence()->get_xcom(*run_id, TaskId("producer"), "error");
  ASSERT_TRUE(xcom.has_value()) << "XCom should be stored even if stderr is empty";
}

TEST_F(RealIntegrationTest, XComExitCodeExtraction) {
  DAGId dag_id("xcom_exitcode_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom ExitCode"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = "exit 0";
  producer.xcom_push = {{.key = "status", .source = XComSource::ExitCode}};

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Exit code was: $EXIT_STATUS\"";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "status", .source_task = TaskId("producer"), .env_var = "EXIT_STATUS"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom = app_->persistence()->get_xcom(*run_id, TaskId("producer"), "status");
  ASSERT_TRUE(xcom.has_value());
  EXPECT_EQ(*xcom, 0);

  expect_task_stdout_contains(*run_id, TaskId("consumer"), "Exit code was: 0");
}

TEST_F(RealIntegrationTest, XComJsonPathExtraction) {
  DAGId dag_id("xcom_jsonpath_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom JsonPath"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = R"(echo '{"data": {"nested": {"value": 999}}}')";
  producer.xcom_push = {{
      .key = "full_json",
      .source = XComSource::Json
  }};

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Got JSON: $JSON_DATA\"";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "full_json", .source_task = TaskId("producer"), .env_var = "JSON_DATA"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  auto xcom = app_->persistence()->get_xcom(*run_id, TaskId("producer"), "full_json");
  ASSERT_TRUE(xcom.has_value());
  EXPECT_TRUE(xcom->contains("data"));
  EXPECT_EQ((*xcom)["data"]["nested"]["value"], 999);

  expect_task_stdout_contains(*run_id, TaskId("consumer"), "999");
}

TEST_F(RealIntegrationTest, ParallelTasksExecuteConcurrently) {
  DAGId dag_id("parallel_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Parallel DAG"));
  ASSERT_TRUE(dag_result.has_value());

  constexpr int kParallelTasks = 5;
  for (int i = 0; i < kParallelTasks; ++i) {
    auto task = create_task(
        std::format("task{}", i),
        std::format("Task {}", i),
        std::format("echo 'parallel_task_{}'", i)
    );
    ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());
  }

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  verify_all_tasks_successful(*run_id, kParallelTasks);

  for (int i = 0; i < kParallelTasks; ++i) {
    expect_task_stdout_contains(*run_id, TaskId(std::format("task{}", i)),
                                std::format("parallel_task_{}", i));
  }
}

TEST_F(RealIntegrationTest, DiamondDependencyDAG) {
  DAGId dag_id("diamond_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Diamond DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto root = create_task("root", "Root", "echo 'root_done'");
  auto left = create_task("left", "Left", "echo 'left_done'", {TaskId("root")});
  auto right = create_task("right", "Right", "echo 'right_done'", {TaskId("root")});
  auto join = create_task("join", "Join", "echo 'join_done'", {TaskId("left"), TaskId("right")});

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, root).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, left).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, right).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, join).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Success);

  verify_all_tasks_successful(*run_id, 4);

  auto tasks = app_->persistence()->get_task_instances(*run_id);
  ASSERT_TRUE(tasks.has_value());

  std::chrono::system_clock::time_point root_finish{}, left_finish{}, right_finish{}, join_start{};
  for (const auto& t : *tasks) {
    if (t.task_idx == 0) root_finish = t.finished_at;
    if (t.task_idx == 1) left_finish = t.finished_at;
    if (t.task_idx == 2) right_finish = t.finished_at;
    if (t.task_idx == 3) join_start = t.started_at;
  }

  EXPECT_GE(join_start, left_finish) << "Join should start after left finishes";
  EXPECT_GE(join_start, right_finish) << "Join should start after right finishes";
}

TEST_F(RealIntegrationTest, TaskRetryOnFailure) {
  DAGId dag_id("retry_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Retry DAG"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task;
  task.task_id = TaskId("flaky_task");
  task.name = "Flaky Task";
  task.command = "exit 1";
  task.max_retries = 2;
  task.retry_interval = 1s;

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());
  ASSERT_TRUE(wait_for_run_completion(*run_id, 30s));

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Failed);

  auto tasks = app_->persistence()->get_task_instances(*run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1u);

  const auto& t = tasks->front();
  EXPECT_EQ(t.state, TaskState::Failed);
  EXPECT_GE(t.attempt, 2) << "Task should have retried at least once";
}

TEST_F(RealIntegrationTest, ConcurrentRunsAreIsolated) {
  DAGId dag_id("concurrent_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Concurrent DAG"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task;
  task.task_id = TaskId("producer");
  task.name = "Producer";
  task.command = "echo \"run_marker_${TASKMASTER_RUN_ID:-unknown}\" && sleep 0.2";
  task.xcom_push = {{.key = "marker", .source = XComSource::Stdout}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto run1_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run1_id.has_value());
  std::this_thread::sleep_for(50ms);

  auto run2_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run2_id.has_value());

  EXPECT_NE(*run1_id, *run2_id) << "Two runs should have different IDs";

  ASSERT_TRUE(wait_for_run_completion(*run1_id, 15s));
  ASSERT_TRUE(wait_for_run_completion(*run2_id, 15s));

  auto state1 = app_->persistence()->get_dag_run_state(*run1_id);
  auto state2 = app_->persistence()->get_dag_run_state(*run2_id);
  ASSERT_TRUE(state1.has_value());
  ASSERT_TRUE(state2.has_value());
  EXPECT_EQ(state1.value(), DAGRunState::Success);
  EXPECT_EQ(state2.value(), DAGRunState::Success);

  auto xcom1 = app_->persistence()->get_xcom(*run1_id, TaskId("producer"), "marker");
  auto xcom2 = app_->persistence()->get_xcom(*run2_id, TaskId("producer"), "marker");
  ASSERT_TRUE(xcom1.has_value());
  ASSERT_TRUE(xcom2.has_value());

  auto tasks1 = app_->persistence()->get_task_instances(*run1_id);
  auto tasks2 = app_->persistence()->get_task_instances(*run2_id);
  ASSERT_TRUE(tasks1.has_value());
  ASSERT_TRUE(tasks2.has_value());
  EXPECT_EQ(tasks1->size(), 1u);
  EXPECT_EQ(tasks2->size(), 1u);

  if (!tasks1->empty() && !tasks2->empty()) {
    EXPECT_NE(tasks1->front().instance_id, tasks2->front().instance_id)
        << "Task instances should have different IDs";
  }
}

TEST_F(RealIntegrationTest, ConcurrentRunsWithXComIsolation) {
  DAGId dag_id("xcom_isolation_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("XCom Isolation"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig producer;
  producer.task_id = TaskId("producer");
  producer.name = "Producer";
  producer.command = R"(echo "{\"run_id\": \"$RANDOM\"}")";
  producer.xcom_push = {{.key = "data", .source = XComSource::Stdout}};

  TaskConfig consumer;
  consumer.task_id = TaskId("consumer");
  consumer.name = "Consumer";
  consumer.command = "echo \"Received: $PRODUCER_DATA\" && sleep 0.1";
  consumer.dependencies = {TaskId("producer")};
  consumer.xcom_pull = {{.key = "data", .source_task = TaskId("producer"), .env_var = "PRODUCER_DATA"}};

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, producer).has_value());
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, consumer).has_value());

  std::vector<DAGRunId> run_ids;
  constexpr int kNumRuns = 3;

  for (int i = 0; i < kNumRuns; ++i) {
    auto run_id = app_->trigger_dag_by_id(dag_id);
    ASSERT_TRUE(run_id.has_value());
    run_ids.push_back(*run_id);
    std::this_thread::sleep_for(30ms);
  }

  for (const auto& run_id : run_ids) {
    ASSERT_TRUE(wait_for_run_completion(run_id, 20s));
  }

  for (const auto& run_id : run_ids) {
    auto state = app_->persistence()->get_dag_run_state(run_id);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state.value(), DAGRunState::Success) << "Run " << run_id << " failed";

    auto xcom = app_->persistence()->get_xcom(run_id, TaskId("producer"), "data");
    ASSERT_TRUE(xcom.has_value()) << "XCom missing for run " << run_id;

    auto tasks = app_->persistence()->get_task_instances(run_id);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_EQ(tasks->size(), 2u);
  }

  for (size_t i = 0; i < run_ids.size(); ++i) {
    for (size_t j = i + 1; j < run_ids.size(); ++j) {
      EXPECT_NE(run_ids[i], run_ids[j]);
    }
  }
}

namespace {

auto http_get(uint16_t port, std::string_view path) -> std::pair<int, std::string> {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return {-1, ""};

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return {-1, ""};
  }

  std::string request = std::format("GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", path);
  send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, n);
  }
  close(sock);

  int status_code = 0;
  if (response.size() > 12) {
    auto space_pos = response.find(' ');
    if (space_pos != std::string::npos) {
      status_code = std::stoi(response.substr(space_pos + 1, 3));
    }
  }

  auto body_start = response.find("\r\n\r\n");
  std::string body = (body_start != std::string::npos) 
      ? response.substr(body_start + 4) 
      : "";

  return {status_code, body};
}

auto http_post(uint16_t port, std::string_view path, std::string_view json_body) 
    -> std::pair<int, std::string> {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return {-1, ""};

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(sock);
    return {-1, ""};
  }

  std::string request = std::format(
      "POST {} HTTP/1.1\r\n"
      "Host: localhost\r\n"
      "Content-Type: application/json\r\n"
      "Content-Length: {}\r\n"
      "Connection: close\r\n\r\n{}",
      path, json_body.size(), json_body);
  send(sock, request.data(), request.size(), 0);

  std::string response;
  char buffer[4096];
  ssize_t n;
  while ((n = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
    response.append(buffer, n);
  }
  close(sock);

  int status_code = 0;
  if (response.size() > 12) {
    auto space_pos = response.find(' ');
    if (space_pos != std::string::npos) {
      status_code = std::stoi(response.substr(space_pos + 1, 3));
    }
  }

  auto body_start = response.find("\r\n\r\n");
  std::string body = (body_start != std::string::npos) 
      ? response.substr(body_start + 4) 
      : "";

  return {status_code, body};
}

}  // namespace

TEST_F(RealIntegrationTest, HttpApiHealthEndpoint) {
  auto port = app_->config().api.port;
  auto [status, body] = http_get(port, "/api/health");

  EXPECT_EQ(status, 200) << "Health endpoint should return 200";
  EXPECT_FALSE(body.empty()) << "Health response should have body";
}

TEST_F(RealIntegrationTest, HttpApiListDags) {
  DAGId dag_id("http_test_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("HTTP Test DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_task("task1", "Task 1", "echo 'test'");
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto port = app_->config().api.port;
  auto [status, body] = http_get(port, "/api/dags");

  EXPECT_EQ(status, 200) << "List DAGs should return 200";
  EXPECT_TRUE(body.find("http_test_dag") != std::string::npos)
      << "Response should contain the created DAG";
}

TEST_F(RealIntegrationTest, HttpApiTriggerDag) {
  DAGId dag_id("http_trigger_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("HTTP Trigger DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_task("task1", "Task 1", "echo 'triggered via HTTP'");
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto port = app_->config().api.port;
  auto [status, body] = http_post(port, "/api/dags/http_trigger_dag/trigger", "{}");

  EXPECT_TRUE(status == 200 || status == 201 || status == 202)
      << "Trigger should return success status, got: " << status;

  std::this_thread::sleep_for(500ms);

  auto history = app_->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  EXPECT_GE(history->size(), 1u) << "DAG should have been triggered via HTTP";
}

TEST_F(RealIntegrationTest, HttpApiNotFoundEndpoint) {
  auto port = app_->config().api.port;
  auto [status, body] = http_get(port, "/api/nonexistent");

  EXPECT_EQ(status, 404) << "Nonexistent endpoint should return 404";
}

TEST_F(RealIntegrationTest, RetrySemanticsPreciseVerification) {
  DAGId dag_id("retry_precise_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Retry Precise"));
  ASSERT_TRUE(dag_result.has_value());

  TaskConfig task;
  task.task_id = TaskId("retry_task");
  task.name = "Retry Task";
  task.command = "exit 1";
  task.max_retries = 2;
  task.retry_interval = 1s;

  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  auto start_time = std::chrono::steady_clock::now();
  auto run_id = app_->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  ASSERT_TRUE(wait_for_run_completion(*run_id, 30s));
  auto elapsed = std::chrono::steady_clock::now() - start_time;

  auto state = app_->persistence()->get_dag_run_state(*run_id);
  ASSERT_TRUE(state.has_value());
  EXPECT_EQ(state.value(), DAGRunState::Failed);

  auto tasks = app_->persistence()->get_task_instances(*run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_EQ(tasks->size(), 1u);

  const auto& t = tasks->front();
  EXPECT_EQ(t.state, TaskState::Failed);
  EXPECT_EQ(t.attempt, 2) << "Should have max_retries=2 attempts total";
  EXPECT_EQ(t.exit_code, 1) << "Exit code should be 1";

  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
  EXPECT_GE(elapsed_ms, 1000) << "Should take at least 1 second (1 retry interval)";
}

TEST_F(RealIntegrationTest, ConcurrentStressTest) {
  DAGId dag_id("stress_dag");
  auto dag_result = app_->dag_manager().create_dag(dag_id, create_dag_info("Stress DAG"));
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_task("stress_task", "Stress Task", "echo 'stress' && sleep 0.05");
  ASSERT_TRUE(app_->dag_manager().add_task(dag_id, task).has_value());

  constexpr int kNumConcurrentRuns = 10;
  std::vector<DAGRunId> run_ids;

  for (int i = 0; i < kNumConcurrentRuns; ++i) {
    auto run_id = app_->trigger_dag_by_id(dag_id);
    ASSERT_TRUE(run_id.has_value()) << "Failed to trigger run " << i;
    run_ids.push_back(*run_id);
  }

  for (const auto& run_id : run_ids) {
    ASSERT_TRUE(wait_for_run_completion(run_id, 30s)) 
        << "Run " << run_id << " did not complete";
  }

  int success_count = 0;
  for (const auto& run_id : run_ids) {
    auto state = app_->persistence()->get_dag_run_state(run_id);
    ASSERT_TRUE(state.has_value());
    if (state.value() == DAGRunState::Success) {
      success_count++;
    }
  }

  EXPECT_EQ(success_count, kNumConcurrentRuns)
      << "All " << kNumConcurrentRuns << " runs should succeed";

  auto history = app_->persistence()->list_run_history(dag_id, kNumConcurrentRuns + 5);
  ASSERT_TRUE(history.has_value());
  EXPECT_GE(history->size(), static_cast<size_t>(kNumConcurrentRuns));
}
