// Tests for SensorExecutor - File, HTTP, and Command sensors

#include "taskmaster/app/application.hpp"
#include "taskmaster/config/task_config.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/storage/persistence.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <latch>
#include <thread>

using namespace taskmaster;
using namespace std::chrono_literals;

class SensorExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Clean up any leftover test files
    std::error_code ec;
    std::filesystem::remove("/tmp/sensor_test_file.txt", ec);
    std::filesystem::remove("/tmp/sensor_test_ready", ec);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove("/tmp/sensor_test_file.txt", ec);
    std::filesystem::remove("/tmp/sensor_test_ready", ec);
  }

  auto create_app() -> std::unique_ptr<Application> {
    SystemConfig config;
    config.storage.db_file = ":memory:";
    config.api.enabled = false;
    config.scheduler.tick_interval_ms = 50;
    return std::make_unique<Application>(std::move(config));
  }

  auto create_sensor_task(const std::string &id, SensorType type,
                          const std::string &target,
                          std::chrono::seconds timeout = 5s,
                          std::chrono::seconds interval = 1s,
                          int expected_status = 200, int max_retries = 0)
      -> TaskConfig {
    TaskConfig task;
    task.task_id = TaskId(id);
    task.name = id;
    task.executor = ExecutorType::Sensor;
    task.execution_timeout = timeout;
    task.max_retries = max_retries;

    SensorTaskConfig sensor_config;
    sensor_config.type = type;
    sensor_config.target = target;
    sensor_config.poke_interval = interval;
    sensor_config.sensor_timeout = timeout;
    sensor_config.expected_status = expected_status;
    task.executor_config = sensor_config;

    return task;
  }
};

TEST_F(SensorExecutorTest, FileSensor_FileExists_Succeeds) {
  auto app = create_app();
  app->start();

  // Create the file before running the sensor
  std::ofstream("/tmp/sensor_test_file.txt") << "test content";

  DAGId dag_id("file_sensor_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "File Sensor Test"});
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_sensor_task("wait_file", SensorType::File,
                                 "/tmp/sensor_test_file.txt");
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  // Wait for completion
  std::this_thread::sleep_for(2s);

  auto history = app->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  ASSERT_FALSE(history->empty());
  EXPECT_EQ(history->front().state, DAGRunState::Success);

  app->stop();
}

TEST_F(SensorExecutorTest, FileSensor_FileAppearsLater_Succeeds) {
  auto app = create_app();
  app->start();

  DAGId dag_id("file_sensor_delayed_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "File Sensor Delayed"});
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_sensor_task("wait_file", SensorType::File,
                                 "/tmp/sensor_test_file.txt", 10s, 1s);
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  // Wait a bit, then create the file
  std::this_thread::sleep_for(1s);
  std::ofstream("/tmp/sensor_test_file.txt") << "delayed content";

  // Wait for completion
  std::this_thread::sleep_for(3s);

  auto history = app->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  ASSERT_FALSE(history->empty());
  EXPECT_EQ(history->front().state, DAGRunState::Success);

  app->stop();
}

TEST_F(SensorExecutorTest, FileSensor_Timeout_Fails) {
  auto app = create_app();
  app->start();

  DAGId dag_id("file_sensor_timeout_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "File Sensor Timeout"});
  ASSERT_TRUE(dag_result.has_value());

  // Short timeout, file doesn't exist
  auto task = create_sensor_task("wait_file", SensorType::File,
                                 "/tmp/nonexistent_file_xyz.txt", 2s, 1s);
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  // Wait for timeout
  std::this_thread::sleep_for(4s);

  auto history = app->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  ASSERT_FALSE(history->empty());
  EXPECT_EQ(history->front().state, DAGRunState::Failed);

  app->stop();
}

TEST_F(SensorExecutorTest, CommandSensor_SuccessfulCommand_Succeeds) {
  auto app = create_app();
  app->start();

  DAGId dag_id("command_sensor_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "Command Sensor Test"});
  ASSERT_TRUE(dag_result.has_value());

  // Command that always succeeds
  auto task =
      create_sensor_task("check_cmd", SensorType::Command, "true", 5s, 1s);
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  std::this_thread::sleep_for(3s);

  auto history = app->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  ASSERT_FALSE(history->empty());

  EXPECT_EQ(history->front().state, DAGRunState::Success);

  app->stop();
}

TEST_F(SensorExecutorTest, HttpSensor_EndpointUnavailable_TimesOutAndFails) {
  auto app = create_app();
  app->start();

  DAGId dag_id("http_sensor_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "HTTP Sensor Test"});
  ASSERT_TRUE(dag_result.has_value());

  auto task = create_sensor_task("check_http", SensorType::Http,
                                 "http://localhost:9999/health", 2s, 1s, 200);
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  std::this_thread::sleep_for(4s);

  auto history = app->persistence()->list_run_history(dag_id, 1);
  ASSERT_TRUE(history.has_value());
  ASSERT_FALSE(history->empty());

  EXPECT_EQ(history->front().state, DAGRunState::Failed);

  auto tasks =
      app->persistence()->get_task_instances(history->front().dag_run_id);
  ASSERT_TRUE(tasks.has_value());
  ASSERT_FALSE(tasks->empty());
  EXPECT_TRUE(tasks->front().error_message.find("timeout") != std::string::npos)
      << "Expected timeout error, got: " << tasks->front().error_message;

  app->stop();
}

TEST_F(SensorExecutorTest, FileSensor_Cancel_StopsSensor) {
  auto app = create_app();
  app->start();

  DAGId dag_id("cancel_sensor_dag");
  auto dag_result = app->dag_manager().create_dag(
      dag_id, DAGInfo{.name = "Cancel Sensor Test"});
  ASSERT_TRUE(dag_result.has_value());

  // Long timeout so we can cancel before it finishes
  auto task = create_sensor_task("wait_file", SensorType::File,
                                 "/tmp/never_exists_file.txt", 60s, 1s);
  ASSERT_TRUE(app->dag_manager().add_task(dag_id, task).has_value());

  auto run_id = app->trigger_dag_by_id(dag_id);
  ASSERT_TRUE(run_id.has_value());

  // Wait a bit then stop the app (which should cancel running tasks)
  std::this_thread::sleep_for(1s);
  app->stop();

  // App stopped successfully without hanging - test passes
  SUCCEED();
}
