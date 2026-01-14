#include "taskmaster/config/config.hpp"
#include "taskmaster/config/task_config.hpp"
#include "taskmaster/util/id.hpp"

#include <fstream>

#include "gtest/gtest.h"

using namespace taskmaster;

// TaskConfig tests
TEST(ConfigTest, TaskConfigDefaults) {
  TaskConfig task;

  EXPECT_TRUE(task.task_id.empty());
  EXPECT_TRUE(task.name.empty());
  EXPECT_TRUE(task.command.empty());
  EXPECT_TRUE(task.working_dir.empty());
  EXPECT_TRUE(task.dependencies.empty());
  EXPECT_EQ(task.executor, ExecutorType::Shell);
  EXPECT_EQ(task.timeout, std::chrono::seconds(300));
  EXPECT_EQ(task.retry_interval, std::chrono::seconds(60));
  EXPECT_EQ(task.max_retries, 3);
}

TEST(ConfigTest, TaskConfigWithDeps) {
  TaskConfig task;

  task.dependencies.push_back(TaskId("dep1"));
  task.dependencies.push_back(TaskId("dep2"));

  EXPECT_EQ(task.dependencies.size(), 2);
  EXPECT_EQ(task.dependencies[0], TaskId("dep1"));
  EXPECT_EQ(task.dependencies[1], TaskId("dep2"));
}

TEST(ConfigTest, TaskConfigCustomValues) {
  TaskConfig task;

  task.task_id = TaskId("custom_id");
  task.name = "Custom Task";
  task.command = "echo custom";
  task.working_dir = "/tmp";
  task.timeout = std::chrono::seconds(600);
  task.retry_interval = std::chrono::seconds(120);
  task.max_retries = 5;

  EXPECT_EQ(task.task_id, TaskId("custom_id"));
  EXPECT_EQ(task.name, "Custom Task");
  EXPECT_EQ(task.command, "echo custom");
  EXPECT_EQ(task.working_dir, "/tmp");
  EXPECT_EQ(task.timeout, std::chrono::seconds(600));
  EXPECT_EQ(task.retry_interval, std::chrono::seconds(120));
  EXPECT_EQ(task.max_retries, 5);
}

// SchedulerConfig tests
TEST(ConfigTest, SchedulerConfigDefaults) {
  SchedulerConfig config;

  EXPECT_EQ(config.log_level, "info");
  EXPECT_TRUE(config.log_file.empty());
  EXPECT_TRUE(config.pid_file.empty());
  EXPECT_EQ(config.tick_interval_ms, 1000);
}

TEST(ConfigTest, SchedulerConfigCustomValues) {
  SchedulerConfig config;

  config.log_level = "debug";
  config.log_file = "/var/log/taskmaster.log";
  config.pid_file = "/var/run/taskmaster.pid";
  config.tick_interval_ms = 500;

  EXPECT_EQ(config.log_level, "debug");
  EXPECT_EQ(config.log_file, "/var/log/taskmaster.log");
  EXPECT_EQ(config.pid_file, "/var/run/taskmaster.pid");
  EXPECT_EQ(config.tick_interval_ms, 500);
}

// ApiConfig tests
TEST(ConfigTest, ApiConfigDefaults) {
  ApiConfig config;

  EXPECT_FALSE(config.enabled);
  EXPECT_EQ(config.port, 8080);
  EXPECT_EQ(config.host, "127.0.0.1");
}

TEST(ConfigTest, ApiConfigCustomValues) {
  ApiConfig config;

  config.enabled = true;
  config.port = 9000;
  config.host = "0.0.0.0";

  EXPECT_TRUE(config.enabled);
  EXPECT_EQ(config.port, 9000);
  EXPECT_EQ(config.host, "0.0.0.0");
}

// StorageConfig tests
TEST(ConfigTest, StorageConfigDefaults) {
  StorageConfig config;

  EXPECT_EQ(config.db_file, "taskmaster.db");
}

TEST(ConfigTest, StorageConfigCustomValues) {
  StorageConfig config;

  config.db_file = "/var/lib/taskmaster.db";

  EXPECT_EQ(config.db_file, "/var/lib/taskmaster.db");
}

// Config tests (without tasks field)
TEST(ConfigTest, ConfigDefaults) {
  Config config;

  EXPECT_EQ(config.scheduler.log_level, "info");
  EXPECT_FALSE(config.api.enabled);
  EXPECT_EQ(config.storage.db_file, "taskmaster.db");
}

TEST(ConfigTest, LoadFromYamlString) {
  std::string yaml = R"(
storage:
  db_file: /test/db.db

scheduler:
  log_level: debug
  log_file: /var/log/test.log
  tick_interval_ms: 500

api:
  enabled: true
  port: 9999
  host: 0.0.0.0

dag_source:
  mode: file
  directory: ./custom_dags
  scan_interval_sec: 60
)";

  auto result = ConfigLoader::load_from_string(yaml);
  ASSERT_TRUE(result.has_value()) << "Failed to parse YAML: " << result.error().message();

  auto& config = *result;
  EXPECT_EQ(config.storage.db_file, "/test/db.db");
  EXPECT_EQ(config.scheduler.log_level, "debug");
  EXPECT_EQ(config.scheduler.log_file, "/var/log/test.log");
  EXPECT_EQ(config.scheduler.tick_interval_ms, 500);
  EXPECT_TRUE(config.api.enabled);
  EXPECT_EQ(config.api.port, 9999);
  EXPECT_EQ(config.api.host, "0.0.0.0");
  EXPECT_EQ(config.dag_source.directory, "./custom_dags");
  EXPECT_EQ(config.dag_source.scan_interval_sec, 60);
}

TEST(ConfigTest, LoadFromYamlFile) {
  std::string yaml = R"(
scheduler:
  log_level: info

api:
  enabled: false
  port: 8080
)";

  std::string temp_path = "/tmp/taskmaster_config_test.yaml";
  std::ofstream out(temp_path);
  out << yaml;
  out.close();

  auto result = ConfigLoader::load_from_file(temp_path);
  ASSERT_TRUE(result.has_value()) << "Failed to load file: " << result.error().message();

  auto& config = *result;
  EXPECT_EQ(config.scheduler.log_level, "info");
  EXPECT_FALSE(config.api.enabled);

  std::remove(temp_path.c_str());
}

TEST(ConfigTest, LoadFromYamlMissingFile) {
  auto result = ConfigLoader::load_from_file("/nonexistent/path/config.yaml");
  EXPECT_FALSE(result.has_value());
}

TEST(ConfigTest, ConfigFieldAccess) {
  Config config;
  config.storage.db_file = "/custom/db.db";
  config.scheduler.log_level = "debug";
  config.api.enabled = true;
  config.api.port = 9000;
  config.dag_source.directory = "./my_dags";

  EXPECT_EQ(config.storage.db_file, "/custom/db.db");
  EXPECT_EQ(config.scheduler.log_level, "debug");
  EXPECT_TRUE(config.api.enabled);
  EXPECT_EQ(config.api.port, 9000);
  EXPECT_EQ(config.dag_source.directory, "./my_dags");
}

TEST(ConfigTest, ConfigRoundTrip) {
  std::string yaml = R"(
storage:
  db_file: /roundtrip/test.db

scheduler:
  log_level: warn

api:
  enabled: false
  port: 8080
)";

  auto load_result = ConfigLoader::load_from_string(yaml);
  ASSERT_TRUE(load_result.has_value());

  EXPECT_EQ(load_result->storage.db_file, "/roundtrip/test.db");
  EXPECT_EQ(load_result->scheduler.log_level, "warn");
}

TEST(ConfigTest, DAGSourceConfigDefaults) {
  DAGSourceConfig config;

  EXPECT_EQ(config.mode, DAGSourceMode::File);
  EXPECT_EQ(config.directory, "./dags");
  EXPECT_EQ(config.scan_interval_sec, 30);
}

TEST(ConfigTest, DAGSourceConfigCustomValues) {
  DAGSourceConfig config;

  config.mode = DAGSourceMode::File;
  config.directory = "/opt/dags";
  config.scan_interval_sec = 120;

  EXPECT_EQ(config.mode, DAGSourceMode::File);
  EXPECT_EQ(config.directory, "/opt/dags");
  EXPECT_EQ(config.scan_interval_sec, 120);
}
