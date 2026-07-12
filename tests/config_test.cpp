#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/config/task_config.hpp"

#include "gtest/gtest.h"

#include <cstdlib>

using namespace dagforge;

TEST(ConfigTest, DatabaseDefaults) {
  DatabaseConfig db;
  EXPECT_EQ(db.host, "127.0.0.1");
  EXPECT_EQ(db.port, 3306);
  EXPECT_EQ(db.username, "dagforge");
  EXPECT_EQ(db.database, "dagforge");
}

TEST(ConfigTest, ComputeDefaults) {
  ComputeConfig cfg;
  EXPECT_EQ(cfg.threads, 0);
  EXPECT_EQ(cfg.queue_capacity, 1024);
  EXPECT_FALSE(cfg.pin_threads_to_cores);
  EXPECT_EQ(cfg.cpu_affinity_offset, 0);
}

TEST(ConfigTest, SchedulerDefaults) {
  SchedulerConfig cfg;
  EXPECT_EQ(cfg.log_level, "info");
  EXPECT_EQ(cfg.tick_interval_ms, 1000);
  EXPECT_EQ(cfg.max_concurrency, 10);
  EXPECT_EQ(cfg.scheduler_shards, 1);
  EXPECT_FALSE(cfg.pin_shards_to_cores);
  EXPECT_EQ(cfg.cpu_affinity_offset, 0);
}

TEST(ConfigTest, ApiDefaults) {
  ApiConfig cfg;
  EXPECT_FALSE(cfg.enabled);
  EXPECT_EQ(cfg.host, "127.0.0.1");
  EXPECT_EQ(cfg.port, 8888);
}

TEST(ConfigTest, LoadFromTomlString) {
  std::string toml = R"(
[database]
host = "127.0.0.1"
port = 3306
username = "dagforge"
password = "dagforge"
database = "dagforge_test"
pool_size = 4
connect_timeout = 5

[compute]
threads = 3
queue_capacity = 256
pin_threads_to_cores = true
cpu_affinity_offset = 2

[scheduler]
log_level = "debug"
max_concurrency = 8
scheduler_shards = 2
pin_shards_to_cores = true
cpu_affinity_offset = 1

[api]
enabled = true
port = 9999
host = "0.0.0.0"

[dag_source]
mode = "file"
directory = "./dags"
scan_interval_sec = 30
)";

  auto result = SystemConfigLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(result->database.database, "dagforge_test");
  EXPECT_EQ(result->compute.threads, 3);
  EXPECT_EQ(result->compute.queue_capacity, 256);
  EXPECT_TRUE(result->compute.pin_threads_to_cores);
  EXPECT_EQ(result->compute.cpu_affinity_offset, 2);
  EXPECT_EQ(result->scheduler.log_level, "debug");
  EXPECT_EQ(result->scheduler.scheduler_shards, 2);
  EXPECT_TRUE(result->scheduler.pin_shards_to_cores);
  EXPECT_EQ(result->scheduler.cpu_affinity_offset, 1);
  EXPECT_TRUE(result->api.enabled);
  EXPECT_EQ(result->api.port, 9999);
}

TEST(ConfigTest, RejectsInvalidComputeConfiguration) {
  auto negative_threads = SystemConfigLoader::load_from_string(R"(
[compute]
threads = -1
)");
  ASSERT_FALSE(negative_threads.has_value());
  EXPECT_EQ(negative_threads.error(), make_error_code(Error::ParseError));

  auto empty_queue = SystemConfigLoader::load_from_string(R"(
[compute]
queue_capacity = 0
)");
  ASSERT_FALSE(empty_queue.has_value());
  EXPECT_EQ(empty_queue.error(), make_error_code(Error::ParseError));
}

TEST(ConfigTest, TaskConfigDefaults) {
  TaskConfig task;
  EXPECT_TRUE(task.task_id.empty());
  EXPECT_EQ(task.executor, ExecutorType::Shell);
  EXPECT_EQ(task.max_retries, 3);
}

TEST(ConfigTest, EnvironmentOverridesTakePrecedence) {
  constexpr auto *kApiPort = "DAGFORGE_API_PORT";
  constexpr auto *kComputeThreads = "DAGFORGE_COMPUTE_THREADS";
  constexpr auto *kDbHost = "DAGFORGE_DB_HOST";
  constexpr auto *kDagDir = "DAGFORGE_DAG_DIRECTORY";

  ::setenv(kApiPort, "7777", 1);
  ::setenv(kComputeThreads, "6", 1);
  ::setenv(kDbHost, "db.internal", 1);
  ::setenv(kDagDir, "/tmp/dags-env", 1);

  std::string toml = R"(
[database]
host = "127.0.0.1"

[dag_source]
mode = "file"
directory = "./dags"

[api]
port = 8080
)";

  auto result = SystemConfigLoader::load_from_string(toml);

  ::unsetenv(kApiPort);
  ::unsetenv(kComputeThreads);
  ::unsetenv(kDbHost);
  ::unsetenv(kDagDir);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->api.port, 7777);
  EXPECT_EQ(result->compute.threads, 6);
  EXPECT_EQ(result->database.host, "db.internal");
  EXPECT_EQ(result->dag_source.directory, "/tmp/dags-env");
}
