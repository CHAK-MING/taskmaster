#include <gtest/gtest.h>

import dagforge.core;
import dagforge.config;
import dagforge.http;
import dagforge.io;
import dagforge.metrics;
import dagforge.dag;
import dagforge.domain;
import dagforge.scheduler;
import dagforge.storage.schema;
import dagforge.util;

TEST(ModulesCoreSmokeTest, ImportsCoreWithoutDagforgeHeaders) {
  auto result = dagforge::ok(42);
  dagforge::metrics::Counter counter;
  counter.inc();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
  EXPECT_EQ(counter.load(), 1U);
  EXPECT_GT(dagforge::timing::kShutdownPollInterval.count(), 0);
}

TEST(ModulesCoreSmokeTest, ImportsNonCoreValueModulesWithoutDagforgeHeaders) {
  dagforge::DAGId dag_id{"example_dag"};
  dagforge::TaskId task_id{"extract"};
  dagforge::InstanceId instance_id{"example_dag__run_extract"};
  dagforge::RetryPolicy retry_policy;

  EXPECT_EQ(dag_id.value(), "example_dag");
  EXPECT_EQ(task_id.value(), "extract");
  EXPECT_EQ(instance_id.value(), "example_dag__run_extract");
  EXPECT_EQ(retry_policy.max_retries, 3);
  EXPECT_EQ(retry_policy.retry_interval.count(), 0);
  EXPECT_EQ(dagforge::to_string_view(dagforge::TaskState::Pending), "pending");
  EXPECT_EQ(dagforge::to_string_view(dagforge::TaskState::Ready), "ready");
  EXPECT_EQ(dagforge::to_string_view(dagforge::TriggerRule::AllSuccess),
            "all_success");
}

TEST(ModulesCoreSmokeTest, DagRunIdsUseUuidV7) {
  const dagforge::DAGId dag_id{"example"};
  const auto first = dagforge::generate_dag_run_id(dag_id);
  const auto second = dagforge::generate_dag_run_id(dag_id);

  constexpr std::string_view kPrefix = "example__";
  ASSERT_TRUE(first.value().starts_with(kPrefix));
  ASSERT_TRUE(second.value().starts_with(kPrefix));
  const auto first_uuid = first.value().substr(kPrefix.size());
  const auto second_uuid = second.value().substr(kPrefix.size());
  ASSERT_EQ(first_uuid.size(), 36U);
  ASSERT_EQ(second_uuid.size(), 36U);
  EXPECT_EQ(first_uuid[14], '7');
  EXPECT_NE(std::string_view{"89ab"}.find(first_uuid[19]),
            std::string_view::npos);
  EXPECT_NE(first_uuid, second_uuid);
  EXPECT_EQ(dagforge::dag_id_from_run_id(first), dag_id);
}

TEST(ModulesCoreSmokeTest, ImportsLeafUtilityAndIoModules) {
  auto parsed = dagforge::util::parse_int<int>("42");
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(*parsed, 42);

  const auto shard = dagforge::util::shard_of(12345U, 8);
  EXPECT_LT(shard, 8U);

  auto io_ec = dagforge::io::make_error_code(dagforge::io::IoError::TimedOut);
  EXPECT_EQ(io_ec.category().name(), std::string_view{"dagforge.io"});

  dagforge::http::QueryParams params{"dag_id=example&limit=10"};
  EXPECT_TRUE(params.has("dag_id"));
  EXPECT_EQ(params.get("limit").value(), "10");
}

TEST(ModulesCoreSmokeTest, ImportsDagAndConfigValueModulesWithoutDagforgeHeaders) {
  dagforge::TaskInstanceInfo task_instance{};
  task_instance.instance_id = dagforge::InstanceId{"dag__run__task"};
  task_instance.task_id = dagforge::TaskId{"task"};

  EXPECT_EQ(dagforge::to_string_view(dagforge::DAGRunState::Running),
            "running");
  EXPECT_EQ(task_instance.state, dagforge::TaskState::Pending);
  EXPECT_EQ(dagforge::to_string_view(dagforge::DAGSourceMode::Hybrid),
            "hybrid");

  dagforge::SystemConfig config;
  EXPECT_EQ(config.database.port, 3306);
  EXPECT_EQ(config.api.port, 8888);
  EXPECT_EQ(config.dag_source.mode, dagforge::DAGSourceMode::File);
}

TEST(ModulesCoreSmokeTest, ImportsStorageSchemaWithoutDagforgeHeaders) {
  EXPECT_EQ(dagforge::schema::CURRENT_SCHEMA_VERSION, 4);
  EXPECT_NE(dagforge::schema::V1_SCHEMA.find("CREATE TABLE IF NOT EXISTS dags"),
            std::string_view::npos);
}
