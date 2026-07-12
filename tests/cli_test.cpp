#include "dagforge/cli/commands.hpp"

#include "gtest/gtest.h"

using namespace dagforge;
using namespace dagforge::cli;

TEST(CLITest, TriggerOptionsDefaults) {
  TriggerOptions opts;
  EXPECT_TRUE(opts.dag_id.empty());
  EXPECT_TRUE(opts.execution_date.empty());
  EXPECT_FALSE(opts.no_api);
  EXPECT_FALSE(opts.wait);
  EXPECT_FALSE(opts.json);
}

TEST(CLITest, ServeStartOptionsDefaults) {
  ServeStartOptions opts;
  EXPECT_TRUE(opts.config_file.empty());
  EXPECT_FALSE(opts.pid_file.has_value());
  EXPECT_FALSE(opts.log_file.has_value());
  EXPECT_FALSE(opts.log_level.has_value());
  EXPECT_FALSE(opts.no_api);
  EXPECT_FALSE(opts.daemon);
  EXPECT_FALSE(opts.shards.has_value());
}

TEST(CLITest, ServeStopStatusOptionsDefaults) {
  ServeStopOptions stop;
  EXPECT_TRUE(stop.config_file.empty());
  EXPECT_FALSE(stop.pid_file.has_value());
  EXPECT_EQ(stop.timeout_sec, 10);
  EXPECT_FALSE(stop.force);

  ServeStatusOptions status;
  EXPECT_TRUE(status.config_file.empty());
  EXPECT_FALSE(status.pid_file.has_value());
  EXPECT_FALSE(status.json);
}

TEST(CLITest, ListRunsOptionsDefaults) {
  ListRunsOptions opts;
  EXPECT_EQ(opts.limit, 50u);
  EXPECT_EQ(opts.output, "table");
  EXPECT_FALSE(opts.json);
  EXPECT_TRUE(opts.dag_id.empty());
  EXPECT_TRUE(opts.state.empty());
}

TEST(CLITest, ListDagsOptionsDefaults) {
  ListDagsOptions opts;
  EXPECT_EQ(opts.limit, 50u);
  EXPECT_EQ(opts.output, "table");
  EXPECT_FALSE(opts.json);
  EXPECT_FALSE(opts.include_stale);
}

TEST(CLITest, ListTasksOptionsDefaults) {
  ListTasksOptions opts;
  EXPECT_EQ(opts.output, "table");
  EXPECT_TRUE(opts.dag_id.empty());
  EXPECT_FALSE(opts.json);
}

TEST(CLITest, ClearOptionsDefaults) {
  ClearOptions opts;
  EXPECT_TRUE(opts.run_id.empty());
  EXPECT_FALSE(opts.failed_only);
  EXPECT_FALSE(opts.all_tasks);
  EXPECT_FALSE(opts.downstream);
  EXPECT_FALSE(opts.dry_run);
  EXPECT_FALSE(opts.task.has_value());
}

TEST(CLITest, PauseUnpauseOptionsDefaults) {
  PauseOptions pause;
  EXPECT_TRUE(pause.dag_id.empty());
  EXPECT_FALSE(pause.json);

  UnpauseOptions unpause;
  EXPECT_TRUE(unpause.dag_id.empty());
  EXPECT_FALSE(unpause.json);
}

TEST(CLITest, InspectOptionsDefaults) {
  InspectOptions opts;
  EXPECT_TRUE(opts.run_id.empty());
  EXPECT_FALSE(opts.details);
  EXPECT_FALSE(opts.json);
}

TEST(CLITest, LogsAndTestDefaults) {
  LogsOptions logs;
  EXPECT_FALSE(logs.short_time);

  TestTaskOptions test_task;
  EXPECT_TRUE(test_task.config_file.empty());
  EXPECT_TRUE(test_task.dag_id.empty());
  EXPECT_TRUE(test_task.task_id.empty());
  EXPECT_FALSE(test_task.json);
}
