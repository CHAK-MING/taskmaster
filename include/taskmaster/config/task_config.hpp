#pragma once

#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/xcom/xcom_types.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace taskmaster {

enum class XComSource { Stdout, Stderr, ExitCode, Json };

struct XComPushConfig {
  std::string key;
  XComSource source{XComSource::Stdout};
  std::optional<std::string> json_path;
  std::optional<std::string> regex_pattern;
  int regex_group{0};
};

struct XComPullConfig {
  std::string key;
  TaskId source_task;
  std::string env_var;
};

struct TaskConfig {
  TaskId task_id;
  std::string name;
  std::string command;
  std::string working_dir;
  std::vector<TaskId> dependencies;
  ExecutorType executor{ExecutorType::Shell};
  std::chrono::seconds timeout{std::chrono::seconds(300)};
  std::chrono::seconds retry_interval{std::chrono::seconds(60)};
  int max_retries{3};

  std::vector<XComPushConfig> xcom_push;
  std::vector<XComPullConfig> xcom_pull;
};

}  // namespace taskmaster
