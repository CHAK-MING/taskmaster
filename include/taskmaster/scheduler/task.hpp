#pragma once

#include "taskmaster/scheduler/cron.hpp"
#include "taskmaster/util/id.hpp"

#include <chrono>
#include <cstdint>
#include <string>

namespace taskmaster {

enum class TaskState : std::uint8_t {
  Pending,
  Running,
  Success,
  Failed,
  UpstreamFailed,
  Retrying,
  Skipped,
};

struct RetryPolicy {
  int max_retries{3};
  std::chrono::seconds retry_interval{std::chrono::seconds(0)};
};

struct ExecutionInfo {
  DAGId dag_id;
  TaskId task_id;
  std::string name;
  std::optional<CronExpr> cron_expr;
  std::optional<std::chrono::system_clock::time_point> start_date;
  std::optional<std::chrono::system_clock::time_point> end_date;
  bool catchup{false};
};

struct TaskInstance {
  InstanceId instance_id;
  TaskId task_id;
  TaskState state{TaskState::Pending};
  std::chrono::system_clock::time_point scheduled_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  int attempt{0};
  int exit_code{0};
  std::string error_message;
};

}  // namespace taskmaster
