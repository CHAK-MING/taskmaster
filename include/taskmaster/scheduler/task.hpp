#pragma once

#include "taskmaster/executor/executor.hpp"
#include "taskmaster/scheduler/cron.hpp"

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
};

struct RetryPolicy {
  int max_attempts{3};
  std::chrono::seconds delay{std::chrono::seconds(0)};
};

struct TaskDefinition {
  std::string id;
  std::string name;
  CronExpr schedule;
  ExecutorConfig executor;
  RetryPolicy retry{};
  bool enabled{true};
};

struct TaskInstance {
  std::string instance_id;
  std::string task_id;
  TaskState state{TaskState::Pending};
  std::chrono::system_clock::time_point scheduled_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  int attempt{0};
  int exit_code{0};
  std::string error_message;
};

}  // namespace taskmaster
