#pragma once

#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"

#include <chrono>
#include <string>
#include <vector>

namespace taskmaster {

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
};

}  // namespace taskmaster
