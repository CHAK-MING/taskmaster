#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_policies.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/config/task_types.hpp"
#include "dagforge/executor/executor_types.hpp"
#include "dagforge/util/id.hpp"

#include <chrono>
#include <string>
#include <vector>
#endif

namespace dagforge {

struct TaskConfig {
  struct Compiled {
    int64_t task_rowid{0};
    TaskId task_id;
    ExecutorType executor{ExecutorType::Shell};
    std::string command;
    std::string working_dir;
    std::chrono::seconds execution_timeout{task_defaults::kExecutionTimeout};
  };

  struct Builder;
  static auto builder() -> Builder;

  int64_t task_rowid{0};
  TaskId task_id;
  std::string name;
  std::string command;
  std::string working_dir;
  std::vector<TaskDependency> dependencies;
  ExecutorType executor{ExecutorType::Shell};
  ExecutorConfig executor_config{ShellExecutorConfig{}};
  std::chrono::seconds execution_timeout{task_defaults::kExecutionTimeout};
  std::chrono::seconds retry_interval{task_defaults::kRetryInterval};
  int max_retries{task_defaults::kMaxRetries};
  TriggerRule trigger_rule{TriggerRule::AllSuccess};

  [[nodiscard]] auto compiled() const -> Compiled {
    return Compiled{
        .task_rowid = task_rowid,
        .task_id = task_id,
        .executor = executor,
        .command = command,
        .working_dir = working_dir,
        .execution_timeout = execution_timeout,
    };
  }
};

struct TaskConfig::Builder {
  TaskConfig config_;

  auto id(std::string id_str) -> Builder && {
    config_.task_id = TaskId{std::move(id_str)};
    return std::move(*this);
  }

  auto name(std::string n) -> Builder && {
    config_.name = std::move(n);
    return std::move(*this);
  }

  auto command(std::string cmd) -> Builder && {
    config_.command = std::move(cmd);
    return std::move(*this);
  }

  auto working_dir(std::string dir) -> Builder && {
    config_.working_dir = std::move(dir);
    return std::move(*this);
  }

  auto depends_on(std::string dep_id, std::string label = "") -> Builder && {
    config_.dependencies.push_back(
        {TaskId{std::move(dep_id)}, std::move(label)});
    return std::move(*this);
  }

  auto executor(ExecutorType type) -> Builder && {
    config_.executor = type;
    return std::move(*this);
  }

  auto config(ExecutorConfig cfg) -> Builder && {
    config_.executor_config = std::move(cfg);
    return std::move(*this);
  }

  auto timeout(std::chrono::seconds t) -> Builder && {
    config_.execution_timeout = t;
    return std::move(*this);
  }

  auto retry(int max, std::chrono::seconds interval) -> Builder && {
    config_.max_retries = max;
    config_.retry_interval = interval;
    return std::move(*this);
  }

  auto trigger_rule(TriggerRule rule) -> Builder && {
    config_.trigger_rule = rule;
    return std::move(*this);
  }

  [[nodiscard]] auto build() && -> Result<TaskConfig> {
    if (config_.task_id.empty()) {
      if (config_.name.empty()) {
        return fail(Error::InvalidArgument);
      }
      config_.task_id = TaskId{config_.name};
    }
    if (config_.command.empty() && config_.executor != ExecutorType::Lua &&
        config_.executor != ExecutorType::Noop) {
      return fail(Error::InvalidArgument);
    }
    return ok(std::move(config_));
  }
};

inline auto TaskConfig::builder() -> Builder { return {}; }

} // namespace dagforge
