#pragma once

#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/xcom/xcom_types.hpp"

#include <chrono>
#include <optional>
#include <ranges>
#include <string>
#include <variant>
#include <vector>

namespace taskmaster {

enum class TriggerRule : std::uint8_t {
  AllSuccess,             // Default: all upstream tasks succeeded
  AllFailed,              // All upstream tasks failed
  AllDone,                // All upstream tasks completed (success or failed)
  OneSuccess,             // At least one upstream task succeeded
  OneFailed,              // At least one upstream task failed
  NoneFailed,             // No upstream task failed (may have skipped)
  NoneSkipped,            // No upstream task was skipped
  AllDoneMinOneSuccess,   // All done and at least one succeeded
  AllSkipped,             // All upstream tasks were skipped
  OneDone,                // At least one upstream completed (success or fail)
  NoneFailedMinOneSuccess,// None failed and at least one succeeded
  Always,                 // Always trigger regardless of upstream states
};

[[nodiscard]] constexpr auto to_string_view(TriggerRule rule) noexcept
    -> std::string_view {
  switch (rule) {
    case TriggerRule::AllSuccess: return "all_success";
    case TriggerRule::AllFailed: return "all_failed";
    case TriggerRule::AllDone: return "all_done";
    case TriggerRule::OneSuccess: return "one_success";
    case TriggerRule::OneFailed: return "one_failed";
    case TriggerRule::NoneFailed: return "none_failed";
    case TriggerRule::NoneSkipped: return "none_skipped";
    case TriggerRule::AllDoneMinOneSuccess: return "all_done_min_one_success";
    case TriggerRule::AllSkipped: return "all_skipped";
    case TriggerRule::OneDone: return "one_done";
    case TriggerRule::NoneFailedMinOneSuccess: return "none_failed_min_one_success";
    case TriggerRule::Always: return "always";
  }
  std::unreachable();
}

template <typename T>
[[nodiscard]] auto parse(std::string_view s) noexcept -> T;

template <>
[[nodiscard]] inline auto parse<TriggerRule>(std::string_view s) noexcept
    -> TriggerRule {
  if (s == "all_failed") return TriggerRule::AllFailed;
  if (s == "all_done") return TriggerRule::AllDone;
  if (s == "one_success") return TriggerRule::OneSuccess;
  if (s == "one_failed") return TriggerRule::OneFailed;
  if (s == "none_failed") return TriggerRule::NoneFailed;
  if (s == "none_skipped") return TriggerRule::NoneSkipped;
  if (s == "all_done_min_one_success") return TriggerRule::AllDoneMinOneSuccess;
  if (s == "all_skipped") return TriggerRule::AllSkipped;
  if (s == "one_done") return TriggerRule::OneDone;
  if (s == "none_failed_min_one_success") return TriggerRule::NoneFailedMinOneSuccess;
  if (s == "always") return TriggerRule::Always;
  return TriggerRule::AllSuccess;
}

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

struct TaskDependency {
  TaskId task_id;
  std::string label;
  
  bool operator==(const TaskId& other) const { return task_id == other; }
  bool operator==(const TaskDependency& other) const = default;
};

inline auto get_dep_task_ids(const std::vector<TaskDependency>& deps) {
  return deps | std::views::transform([](const TaskDependency& d) -> const TaskId& { 
    return d.task_id; 
  });
}

struct ShellTaskConfig {};

struct DockerTaskConfig {
  std::string image;
  std::string socket{"/var/run/docker.sock"};
  ImagePullPolicy pull_policy{ImagePullPolicy::Never};
};

struct SensorTaskConfig {
  SensorType type{SensorType::File};
  std::string target;
  std::chrono::seconds poke_interval{std::chrono::seconds(30)};
  std::chrono::seconds sensor_timeout{std::chrono::seconds(3600)};
  bool soft_fail{false};
  int expected_status{200};
  std::string http_method{"GET"};
};

using ExecutorTaskConfig = std::variant<ShellTaskConfig, DockerTaskConfig, SensorTaskConfig>;

struct TaskConfig {
  TaskId task_id;
  std::string name;
  std::string command;
  std::string working_dir;
  std::vector<TaskDependency> dependencies;
  ExecutorType executor{ExecutorType::Shell};
  ExecutorTaskConfig executor_config{ShellTaskConfig{}};
  std::chrono::seconds execution_timeout{std::chrono::seconds(300)};
  std::chrono::seconds retry_interval{std::chrono::seconds(60)};
  int max_retries{3};
  TriggerRule trigger_rule{TriggerRule::AllSuccess};
  bool is_branch{false};
  std::string branch_xcom_key{"branch"};
  bool depends_on_past{false};

  std::vector<XComPushConfig> xcom_push;
  std::vector<XComPullConfig> xcom_pull;
};

}  // namespace taskmaster
