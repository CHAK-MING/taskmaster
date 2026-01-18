#pragma once

#include "taskmaster/dag/dag_run.hpp"

#include <array>
#include <ranges>
#include <string_view>
#include <utility>

namespace taskmaster {

namespace detail {

constexpr std::array<std::string_view, 3> kDagRunStateNames = {
    "running",
    "success",
    "failed",
};

constexpr std::array<std::string_view, 7> kTaskStateNames = {
    "pending",
    "running",
    "success",
    "failed",
    "upstream_failed",
    "retrying",
    "skipped",
};

constexpr std::array<std::string_view, 2> kTriggerTypeNames = {
    "manual",
    "schedule",
};

}  // namespace detail

[[nodiscard]] inline auto dag_run_state_name(DAGRunState state) noexcept
    -> const char* {
  auto idx = std::to_underlying(state);
  return idx < detail::kDagRunStateNames.size()
             ? detail::kDagRunStateNames[idx].data()
             : "unknown";
}

[[nodiscard]] inline auto parse_dag_run_state(std::string_view name) noexcept
    -> DAGRunState {
  auto it = std::ranges::find(detail::kDagRunStateNames, name);
  if (it != detail::kDagRunStateNames.end()) {
    return static_cast<DAGRunState>(
        std::ranges::distance(detail::kDagRunStateNames.begin(), it));
  }
  return DAGRunState::Running;
}

[[nodiscard]] inline auto trigger_type_name(TriggerType type) noexcept
    -> const char* {
  auto idx = std::to_underlying(type);
  return idx < detail::kTriggerTypeNames.size()
             ? detail::kTriggerTypeNames[idx].data()
             : "manual";
}

[[nodiscard]] inline auto parse_trigger_type(std::string_view name) noexcept
    -> TriggerType {
  auto it = std::ranges::find(detail::kTriggerTypeNames, name);
  if (it != detail::kTriggerTypeNames.end()) {
    return static_cast<TriggerType>(
        std::ranges::distance(detail::kTriggerTypeNames.begin(), it));
  }
  return TriggerType::Manual;
}

[[nodiscard]] inline auto task_state_name(TaskState state) noexcept
    -> const char* {
  auto idx = std::to_underlying(state);
  return idx < detail::kTaskStateNames.size()
             ? detail::kTaskStateNames[idx].data()
             : "unknown";
}

[[nodiscard]] inline auto parse_task_state(std::string_view name) noexcept
    -> TaskState {
  auto it = std::ranges::find(detail::kTaskStateNames, name);
  if (it != detail::kTaskStateNames.end()) {
    return static_cast<TaskState>(
        std::ranges::distance(detail::kTaskStateNames.begin(), it));
  }
  return TaskState::Pending;
}

}  // namespace taskmaster
