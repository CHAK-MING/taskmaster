#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/dag/graph_types.hpp"
#include "dagforge/scheduler/task_state.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <chrono>
#include <cstdint>
#include <string>
#endif

namespace dagforge {

enum class DAGRunState : std::uint8_t {
  Queued,
  Running,
  Success,
  Failed,
  Skipped,
  Cancelled,
};

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::DAGRunState> {
  static constexpr auto value = glz::enumerate(
      "queued", dagforge::DAGRunState::Queued, "running",
      dagforge::DAGRunState::Running, "success",
      dagforge::DAGRunState::Success, "failed", dagforge::DAGRunState::Failed,
      "skipped", dagforge::DAGRunState::Skipped, "cancelled",
      dagforge::DAGRunState::Cancelled);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(DAGRunState value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<DAGRunState>(std::string_view s) noexcept
    -> DAGRunState {
  return ::dagforge::util::parse_enum(s, DAGRunState::Running);
}

enum class TriggerType : std::uint8_t {
  Manual,
  Schedule,
  Api,
  Backfill,
};

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::TriggerType> {
  static constexpr auto value = glz::enumerate(
      "manual", dagforge::TriggerType::Manual, "schedule",
      dagforge::TriggerType::Schedule, "api", dagforge::TriggerType::Api,
      "backfill", dagforge::TriggerType::Backfill);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(TriggerType value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<TriggerType>(std::string_view s) noexcept
    -> TriggerType {
  return ::dagforge::util::parse_enum(s, TriggerType::Manual);
}

struct TaskInstanceInfo {
  InstanceId instance_id;
  TaskId task_id;
  NodeIndex task_idx{kInvalidNode};
  std::int64_t task_rowid{0};
  TaskState state{TaskState::Pending};
  int attempt{0};
  std::chrono::system_clock::time_point started_at;
  std::chrono::system_clock::time_point finished_at;
  int exit_code{0};
  std::string error_message;
  std::string error_type;
  std::int64_t run_rowid{-1};
};

struct TaskInstanceKey {
  std::int64_t run_rowid{-1};
  std::int64_t task_rowid{0};
  int attempt{0};

  [[nodiscard]] auto valid() const noexcept -> bool {
    return run_rowid > 0 && task_rowid > 0 && attempt > 0;
  }
};

} // namespace dagforge
