#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include "dagforge/workflow/execution_failure.hpp"
#include "dagforge/workflow/workflow_value.hpp"
#include <glaze/json/chrono_format.hpp>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

enum class RunState : std::uint8_t {
  Running,
  Pausing,
  Paused,
  Stopping,
  Succeeded,
  Failed,
  Cancelled,
};

enum class StopIntent : std::uint8_t {
  Succeed,
  Fail,
  Cancel,
};

enum class TaskState : std::uint8_t {
  Pending,
  Ready,
  Running,
  RetryWaiting,
  Succeeded,
  Failed,
  Skipped,
  Cancelled,
};

enum class AttemptState : std::uint8_t {
  Starting,
  Running,
  Terminating,
  Succeeded,
  Failed,
  TimedOut,
  Cancelled,
};

enum class SkipReason : std::uint8_t {
  ConditionFalse,
  UpstreamFailed,
  UpstreamCancelled,
  BranchNotSelected,
};

enum class TerminationReason : std::uint8_t {
  RunCancelled,
  RunFailed,
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::RunState> {
  using E = dagforge::workflow::RunState;
  static constexpr auto value = enumerate(
      "running", E::Running, "pausing", E::Pausing, "paused", E::Paused,
      "stopping", E::Stopping, "succeeded", E::Succeeded, "failed",
      E::Failed, "cancelled", E::Cancelled);
};

template <> struct meta<dagforge::workflow::StopIntent> {
  using E = dagforge::workflow::StopIntent;
  static constexpr auto value = enumerate("succeed", E::Succeed, "fail",
                                          E::Fail, "cancel", E::Cancel);
};

template <> struct meta<dagforge::workflow::TaskState> {
  using E = dagforge::workflow::TaskState;
  static constexpr auto value = enumerate(
      "pending", E::Pending, "ready", E::Ready, "running", E::Running,
      "retry_waiting", E::RetryWaiting, "succeeded", E::Succeeded, "failed",
      E::Failed, "skipped", E::Skipped, "cancelled", E::Cancelled);
};

template <> struct meta<dagforge::workflow::AttemptState> {
  using E = dagforge::workflow::AttemptState;
  static constexpr auto value = enumerate(
      "starting", E::Starting, "running", E::Running, "terminating",
      E::Terminating, "succeeded", E::Succeeded, "failed", E::Failed,
      "timed_out", E::TimedOut, "cancelled", E::Cancelled);
};

template <> struct meta<dagforge::workflow::SkipReason> {
  using E = dagforge::workflow::SkipReason;
  static constexpr auto value = enumerate(
      "condition_false", E::ConditionFalse, "upstream_failed",
      E::UpstreamFailed, "upstream_cancelled", E::UpstreamCancelled,
      "branch_not_selected", E::BranchNotSelected);
};

template <> struct meta<dagforge::workflow::TerminationReason> {
  using E = dagforge::workflow::TerminationReason;
  static constexpr auto value = enumerate("run_cancelled", E::RunCancelled,
                                          "run_failed", E::RunFailed);
};

} // namespace glz

namespace dagforge::workflow {

struct TriggerEnvelope {
  WorkflowTriggerId trigger_id;
  WorkflowId workflow_id;
  std::string source;
  std::string event_type;
  WorkflowValue payload;
  std::string idempotency_key;
  Principal principal;
  TraceContext trace;
  std::chrono::system_clock::time_point occurred_at{
      std::chrono::system_clock::now()};
};

struct AttemptSnapshot {
  AttemptId attempt_id;
  std::uint32_t number{0};
  AttemptState state{AttemptState::Starting};
  std::optional<TerminationReason> termination_reason;
  std::optional<int> exit_code;
  std::optional<ExecutionFailure> failure;
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
};

struct TaskSnapshot {
  WorkflowNodeId node_id;
  TaskState state{TaskState::Pending};
  std::uint32_t attempt_count{0};
  std::optional<AttemptId> active_attempt_id;
  std::optional<std::chrono::system_clock::time_point> next_attempt_at;
  std::optional<SkipReason> skip_reason;
  std::optional<ExecutionFailure> failure;
  std::optional<WorkflowRunId> reused_from_run_id;
  std::vector<AttemptSnapshot> attempts;
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
};

struct RunSnapshot {
  WorkflowRunId run_id;
  WorkflowId workflow_id;
  WorkflowPlanId plan_id;
  RunState state{RunState::Running};
  std::optional<StopIntent> stop_intent;
  std::string stop_reason;
  std::optional<WorkflowRunId> parent_run_id;
  std::optional<WorkflowPlanId> parent_plan_id;
  std::uint32_t repair_revision{0};
  std::string repair_reason;
  std::vector<TaskSnapshot> tasks;
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  std::optional<ExecutionFailure> failure;
};

[[nodiscard]] constexpr auto to_string_view(RunState value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(StopIntent value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(TaskState value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(AttemptState value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(SkipReason value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(TerminationReason value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto is_terminal(RunState state) noexcept -> bool {
  return state == RunState::Succeeded || state == RunState::Failed ||
         state == RunState::Cancelled;
}

[[nodiscard]] constexpr auto is_terminal(TaskState state) noexcept -> bool {
  return state == TaskState::Succeeded || state == TaskState::Failed ||
         state == TaskState::Skipped || state == TaskState::Cancelled;
}

[[nodiscard]] constexpr auto is_terminal(AttemptState state) noexcept -> bool {
  return state == AttemptState::Succeeded || state == AttemptState::Failed ||
         state == AttemptState::TimedOut || state == AttemptState::Cancelled;
}

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::TriggerEnvelope> {
  using T = dagforge::workflow::TriggerEnvelope;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    return key == "occurred_at" ? "occurred_at_ms" : key;
  }
  static constexpr auto modify = object(
      "occurred_at_ms",
      epoch_count<std::chrono::milliseconds>(&T::occurred_at));
};

template <> struct meta<dagforge::workflow::AttemptSnapshot> {
  using T = dagforge::workflow::AttemptSnapshot;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    if (key == "created_at") {
      return "created_at_ms";
    }
    if (key == "started_at") {
      return "started_at_ms";
    }
    return key == "finished_at" ? "finished_at_ms" : key;
  }
  static constexpr auto modify = object(
      "created_at_ms", epoch_count<std::chrono::milliseconds>(&T::created_at),
      "started_at_ms", epoch_count<std::chrono::milliseconds>(&T::started_at),
      "finished_at_ms",
      epoch_count<std::chrono::milliseconds>(&T::finished_at));

  template <class V>
  static constexpr auto skip_if(V &&field, std::string_view,
                                const meta_context &) -> bool {
    if constexpr (requires { field.val; }) {
      using U = std::remove_cvref_t<decltype(field.val)>;
      if constexpr (std::is_same_v<
                        U, std::chrono::system_clock::time_point>) {
        return field.val == U{};
      }
    }
    return false;
  }
};

template <> struct meta<dagforge::workflow::TaskSnapshot> {
  using T = dagforge::workflow::TaskSnapshot;

  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    if (key == "next_attempt_at") {
      return "next_attempt_at_ms";
    }
    if (key == "started_at") {
      return "started_at_ms";
    }
    return key == "finished_at" ? "finished_at_ms" : key;
  }

  static constexpr auto read_next_attempt =
      [](T &task, std::optional<std::int64_t> epoch_millis) {
    if (!epoch_millis) {
      task.next_attempt_at.reset();
      return;
    }
    task.next_attempt_at = std::chrono::system_clock::time_point{
        std::chrono::duration_cast<std::chrono::system_clock::duration>(
            std::chrono::milliseconds{*epoch_millis})};
  };
  static constexpr auto write_next_attempt =
      [](const T &task) -> std::optional<std::int64_t> {
    if (!task.next_attempt_at) {
      return std::nullopt;
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               task.next_attempt_at->time_since_epoch())
        .count();
  };

  static constexpr auto modify = object(
      "next_attempt_at_ms", custom<read_next_attempt, write_next_attempt>,
      "started_at_ms",
      epoch_count<std::chrono::milliseconds>(&T::started_at), "finished_at_ms",
      epoch_count<std::chrono::milliseconds>(&T::finished_at));

  template <class V>
  static constexpr auto skip_if(V &&field, std::string_view,
                                const meta_context &) -> bool {
    if constexpr (requires { field.val; }) {
      using U = std::remove_cvref_t<decltype(field.val)>;
      if constexpr (std::is_same_v<
                        U, std::chrono::system_clock::time_point>) {
        return field.val == U{};
      }
    }
    return false;
  }
};

template <> struct meta<dagforge::workflow::RunSnapshot> {
  using T = dagforge::workflow::RunSnapshot;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    if (key == "created_at") {
      return "created_at_ms";
    }
    if (key == "started_at") {
      return "started_at_ms";
    }
    return key == "finished_at" ? "finished_at_ms" : key;
  }
  static constexpr auto modify = object(
      "created_at_ms", epoch_count<std::chrono::milliseconds>(&T::created_at),
      "started_at_ms", epoch_count<std::chrono::milliseconds>(&T::started_at),
      "finished_at_ms",
      epoch_count<std::chrono::milliseconds>(&T::finished_at));

  template <class V>
  static constexpr auto skip_if(V &&field, std::string_view key,
                                const meta_context &) -> bool {
    using U = std::remove_cvref_t<V>;
    if constexpr (std::is_same_v<U, std::string>) {
      return key == "repair_reason" && field.empty();
    }
    if constexpr (requires { field.val; }) {
      using Wrapped = std::remove_cvref_t<decltype(field.val)>;
      if constexpr (std::is_same_v<
                        Wrapped, std::chrono::system_clock::time_point>) {
        return field.val == Wrapped{};
      }
    }
    return false;
  }
};

} // namespace glz
