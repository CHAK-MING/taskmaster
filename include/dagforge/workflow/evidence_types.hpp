#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include <cstdint>
#include <string_view>
#endif

namespace dagforge::workflow {

enum class EvidenceType : std::uint8_t {
  TriggerReceived,
  PlanCompiled,
  PolicyAccepted,
  PolicyRejected,
  TaskStarted,
  TaskCompleted,
  TaskFailed,
  RunPauseRequested,
  RunPaused,
  RunResumed,
  RunStopRequested,
  AttemptStarted,
  AttemptCompleted,
  Checkpoint,
  RunCompleted,
  RunFailed,
  RunCancelled,
  RunRecoveryResumed,
  RepairRunStarted,
  TaskReused,
  TaskInvalidated,
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::EvidenceType> {
  using E = dagforge::workflow::EvidenceType;
  static constexpr auto value = enumerate(
      "trigger_received", E::TriggerReceived, "plan_compiled",
      E::PlanCompiled, "policy_accepted", E::PolicyAccepted,
      "policy_rejected", E::PolicyRejected, "task_started", E::TaskStarted,
      "task_completed", E::TaskCompleted, "task_failed", E::TaskFailed,
      "run_pause_requested", E::RunPauseRequested, "run_paused",
      E::RunPaused, "run_resumed", E::RunResumed, "run_stop_requested",
      E::RunStopRequested, "attempt_started", E::AttemptStarted,
      "attempt_completed", E::AttemptCompleted, "checkpoint", E::Checkpoint,
      "run_completed", E::RunCompleted, "run_failed", E::RunFailed,
      "run_cancelled", E::RunCancelled, "run_recovery_resumed",
      E::RunRecoveryResumed, "repair_run_started", E::RepairRunStarted,
      "task_reused", E::TaskReused, "task_invalidated", E::TaskInvalidated);
};

} // namespace glz

namespace dagforge::workflow {

[[nodiscard]] constexpr auto to_string_view(EvidenceType value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

} // namespace dagforge::workflow
