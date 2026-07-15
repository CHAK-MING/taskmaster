#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
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

[[nodiscard]] constexpr auto to_string_view(EvidenceType value) noexcept
    -> std::string_view {
  switch (value) {
  case EvidenceType::TriggerReceived:
    return "trigger_received";
  case EvidenceType::PlanCompiled:
    return "plan_compiled";
  case EvidenceType::PolicyAccepted:
    return "policy_accepted";
  case EvidenceType::PolicyRejected:
    return "policy_rejected";
  case EvidenceType::TaskStarted:
    return "task_started";
  case EvidenceType::TaskCompleted:
    return "task_completed";
  case EvidenceType::TaskFailed:
    return "task_failed";
  case EvidenceType::RunPauseRequested:
    return "run_pause_requested";
  case EvidenceType::RunPaused:
    return "run_paused";
  case EvidenceType::RunResumed:
    return "run_resumed";
  case EvidenceType::RunStopRequested:
    return "run_stop_requested";
  case EvidenceType::AttemptStarted:
    return "attempt_started";
  case EvidenceType::AttemptCompleted:
    return "attempt_completed";
  case EvidenceType::Checkpoint:
    return "checkpoint";
  case EvidenceType::RunCompleted:
    return "run_completed";
  case EvidenceType::RunFailed:
    return "run_failed";
  case EvidenceType::RunCancelled:
    return "run_cancelled";
  case EvidenceType::RunRecoveryResumed:
    return "run_recovery_resumed";
  case EvidenceType::RepairRunStarted:
    return "repair_run_started";
  case EvidenceType::TaskReused:
    return "task_reused";
  case EvidenceType::TaskInvalidated:
    return "task_invalidated";
  }
  return "unknown";
}

} // namespace dagforge::workflow
