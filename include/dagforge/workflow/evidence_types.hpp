#pragma once

#include "dagforge/util/enum.hpp"
#include <glaze/core/common.hpp>
#include <cstdint>
#include <string_view>

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

namespace dagforge::util {

template <> struct EnumTraits<workflow::EvidenceType> {
  using E = workflow::EvidenceType;
  inline static constexpr std::array<EnumEntry<E>, 21> entries{{
      {"trigger_received", E::TriggerReceived},
      {"plan_compiled", E::PlanCompiled},
      {"policy_accepted", E::PolicyAccepted},
      {"policy_rejected", E::PolicyRejected},
      {"task_started", E::TaskStarted},
      {"task_completed", E::TaskCompleted},
      {"task_failed", E::TaskFailed},
      {"run_pause_requested", E::RunPauseRequested},
      {"run_paused", E::RunPaused},
      {"run_resumed", E::RunResumed},
      {"run_stop_requested", E::RunStopRequested},
      {"attempt_started", E::AttemptStarted},
      {"attempt_completed", E::AttemptCompleted},
      {"checkpoint", E::Checkpoint},
      {"run_completed", E::RunCompleted},
      {"run_failed", E::RunFailed},
      {"run_cancelled", E::RunCancelled},
      {"run_recovery_resumed", E::RunRecoveryResumed},
      {"repair_run_started", E::RepairRunStarted},
      {"task_reused", E::TaskReused},
      {"task_invalidated", E::TaskInvalidated},
  }};
  static_assert(enum_entries_are_valid(entries));
};

} // namespace dagforge::util

namespace glz {

template <> struct meta<dagforge::workflow::EvidenceType> {
  using E = dagforge::workflow::EvidenceType;
  static constexpr auto keys = dagforge::util::enum_names<E>();
  static constexpr auto value = dagforge::util::enum_values<E>();
};

} // namespace glz

namespace dagforge::workflow {

[[nodiscard]] constexpr auto to_string_view(EvidenceType value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

} // namespace dagforge::workflow
