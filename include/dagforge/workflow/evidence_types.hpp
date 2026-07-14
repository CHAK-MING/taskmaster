#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstdint>
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
};

} // namespace dagforge::workflow
