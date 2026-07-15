#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/workflow/execution_failure.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

struct AttemptFailureReport {
  AttemptId attempt_id;
  std::uint32_t number{0};
  AttemptState state{AttemptState::Failed};
  std::optional<FailureClass> failure_class;
  std::optional<TerminationReason> termination_reason;
  ExecutionFailure failure;
};

struct TaskFailureReport {
  WorkflowNodeId node_id;
  TaskState state{TaskState::Failed};
  std::optional<WorkflowRunId> reused_from_run_id;
  std::optional<ExecutionFailure> failure;
  std::vector<AttemptFailureReport> attempts;
};

struct RunFailureReport {
  WorkflowRunId run_id;
  WorkflowId workflow_id;
  WorkflowPlanId plan_id;
  RunState state{RunState::Failed};
  std::optional<WorkflowRunId> parent_run_id;
  std::optional<WorkflowPlanId> parent_plan_id;
  std::uint32_t repair_revision{0};
  std::optional<ExecutionFailure> failure;
  std::vector<TaskFailureReport> tasks;
};

struct RepairRequest {
  std::string reason;
  std::string idempotency_key;
};

struct RepairNodeDecision {
  WorkflowNodeId node_id;
  bool reused{false};
  std::string reason;
};

struct RepairStartResult {
  WorkflowRunId run_id;
  WorkflowRunId parent_run_id;
  WorkflowPlanId plan_id;
  std::vector<RepairNodeDecision> nodes;
};

} // namespace dagforge::workflow
