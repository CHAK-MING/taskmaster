#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/artifact_store.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_recovery.hpp"

#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow::detail {

struct RepairPlan {
  std::vector<RepairNodeDecision> decisions;
  std::unordered_set<std::string> reused_nodes;
  std::vector<std::pair<OutputRef, WorkflowValue>> values;
};

[[nodiscard]] auto plan_repair(const ExecutionPlan &revised,
                               const WorkflowCheckpoint &parent,
                               const IArtifactStore &artifacts)
    -> Result<RepairPlan>;

} // namespace dagforge::workflow::detail
