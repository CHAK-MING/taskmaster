#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <chrono>
#include <cstdint>
#include <optional>

namespace dagforge::workflow::detail {

[[nodiscard]] auto next_retry_delay(
    const NodePlan &node, Error failure, std::uint32_t attempt_number,
    const WorkflowRunId &run_id, const WorkflowNodeId &node_id)
    -> std::optional<std::chrono::milliseconds>;

} // namespace dagforge::workflow::detail
