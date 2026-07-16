#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"

namespace dagforge::workflow::detail {

[[nodiscard]] auto validate_checkpoint_model(
    const WorkflowCheckpoint &checkpoint) -> Result<void>;

} // namespace dagforge::workflow::detail
