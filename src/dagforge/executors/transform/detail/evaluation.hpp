#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include <chrono>
#include <stop_token>

namespace dagforge::executors::transform::detail {

[[nodiscard]] auto describe_transform()
    -> Result<workflow::ExecutorDescription>;

[[nodiscard]] auto compile_transform(JsonPayload config,
                                     workflow::ExecutorCompileContext context)
    -> workflow::ExecutorCompileResult<workflow::CompiledExecutorConfig>;

[[nodiscard]] auto
validate_transform_request(const workflow::TaskExecutionRequest &request)
    -> Result<void>;

[[nodiscard]] auto
evaluate_transform(const workflow::TaskExecutionRequest &request,
                   std::stop_token stop_token,
                   std::chrono::steady_clock::time_point accepted_at)
    -> workflow::TaskExecutionResult;

} // namespace dagforge::executors::transform::detail
