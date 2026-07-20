#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include <memory>
#endif

namespace dagforge::executors::transform {

[[nodiscard]] auto create_task_executor(Runtime &runtime)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>>;

} // namespace dagforge::executors::transform
