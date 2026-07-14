#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/http_executor_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/workflow/task_executor.hpp"

#include <memory>
#endif

namespace dagforge::executors::http {

[[nodiscard]] auto create_task_executor(
    Runtime &runtime, const config::HttpEgressConfig &config)
    -> Result<std::shared_ptr<workflow::ITaskExecutor>>;

} // namespace dagforge::executors::http
