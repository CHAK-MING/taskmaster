#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/workflow/executor_registry.hpp"

#include <memory>
#endif

namespace dagforge::workflow {

[[nodiscard]] auto create_http_executor_adapter(Runtime &runtime,
                                                HttpExecutorConfig config)
    -> Result<std::shared_ptr<ITaskExecutor>>;

} // namespace dagforge::workflow
