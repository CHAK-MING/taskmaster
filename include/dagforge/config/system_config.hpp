#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/admission_config.hpp"
#include "dagforge/config/api_config.hpp"
#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/config/http_executor_config.hpp"
#include "dagforge/config/runtime_config.hpp"
#include "dagforge/config/storage_config.hpp"
#include "dagforge/config/workflow_config.hpp"
#endif

namespace dagforge::config {

struct ExecutorsConfig {
  CommandExecutorConfig command;
  HttpExecutorConfig http;

  auto operator==(const ExecutorsConfig &) const -> bool = default;
};

struct SystemConfig {
  WorkflowConfig workflow;
  ExecutorsConfig executors;
  AdmissionConfig admission;
  StorageConfig storage;
  RuntimeConfig runtime;
  ApiConfig api;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge::config
