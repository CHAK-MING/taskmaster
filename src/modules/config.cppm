module;

#include "dagforge/config/admission_config.hpp"
#include "dagforge/config/api_config.hpp"
#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/config/http_executor_config.hpp"
#include "dagforge/config/runtime_config.hpp"
#include "dagforge/config/storage_config.hpp"
#include "dagforge/config/system_config.hpp"
#include "dagforge/config/workflow_config.hpp"

export module dagforge.config;

export namespace dagforge::config {
using ::dagforge::config::AdmissionConfig;
using ::dagforge::config::ApiConfig;
using ::dagforge::config::CommandExecutorConfig;
using ::dagforge::config::CommandPolicyConfig;
using ::dagforge::config::CommandProgramConfig;
using ::dagforge::config::ExecutorsConfig;
using ::dagforge::config::HttpEgressConfig;
using ::dagforge::config::HttpExecutorConfig;
using ::dagforge::config::MinijailConfig;
using ::dagforge::config::RuntimeConfig;
using ::dagforge::config::StorageConfig;
using ::dagforge::config::SystemConfig;
using ::dagforge::config::WorkflowConfig;
} // namespace dagforge::config
