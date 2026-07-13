#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/executor/executor.hpp"
#endif

namespace dagforge {

struct SandboxRequest {
  InstanceId instance_id;
  CommandExecutorConfig command;
  std::chrono::seconds timeout{std::chrono::seconds(3600)};
  std::shared_ptr<pmr::memory_resource> memory_resource;
};

struct SandboxEvents {
  ExecutorHeartbeatCallback on_heartbeat;
  std::move_only_function<void(const InstanceId &, std::string_view)> on_state;
  std::move_only_function<void(const InstanceId &, std::string_view)> on_stdout;
  std::move_only_function<void(const InstanceId &, std::string_view)> on_stderr;
  std::move_only_function<void(const InstanceId &, ExecutorResult)> on_complete;
};

class ISandboxBackend {
public:
  virtual ~ISandboxBackend() = default;

  virtual auto launch(SandboxRequest request, SandboxEvents events)
      -> Result<void> = 0;
  virtual auto terminate(const InstanceId &instance_id) -> void = 0;
};

[[nodiscard]] auto create_minijail_sandbox_backend(Runtime &runtime,
                                                    SandboxConfig config)
    -> std::unique_ptr<ISandboxBackend>;

} // namespace dagforge
