#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"

#include <atomic>
#include <memory>
#include <optional>
#include <string_view>
#include <system_error>
#endif

namespace dagforge {

class ApiServer;
namespace workflow {
class ExecutorRegistry;
class WorkflowControlPlane;
class WorkflowRuntime;
class StorageDirectoryLock;
} // namespace workflow

class Application {
public:
  Application();
  explicit Application(config::SystemConfig config);
  ~Application();

  Application(const Application &) = delete;
  auto operator=(const Application &) -> Application & = delete;

  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto apply_config(config::SystemConfig config) -> Result<void>;
  [[nodiscard]] auto config() const noexcept -> const config::SystemConfig &;

  [[nodiscard]] auto init() -> Result<void>;
  [[nodiscard]] auto start() -> Result<void>;
  auto stop() noexcept -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  [[nodiscard]] auto api_server() -> ApiServer *;
  [[nodiscard]] auto api_server() const -> const ApiServer *;
  [[nodiscard]] auto runtime() -> Runtime &;
  [[nodiscard]] auto runtime() const -> const Runtime &;
  [[nodiscard]] auto workflow_runtime() -> workflow::WorkflowRuntime *;
  [[nodiscard]] auto workflow_runtime() const
      -> const workflow::WorkflowRuntime *;
  [[nodiscard]] auto workflow_control_plane()
      -> workflow::WorkflowControlPlane *;
  [[nodiscard]] auto workflow_control_plane() const
      -> const workflow::WorkflowControlPlane *;

private:
  auto rebuild_components() -> Result<void>;
  auto shutdown_components() noexcept -> void;

  std::atomic<bool> running_{false};
  config::SystemConfig config_;
  std::optional<std::error_code> initialization_error_;
  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<workflow::ExecutorRegistry> executor_registry_;
  std::unique_ptr<workflow::WorkflowControlPlane> workflow_control_plane_;
  std::unique_ptr<workflow::WorkflowRuntime> workflow_runtime_;
  std::unique_ptr<ApiServer> api_;
  std::unique_ptr<workflow::StorageDirectoryLock> storage_lock_;
};

} // namespace dagforge
