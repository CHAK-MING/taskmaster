#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include "taskmaster/error.hpp"

namespace taskmaster {

// Forward declarations - avoid including heavy headers
class ApiServer;
class DAG;
class DAGManager;
class DAGRun;
class Engine;
class IExecutor;
class Persistence;
struct Config;
struct TaskConfig;
struct TaskInstance;
using NodeIndex = std::uint32_t;

class Application {
public:
  Application();
  explicit Application(std::string_view db_path);
  ~Application();

  Application(const Application &) = delete;
  auto operator=(const Application &) -> Application & = delete;

  [[nodiscard]] auto load_config(std::string_view path) -> Result<void>;
  [[nodiscard]] auto load_config_string(std::string_view json_str)
      -> Result<void>;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto trigger_dag(std::string_view dag_id) -> void;
  auto trigger_dag_by_id(std::string_view dag_id) -> void;
  auto wait_for_completion(int timeout_ms = 60000) -> void;
  [[nodiscard]] auto has_active_runs() const -> bool;
  auto list_tasks() const -> void;
  auto show_status() const -> void;

  [[nodiscard]] auto config() const noexcept -> const Config &;
  [[nodiscard]] auto config() noexcept -> Config &;

  [[nodiscard]] auto dag_manager() -> DAGManager &;
  [[nodiscard]] auto dag_manager() const -> const DAGManager &;

  auto register_task_with_engine(std::string_view dag_id,
                                 const TaskConfig &task) -> void;
  auto unregister_task_from_engine(std::string_view dag_id,
                                   std::string_view task_id) -> void;

  [[nodiscard]] auto engine() -> Engine &;

  [[nodiscard]] auto recover_from_crash() -> Result<void>;

  [[nodiscard]] auto api_server() -> ApiServer *;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace taskmaster
