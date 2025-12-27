#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/storage/config.hpp"

#include <functional>
#include <string_view>
#include <vector>

namespace taskmaster {

class DAGManager;

// Callback when engine triggers a task
using EngineReadyCallback = std::function<void(const TaskInstance& inst)>;

// Manages the scheduling engine
class SchedulerService {
public:
  SchedulerService() = default;
  ~SchedulerService() = default;

  SchedulerService(const SchedulerService&) = delete;
  auto operator=(const SchedulerService&) -> SchedulerService& = delete;

  auto set_on_ready(EngineReadyCallback callback) -> void;

  // Initialize from config tasks (root tasks only)
  auto init_from_config(const std::vector<TaskConfig>& tasks,
                        const std::vector<ExecutorConfig>& cfgs) -> Result<void>;

  // Register/unregister tasks dynamically
  auto register_task(std::string_view dag_id, const TaskConfig& task) -> void;
  auto unregister_task(std::string_view dag_id, std::string_view task_id)
      -> void;

  // Register/unregister DAG cron schedules
  auto register_dag_cron(std::string_view dag_id, std::string_view cron_expr)
      -> bool;
  auto unregister_dag_cron(std::string_view dag_id) -> void;

  // Task control
  [[nodiscard]] auto enable_task(std::string_view dag_id,
                                 std::string_view task_id, bool enabled) -> bool;
  [[nodiscard]] auto trigger_task(std::string_view dag_id,
                                  std::string_view task_id) -> bool;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const -> bool;

  [[nodiscard]] auto engine() -> Engine&;

private:
  Engine engine_;
  EngineReadyCallback on_ready_;
};

}  // namespace taskmaster
