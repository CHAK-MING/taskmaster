#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/scheduler/engine.hpp"
#include "taskmaster/config/config.hpp"

#include <functional>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace taskmaster {

class DAGManager;

using DAGTriggerCallback = std::move_only_function<void(const DAGId&, std::chrono::system_clock::time_point)>;

class SchedulerService {
public:
  explicit SchedulerService(Runtime& runtime);
  ~SchedulerService() = default;

  SchedulerService(const SchedulerService&) = delete;
  auto operator=(const SchedulerService&) -> SchedulerService& = delete;

  auto set_on_dag_trigger(DAGTriggerCallback callback) -> void;

  auto register_dag(DAGId dag_id, const DAGInfo& dag_info) -> void;
  auto unregister_dag(DAGId dag_id) -> void;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const -> bool;

  [[nodiscard]] auto engine() -> Engine&;

private:
  Engine engine_;
  DAGTriggerCallback on_dag_trigger_;
  std::unordered_map<DAGId, TaskId> registered_root_tasks_;
};

}  // namespace taskmaster
