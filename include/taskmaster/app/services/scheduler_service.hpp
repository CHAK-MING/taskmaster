#pragma once

#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/scheduler/engine.hpp"

#include <functional>
#include <unordered_map>
#include <thread>

namespace taskmaster {

class DAGManager;
class Persistence;

  using DAGTriggerCallback = std::move_only_function<void(const DAGId&, std::chrono::system_clock::time_point)>;
  using CheckExistsCallback = Engine::CheckExistsCallback;

  class SchedulerService {
  public:
    explicit SchedulerService(Runtime& runtime, Persistence* persistence = nullptr);
    ~SchedulerService() = default;

    SchedulerService(const SchedulerService&) = delete;
    auto operator=(const SchedulerService&) -> SchedulerService& = delete;

    auto set_on_dag_trigger(DAGTriggerCallback callback) -> void;
    auto set_check_exists_callback(CheckExistsCallback callback) -> void;

    auto register_dag(DAGId dag_id, const DAGInfo& dag_info) -> void;

  auto unregister_dag(DAGId dag_id) -> void;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const -> bool;

  [[nodiscard]] auto engine() -> Engine&;

private:
  Runtime& runtime_;
  Engine engine_;
  std::jthread thread_;
  DAGTriggerCallback on_dag_trigger_;
  std::unordered_map<DAGId, TaskId> registered_root_tasks_;
};

}  // namespace taskmaster
