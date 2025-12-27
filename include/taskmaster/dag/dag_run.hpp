#pragma once

#include "taskmaster/dag/dag.hpp"
#include "taskmaster/scheduler/task.hpp"

#include <bitset>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace taskmaster {

enum class DAGRunState : std::uint8_t {
  Running,
  Success,
  Failed,
};

enum class TriggerType : std::uint8_t {
  Manual,
  Schedule,
  Api,
};

struct TaskInstanceInfo {
  std::string instance_id;
  NodeIndex task_idx{INVALID_NODE};
  TaskState state{TaskState::Pending};
  int attempt{0};
  std::chrono::system_clock::time_point started_at{};
  std::chrono::system_clock::time_point finished_at{};
  int exit_code{0};
  std::string error_message;
};

class DAGRun {
public:
  // Maximum number of tasks per DAG run.
  // This limit exists because we use std::bitset for O(1) state tracking.
  // For larger DAGs, consider using dynamic_bitset or vector<bool>.
  static constexpr size_t MAX_TASKS = 4096;

  DAGRun(std::string dag_run_id, const DAG& dag);

  [[nodiscard]] auto id() const noexcept -> const std::string& {
    return dag_run_id_;
  }
  [[nodiscard]] auto state() const noexcept -> DAGRunState {
    return state_;
  }
  [[nodiscard]] auto dag() const noexcept -> const DAG& {
    return dag_;
  }

  [[nodiscard]] auto get_ready_tasks() const -> std::vector<NodeIndex>;
  [[nodiscard]] auto ready_count() const noexcept -> size_t {
    return ready_count_;
  }

  auto mark_task_started(NodeIndex task_idx, std::string_view instance_id)
      -> void;
  auto mark_task_completed(NodeIndex task_idx, int exit_code) -> void;
  auto mark_task_failed(NodeIndex task_idx, std::string_view error,
                        int max_retries) -> void;

  // Initialize instance ID for a task without changing its state
  auto set_instance_id(NodeIndex task_idx, std::string_view instance_id)
      -> void;

  [[nodiscard]] auto is_complete() const noexcept -> bool;
  [[nodiscard]] auto has_failed() const noexcept -> bool;

  [[nodiscard]] auto get_task_info(NodeIndex task_idx) const
      -> std::optional<TaskInstanceInfo>;
  [[nodiscard]] auto all_task_info() const -> std::vector<TaskInstanceInfo>;

  [[nodiscard]] auto scheduled_at() const noexcept
      -> std::chrono::system_clock::time_point {
    return scheduled_at_;
  }
  [[nodiscard]] auto started_at() const noexcept
      -> std::chrono::system_clock::time_point {
    return started_at_;
  }
  [[nodiscard]] auto finished_at() const noexcept
      -> std::chrono::system_clock::time_point {
    return finished_at_;
  }

  auto set_scheduled_at(std::chrono::system_clock::time_point t) noexcept
      -> void {
    scheduled_at_ = t;
  }
  auto set_started_at(std::chrono::system_clock::time_point t) noexcept
      -> void {
    started_at_ = t;
  }
  auto set_finished_at(std::chrono::system_clock::time_point t) noexcept
      -> void {
    finished_at_ = t;
  }

  [[nodiscard]] auto trigger_type() const noexcept -> TriggerType {
    return trigger_type_;
  }
  auto set_trigger_type(TriggerType t) noexcept -> void {
    trigger_type_ = t;
  }

private:
  auto update_state() -> void;
  auto update_ready_set(NodeIndex completed_task) -> void;
  auto init_ready_set() -> void;
  auto mark_downstream_failed(NodeIndex failed_task) -> void;

  std::string dag_run_id_;
  DAG dag_;
  DAGRunState state_{DAGRunState::Running};

  std::bitset<MAX_TASKS> ready_mask_;
  std::bitset<MAX_TASKS> running_mask_;
  std::bitset<MAX_TASKS> completed_mask_;
  std::bitset<MAX_TASKS> failed_mask_;

  std::unordered_set<NodeIndex> ready_set_;
  size_t ready_count_{0};

  std::vector<int> in_degree_;
  std::vector<TaskInstanceInfo> task_info_;

  std::size_t pending_count_{0};
  std::size_t running_count_{0};
  std::size_t completed_count_{0};
  std::size_t failed_count_{0};

  std::chrono::system_clock::time_point scheduled_at_{};
  std::chrono::system_clock::time_point started_at_{};
  std::chrono::system_clock::time_point finished_at_{};
  TriggerType trigger_type_{TriggerType::Manual};
};

}  // namespace taskmaster
