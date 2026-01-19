#pragma once

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/util/id.hpp"


#include <flat_map>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

namespace taskmaster {

class Persistence;

struct DAGInfo {
  DAGId dag_id;
  std::string name;
  std::string description;
  std::string cron;
  int max_concurrent_runs{1};
  std::optional<std::chrono::system_clock::time_point> start_date;
  std::optional<std::chrono::system_clock::time_point> end_date;
  bool catchup{false};
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point updated_at;
  std::vector<TaskConfig> tasks;
  std::flat_map<TaskId, std::size_t, std::less<>> task_index;
  mutable std::flat_map<TaskId, std::vector<TaskId>, std::less<>>
      reverse_adj_cache;
  mutable bool reverse_adj_dirty{true};

  auto rebuild_task_index() -> void {
    task_index.clear();
    for (auto [i, task] : tasks | std::views::enumerate) {
      task_index.emplace(task.task_id, static_cast<std::size_t>(i));
    }
    reverse_adj_dirty = true;
  }

  auto rebuild_reverse_adj() const -> void {
    reverse_adj_cache.clear();
    for (const auto& task : tasks) {
      for (const auto& dep : task.dependencies) {
        reverse_adj_cache[dep.task_id].push_back(task.task_id);
      }
    }
    reverse_adj_dirty = false;
  }

  [[nodiscard]] auto get_reverse_adj() const
      -> const std::flat_map<TaskId, std::vector<TaskId>, std::less<>>& {
    if (reverse_adj_dirty) {
      rebuild_reverse_adj();
    }
    return reverse_adj_cache;
  }

  [[nodiscard]] auto find_task(TaskId task_id) -> TaskConfig* {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }

  [[nodiscard]] auto find_task(TaskId task_id) const
      -> const TaskConfig* {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }

  auto invalidate_cache() -> void {
    reverse_adj_dirty = true;
  }
};

class DAGManager {
public:
  explicit DAGManager(Persistence* persistence = nullptr);
  ~DAGManager() = default;

  DAGManager(const DAGManager&) = delete;
  auto operator=(const DAGManager&) -> DAGManager& = delete;
  DAGManager(DAGManager&&) = delete;
  auto operator=(DAGManager&&) -> DAGManager& = delete;

  auto set_persistence(Persistence* persistence) -> void {
    persistence_ = persistence;
  }

  // DAG CRUD
  [[nodiscard]] auto create_dag(DAGId dag_id, const DAGInfo& info) -> Result<void>;
  [[nodiscard]] auto get_dag(DAGId dag_id) const -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() const -> std::vector<DAGInfo>;
  [[nodiscard]] auto delete_dag(DAGId dag_id) -> Result<void>;
  auto clear_all() -> void;

  // Task CRUD within DAG
  [[nodiscard]] auto add_task(DAGId dag_id, const TaskConfig& task)
      -> Result<void>;
  [[nodiscard]] auto update_task(DAGId dag_id,
                                 TaskId task_id,
                                 const TaskConfig& task) -> Result<void>;
  [[nodiscard]] auto delete_task(DAGId dag_id,
                                 TaskId task_id) -> Result<void>;
  [[nodiscard]] auto get_task(DAGId dag_id,
                              TaskId task_id) const
      -> Result<TaskConfig>;

  // Validation
  [[nodiscard]] auto validate_dag(DAGId dag_id) const
      -> Result<void>;
  [[nodiscard]] auto
  would_create_cycle(DAGId dag_id, TaskId task_id,
                     const std::vector<TaskId>& dependencies) const -> bool;

  // Build DAG graph for execution
  [[nodiscard]] auto build_dag_graph(DAGId dag_id) const
      -> Result<DAG>;


  // Persistence
  auto load_from_database() -> Result<void>;

  [[nodiscard]] auto dag_count() const noexcept -> std::size_t;
  [[nodiscard]] auto has_dag(DAGId dag_id) const -> bool;

private:
  [[nodiscard]] auto generate_dag_id() const -> DAGId;
  [[nodiscard]] auto find_dag(DAGId dag_id) -> DAGInfo*;
  [[nodiscard]] auto find_dag(DAGId dag_id) const -> const DAGInfo*;
  [[nodiscard]] auto
  would_create_cycle_internal(const DAGInfo& dag, TaskId task_id,
                              const std::vector<TaskId>& dependencies) const
      -> bool;

  mutable std::shared_mutex mu_;
  std::flat_map<DAGId, DAGInfo, std::less<>> dags_;
  Persistence* persistence_;
};

}  // namespace taskmaster
