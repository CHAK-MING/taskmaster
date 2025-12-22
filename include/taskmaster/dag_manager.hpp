#pragma once

#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "taskmaster/config.hpp"
#include "taskmaster/dag.hpp"
#include "taskmaster/error.hpp"

namespace taskmaster {

class Persistence;

struct DAGInfo {
  std::string id;
  std::string name;
  std::string description;
  std::chrono::system_clock::time_point created_at;
  std::chrono::system_clock::time_point updated_at;
  std::vector<TaskConfig> tasks;
  std::unordered_map<std::string, std::size_t, StringHash, std::equal_to<>>
      task_index;
  bool from_config{false};

  auto rebuild_task_index() -> void {
    task_index.clear();
    for (std::size_t i = 0; i < tasks.size(); ++i) {
      task_index[tasks[i].id] = i;
    }
  }

  [[nodiscard]] auto find_task(std::string_view task_id) -> TaskConfig * {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }

  [[nodiscard]] auto find_task(std::string_view task_id) const
      -> const TaskConfig * {
    auto it = task_index.find(task_id);
    return it != task_index.end() ? &tasks[it->second] : nullptr;
  }
};

class DAGManager {
public:
  explicit DAGManager(Persistence *persistence = nullptr);
  ~DAGManager() = default;

  DAGManager(const DAGManager &) = delete;
  auto operator=(const DAGManager &) -> DAGManager & = delete;
  DAGManager(DAGManager &&) = delete;
  auto operator=(DAGManager &&) -> DAGManager & = delete;

  auto set_persistence(Persistence *persistence) -> void {
    persistence_ = persistence;
  }

  // DAG CRUD
  [[nodiscard]] auto create_dag(std::string_view name,
                                std::string_view description = "")
      -> Result<std::string>;
  [[nodiscard]] auto get_dag(std::string_view dag_id) const -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() const -> std::vector<DAGInfo>;
  [[nodiscard]] auto delete_dag(std::string_view dag_id) -> Result<void>;
  [[nodiscard]] auto update_dag(std::string_view dag_id, std::string_view name,
                                std::string_view description) -> Result<void>;

  // Task CRUD within DAG
  [[nodiscard]] auto add_task(std::string_view dag_id, const TaskConfig &task)
      -> Result<void>;
  [[nodiscard]] auto update_task(std::string_view dag_id,
                                 std::string_view task_id,
                                 const TaskConfig &task) -> Result<void>;
  [[nodiscard]] auto delete_task(std::string_view dag_id,
                                 std::string_view task_id) -> Result<void>;
  [[nodiscard]] auto get_task(std::string_view dag_id,
                              std::string_view task_id) const
      -> Result<TaskConfig>;

  // Validation
  [[nodiscard]] auto validate_dag(std::string_view dag_id) const
      -> Result<void>;
  [[nodiscard]] auto
  would_create_cycle(std::string_view dag_id, std::string_view task_id,
                     const std::vector<std::string> &deps) const -> bool;

  // Build DAG graph for execution
  [[nodiscard]] auto build_dag_graph(std::string_view dag_id) const
      -> Result<DAG>;

  // Load from config (marks as read-only)
  auto load_from_config(const Config &config) -> Result<void>;

  // Persistence
  auto load_from_database() -> Result<void>;
  auto save_to_database() -> Result<void>;

  [[nodiscard]] auto dag_count() const noexcept -> std::size_t;
  [[nodiscard]] auto has_dag(std::string_view dag_id) const -> bool;

private:
  [[nodiscard]] auto generate_dag_id() const -> std::string;
  [[nodiscard]] auto find_dag(std::string_view dag_id) -> DAGInfo *;
  [[nodiscard]] auto find_dag(std::string_view dag_id) const -> const DAGInfo *;
  [[nodiscard]] auto
  would_create_cycle_internal(const DAGInfo &dag, std::string_view task_id,
                              const std::vector<std::string> &deps) const
      -> bool;

  mutable std::shared_mutex mu_;
  std::unordered_map<std::string, DAGInfo> dags_;
  Persistence *persistence_;
};

} // namespace taskmaster
