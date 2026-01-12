#include "taskmaster/dag/dag_manager.hpp"

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"

#include <algorithm>
#include <mutex>
#include <queue>
#include <unordered_set>

namespace taskmaster {

DAGManager::DAGManager(Persistence* persistence) : persistence_(persistence) {
}

auto DAGManager::find_dag(DAGId dag_id) -> DAGInfo* {
  auto it = dags_.find(dag_id);
  return it != dags_.end() ? &it->second : nullptr;
}

auto DAGManager::find_dag(DAGId dag_id) const -> const DAGInfo* {
  auto it = dags_.find(dag_id);
  return it != dags_.end() ? &it->second : nullptr;
}

auto DAGManager::create_dag(DAGId dag_id, const DAGInfo& info) -> Result<void> {
  std::unique_lock lock(mu_);

  if (find_dag(dag_id)) {
    return fail(Error::AlreadyExists);
  }

  DAGInfo dag_copy = info;
  dag_copy.dag_id = dag_id;
  dag_copy.rebuild_task_index();

  if (persistence_) {
    if (auto r = persistence_->save_dag(dag_copy); !r) {
      log::error("Failed to persist DAG {}: {}", dag_id, r.error().message());
      return fail(r.error());
    }
    for (const auto& task : dag_copy.tasks) {
      if (auto r = persistence_->save_task(dag_id, task); !r) {
        log::error("Failed to persist task {} in DAG {}: {}", task.task_id, dag_id, r.error().message());
        return fail(r.error());
      }
    }
  }

  dags_.emplace(dag_id, std::move(dag_copy));

  return ok();
}

auto DAGManager::get_dag(DAGId dag_id) const -> Result<DAGInfo> {
  std::shared_lock lock(mu_);

  const auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  return ok(*dag);
}

auto DAGManager::list_dags() const -> std::vector<DAGInfo> {
  std::shared_lock lock(mu_);

  std::vector<DAGInfo> result;
  result.reserve(dags_.size());
  for (const auto& [_, dag] : dags_) {
    result.push_back(dag);
  }
  return result;
}

auto DAGManager::delete_dag(DAGId dag_id) -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (persistence_) {
    if (auto r = persistence_->delete_dag(dag_id); !r) {
      log::error("Failed to delete DAG {} from database: {}", dag_id,
                 r.error().message());
      return fail(r.error());
    }
  }

  dags_.erase(dag_id);

  log::info("Deleted DAG: {}", dag_id);
  return ok();
}

auto DAGManager::clear_all() -> void {
  std::unique_lock lock(mu_);
  dags_.clear();
}

auto DAGManager::add_task(DAGId dag_id, const TaskConfig& task)
    -> Result<void> {
  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  // O(1) check if task ID already exists
  if (dag->find_task(task.task_id)) {
    return fail(Error::AlreadyExists);
  }

  // Check for cycle using incremental DFS
  if (would_create_cycle_internal(*dag, task.task_id, task.dependencies)) {
    log::warn("Adding task {} would create a cycle in DAG {}", task.task_id, dag_id);
    return fail(Error::InvalidArgument);
  }

  // Validate dependencies exist (O(1) per dep)
  for (const auto& dep : task.dependencies) {
    if (!dag->find_task(dep)) {
      log::warn("Dependency {} not found in DAG {}", dep, dag_id);
      return fail(Error::NotFound);
    }
  }

  // Save task to database first
  if (persistence_) {
    if (auto r = persistence_->save_task(dag_id, task); !r) {
      log::error("Failed to persist task {} in DAG {}: {}", task.task_id, dag_id,
                 r.error().message());
      return fail(r.error());
    }
  }

  dag->tasks.push_back(task);
  dag->task_index[task.task_id] = dag->tasks.size() - 1;
  dag->updated_at = std::chrono::system_clock::now();
  dag->invalidate_cache();  // Invalidate reverse adjacency cache

  // log::info("Added task {} to DAG {}", task.task_id, dag_id);
  return ok();
}

auto DAGManager::update_task(DAGId dag_id, TaskId task_id,
                             const TaskConfig& task) -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  auto* existing = dag->find_task(task_id);
  if (!existing) {
    return fail(Error::NotFound);
  }

  if (would_create_cycle_internal(*dag, task_id, task.dependencies)) {
    return fail(Error::InvalidArgument);
  }

  *existing = task;
  existing->task_id = task_id;
  dag->updated_at = std::chrono::system_clock::now();
  dag->invalidate_cache();  // Invalidate reverse adjacency cache

  if (persistence_) {
    if (auto r = persistence_->save_task(dag_id, *existing); !r) {
      log::error("Failed to persist task update {} in DAG {}: {}", task_id,
                 dag_id, r.error().message());
      return fail(r.error());
    }
  }

  log::info("Updated task {} in DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::delete_task(DAGId dag_id, TaskId task_id)
    -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  // Check if any task depends on this one
  for (const auto& t : dag->tasks) {
    if (std::ranges::contains(t.dependencies, task_id)) {
      log::warn("Cannot delete task {} - task {} depends on it", task_id, t.task_id);
      return fail(Error::InvalidArgument);
    }
  }

  auto idx_it = dag->task_index.find(task_id);
  if (idx_it == dag->task_index.end()) {
    return fail(Error::NotFound);
  }

  if (persistence_) {
    if (auto r = persistence_->delete_task(dag_id, task_id); !r) {
      log::error("Failed to delete task {} from DAG {} in database: {}",
                 task_id, dag_id, r.error().message());
      return fail(r.error());
    }
  }

  // Swap-and-Pop: O(1) deletion instead of O(n) erase
  std::size_t idx = idx_it->second;
  std::size_t last_idx = dag->tasks.size() - 1;

  if (idx != last_idx) {
    // Update the index of the task being swapped
    dag->task_index[dag->tasks[last_idx].task_id] = idx;
    std::swap(dag->tasks[idx], dag->tasks[last_idx]);
  }

  dag->tasks.pop_back();
  dag->task_index.erase(idx_it);
  dag->invalidate_cache();
  dag->updated_at = std::chrono::system_clock::now();

  log::info("Deleted task {} from DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::get_task(DAGId dag_id,
                          TaskId task_id) const
    -> Result<TaskConfig> {
  std::shared_lock lock(mu_);

  const auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  const auto* task = dag->find_task(task_id);
  if (!task) {
    return fail(Error::NotFound);
  }

  return ok(*task);
}

auto DAGManager::validate_dag(DAGId dag_id) const -> Result<void> {
  auto dag_result = build_dag_graph(dag_id);
  if (!dag_result) {
    return fail(dag_result.error());
  }
  return dag_result->is_valid();
}

auto DAGManager::would_create_cycle_internal(
    const DAGInfo& dag, TaskId task_id,
    const std::vector<TaskId>& dependencies) const -> bool {
  if (dependencies.empty())
    return false;

  const auto& reverse_adj = dag.get_reverse_adj();

  std::unordered_set<TaskId> deps_set(dependencies.begin(), dependencies.end());
  std::unordered_set<TaskId> visited;
  std::queue<TaskId> q;
  q.push(task_id);

  while (!q.empty()) {
    auto current = q.front();
    q.pop();

    if (deps_set.contains(current)) {
      return true;
    }

    if (!visited.insert(current).second)
      continue;

    auto it = reverse_adj.find(current);
    if (it != reverse_adj.end()) {
      for (const auto& next : it->second) {
        q.push(next);
      }
    }
  }

  return false;
}

auto DAGManager::build_dag_graph(DAGId dag_id) const -> Result<DAG> {
  std::shared_lock lock(mu_);

  const auto* dag_info = find_dag(dag_id);
  if (!dag_info) {
    return fail(Error::NotFound);
  }

  DAG dag;

  // Add all nodes
  for (const auto& task : dag_info->tasks) {
    dag.add_node(task.task_id);
  }

  // Add all edges
  for (const auto& task : dag_info->tasks) {
    for (const auto& dep : task.dependencies) {
      if (auto r = dag.add_edge(dep, task.task_id); !r) {
        return fail(r.error());
      }
    }
  }

  return ok(std::move(dag));
}

auto DAGManager::would_create_cycle(DAGId dag_id,
                                    TaskId task_id,
                                    const std::vector<TaskId>& dependencies) const
    -> bool {
  std::shared_lock lock(mu_);
  const auto* dag = find_dag(dag_id);
  if (!dag)
    return false;
  return would_create_cycle_internal(*dag, task_id, dependencies);
}

auto DAGManager::load_from_database() -> Result<void> {
  if (!persistence_) {
    return ok();
  }

  auto dags_result = persistence_->list_dags();
  if (!dags_result) {
    log::error("Failed to load DAGs from database: {}",
               dags_result.error().message());
    return fail(dags_result.error());
  }

  std::unique_lock lock(mu_);
  for (auto& dag : *dags_result) {
    dag.rebuild_task_index();
    DAGId id = dag.dag_id;  // Copy before move
    dags_.insert_or_assign(std::move(id), std::move(dag));
  }

  log::info("Loaded {} DAGs from database", dags_result->size());
  return ok();
}

auto DAGManager::dag_count() const noexcept -> std::size_t {
  std::shared_lock lock(mu_);
  return dags_.size();
}

auto DAGManager::has_dag(DAGId dag_id) const -> bool {
  std::shared_lock lock(mu_);
  return find_dag(dag_id) != nullptr;
}

}  // namespace taskmaster
