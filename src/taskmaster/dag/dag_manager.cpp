#include "taskmaster/dag/dag_manager.hpp"

#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/log.hpp"
#include "taskmaster/util/util.hpp"

#include <algorithm>
#include <mutex>
#include <queue>
#include <ranges>
#include <unordered_set>

namespace taskmaster {

DAGManager::DAGManager(Persistence* persistence) : persistence_(persistence) {
}

auto DAGManager::generate_dag_id() const -> std::string {
  return "dag_" + generate_uuid().substr(0, 8);
}

auto DAGManager::find_dag(std::string_view dag_id) -> DAGInfo* {
  auto it = dags_.find(std::string(dag_id));
  return it != dags_.end() ? &it->second : nullptr;
}

auto DAGManager::find_dag(std::string_view dag_id) const -> const DAGInfo* {
  auto it = dags_.find(std::string(dag_id));
  return it != dags_.end() ? &it->second : nullptr;
}

auto DAGManager::create_dag(std::string_view name, std::string_view description)
    -> Result<std::string> {
  std::unique_lock lock(mu_);

  std::string dag_id = generate_dag_id();
  auto now = std::chrono::system_clock::now();

  DAGInfo info;
  info.id = dag_id;
  info.name = std::string(name);
  info.description = std::string(description);
  info.created_at = now;
  info.updated_at = now;

  if (persistence_) {
    if (auto r = persistence_->save_dag(info); !r) {
      log::error("Failed to persist DAG {}: {}", dag_id, r.error().message());
      return fail(r.error());
    }
  }

  dags_[dag_id] = std::move(info);
  log::info("Created DAG: {} ({})", dag_id, name);

  return ok(dag_id);
}

auto DAGManager::get_dag(std::string_view dag_id) const -> Result<DAGInfo> {
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

auto DAGManager::delete_dag(std::string_view dag_id) -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (dag->from_config) {
    log::warn("Cannot delete DAG {} loaded from config", dag_id);
    return fail(Error::InvalidArgument);
  }

  if (persistence_) {
    if (auto r = persistence_->delete_dag(dag_id); !r) {
      log::error("Failed to delete DAG {} from database: {}", dag_id,
                 r.error().message());
      return fail(r.error());
    }
  }

  dags_.erase(std::string(dag_id));
  log::info("Deleted DAG: {}", dag_id);
  return ok();
}

auto DAGManager::update_dag(std::string_view dag_id, std::string_view name,
                            std::string_view description, std::string_view cron,
                            int max_concurrent_runs, int is_active)
    -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (dag->from_config) {
    log::warn("Cannot update DAG {} loaded from config", dag_id);
    return fail(Error::InvalidArgument);
  }

  if (!name.empty()) {
    dag->name = std::string(name);
  }
  if (!description.empty()) {
    dag->description = std::string(description);
  }
  if (!cron.empty()) {
    dag->cron = std::string(cron);
  }
  if (max_concurrent_runs >= 0) {
    dag->max_concurrent_runs = max_concurrent_runs;
  }
  if (is_active >= 0) {
    dag->is_active = (is_active != 0);
  }
  dag->updated_at = std::chrono::system_clock::now();

  if (persistence_) {
    if (auto r = persistence_->save_dag(*dag); !r) {
      log::error("Failed to persist DAG update {}: {}", dag_id,
                 r.error().message());
      return fail(r.error());
    }
  }

  log::info("Updated DAG: {}", dag_id);
  return ok();
}

auto DAGManager::add_task(std::string_view dag_id, const TaskConfig& task)
    -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (dag->from_config) {
    log::warn("Cannot add task to DAG {} loaded from config", dag_id);
    return fail(Error::InvalidArgument);
  }

  // O(1) check if task ID already exists
  if (dag->find_task(task.id)) {
    return fail(Error::AlreadyExists);
  }

  // Check for cycle using incremental DFS
  if (would_create_cycle_internal(*dag, task.id, task.deps)) {
    log::warn("Adding task {} would create a cycle in DAG {}", task.id, dag_id);
    return fail(Error::InvalidArgument);
  }

  // Validate dependencies exist (O(1) per dep)
  for (const auto& dep : task.deps) {
    if (!dag->find_task(dep)) {
      log::warn("Dependency {} not found in DAG {}", dep, dag_id);
      return fail(Error::NotFound);
    }
  }

  // Save task to database first
  if (persistence_) {
    if (auto r = persistence_->save_task(dag_id, task); !r) {
      log::error("Failed to persist task {} in DAG {}: {}", task.id, dag_id,
                 r.error().message());
      return fail(r.error());
    }
  }

  dag->tasks.push_back(task);
  dag->task_index[task.id] = dag->tasks.size() - 1;
  dag->updated_at = std::chrono::system_clock::now();
  dag->invalidate_cache();  // Invalidate reverse adjacency cache

  log::info("Added task {} to DAG {}", task.id, dag_id);
  return ok();
}

auto DAGManager::update_task(std::string_view dag_id, std::string_view task_id,
                             const TaskConfig& task) -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (dag->from_config) {
    return fail(Error::InvalidArgument);
  }

  auto* existing = dag->find_task(task_id);
  if (!existing) {
    return fail(Error::NotFound);
  }

  if (would_create_cycle_internal(*dag, std::string(task_id), task.deps)) {
    return fail(Error::InvalidArgument);
  }

  *existing = task;
  existing->id = std::string(task_id);
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

auto DAGManager::delete_task(std::string_view dag_id, std::string_view task_id)
    -> Result<void> {
  std::unique_lock lock(mu_);

  auto* dag = find_dag(dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  if (dag->from_config) {
    return fail(Error::InvalidArgument);
  }

  // Check if any task depends on this one
  for (const auto& t : dag->tasks) {
    if (std::ranges::contains(t.deps, task_id)) {
      log::warn("Cannot delete task {} - task {} depends on it", task_id, t.id);
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

  std::size_t idx = idx_it->second;
  dag->tasks.erase(dag->tasks.begin() + static_cast<std::ptrdiff_t>(idx));
  dag->rebuild_task_index();  // This also invalidates the cache
  dag->updated_at = std::chrono::system_clock::now();

  log::info("Deleted task {} from DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::get_task(std::string_view dag_id,
                          std::string_view task_id) const
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

auto DAGManager::validate_dag(std::string_view dag_id) const -> Result<void> {
  auto dag_result = build_dag_graph(dag_id);
  if (!dag_result) {
    return fail(dag_result.error());
  }
  return dag_result->is_valid();
}

auto DAGManager::would_create_cycle_internal(
    const DAGInfo& dag, std::string_view task_id,
    const std::vector<std::string>& deps) const -> bool {
  if (deps.empty())
    return false;

  // Use cached reverse adjacency list (const_cast is safe here as we're only
  // reading/rebuilding the cache, not modifying the logical state)
  const auto& reverse_adj = const_cast<DAGInfo&>(dag).get_reverse_adj();

  std::unordered_set<std::string> deps_set(deps.begin(), deps.end());
  std::unordered_set<std::string> visited;
  std::queue<std::string> q;
  q.push(std::string(task_id));

  while (!q.empty()) {
    std::string current = q.front();
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

auto DAGManager::build_dag_graph(std::string_view dag_id) const -> Result<DAG> {
  std::shared_lock lock(mu_);

  const auto* dag_info = find_dag(dag_id);
  if (!dag_info) {
    return fail(Error::NotFound);
  }

  DAG dag;

  // Add all nodes
  for (const auto& task : dag_info->tasks) {
    dag.add_node(task.id);
  }

  // Add all edges
  for (const auto& task : dag_info->tasks) {
    for (const auto& dep : task.deps) {
      if (auto r = dag.add_edge(dep, task.id); !r) {
        return fail(r.error());
      }
    }
  }

  return ok(std::move(dag));
}

auto DAGManager::would_create_cycle(std::string_view dag_id,
                                    std::string_view task_id,
                                    const std::vector<std::string>& deps) const
    -> bool {
  std::shared_lock lock(mu_);
  const auto* dag = find_dag(dag_id);
  if (!dag)
    return false;
  return would_create_cycle_internal(*dag, task_id, deps);
}

auto DAGManager::load_from_config(const Config& config) -> Result<void> {
  std::unique_lock lock(mu_);

  if (config.tasks.empty()) {
    return ok();
  }

  // Create a single DAG from config tasks
  std::string dag_id = "config_dag";
  auto now = std::chrono::system_clock::now();

  DAGInfo info;
  info.id = dag_id;
  info.name = "Config DAG";
  info.description = "DAG loaded from config file (read-only)";
  info.created_at = now;
  info.updated_at = now;
  info.tasks = config.tasks;
  info.from_config = true;
  info.rebuild_task_index();

  dags_[dag_id] = std::move(info);
  log::info("Loaded {} tasks from config into DAG {}", config.tasks.size(),
            dag_id);

  return ok();
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
    // Skip config DAGs (they will be loaded separately)
    if (dag.from_config) {
      continue;
    }
    dag.rebuild_task_index();
    dags_[dag.id] = std::move(dag);
  }

  log::info("Loaded {} DAGs from database", dags_result->size());
  return ok();
}

auto DAGManager::save_to_database() -> Result<void> {
  if (!persistence_) {
    return ok();
  }

  std::shared_lock lock(mu_);
  for (const auto& [_, dag] : dags_) {
    // Skip config DAGs
    if (dag.from_config) {
      continue;
    }
    if (auto r = persistence_->save_dag(dag); !r) {
      log::error("Failed to save DAG {} to database: {}", dag.id,
                 r.error().message());
      return fail(r.error());
    }
  }

  log::info("Saved {} DAGs to database", dags_.size());
  return ok();
}

auto DAGManager::dag_count() const noexcept -> std::size_t {
  std::shared_lock lock(mu_);
  return dags_.size();
}

auto DAGManager::has_dag(std::string_view dag_id) const -> bool {
  std::shared_lock lock(mu_);
  return find_dag(dag_id) != nullptr;
}

}  // namespace taskmaster
