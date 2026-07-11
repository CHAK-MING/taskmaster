#include "dagforge/dag/dag_manager.hpp"

#include "dagforge/app/config_builder.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/util/log.hpp"


#include <algorithm>
#include <queue>
#include <ranges>
#include <type_traits>
#include <unordered_set>

namespace dagforge {

namespace {

using DagMap = ankerl::unordered_dense::map<DAGId, DAGInfo>;

template <typename T, typename... Args>
auto log_result_error(const Result<T> &result,
                      std::format_string<Args...> fmt,
                      Args &&...args) -> void {
  if (result) {
    return;
  }
  log::error("{}: {}", std::format(fmt, std::forward<Args>(args)...),
             result.error().message());
}

template <typename Map>
[[nodiscard]] auto find_dag_in(Map &dags, const DAGId &dag_id)
    -> std::conditional_t<
        std::is_const_v<Map>,
        const typename std::remove_const_t<Map>::mapped_type *,
        typename std::remove_const_t<Map>::mapped_type *> {
  auto it = dags.find(dag_id);
  return it != dags.end() ? &it->second : nullptr;
}

template <typename T>
[[nodiscard]] auto run_persistence_sync(PersistenceService *persistence,
                                        task<Result<T>> op) -> Result<T> {
  if (persistence == nullptr) {
    return fail(Error::InvalidState);
  }
  return persistence->sync_wait(std::move(op));
}

[[nodiscard]] auto prepare_dag_info(DAGId dag_id, const DAGInfo &info)
    -> Result<DAGInfo> {
  DAGInfo dag_copy = info;
  dag_copy.dag_id = std::move(dag_id);
  if (auto r = dag_copy.prepare_runtime_artifacts(); !r) {
    return fail(r.error());
  }
  return ok(std::move(dag_copy));
}

[[nodiscard]] auto
persist_dag_info_if_needed(PersistenceService *persistence,
                           const DAGId &dag_id, DAGInfo dag,
                           bool existed_pre) -> Result<DAGInfo> {
  if (!persistence || !persistence->is_open()) {
    return ok(std::move(dag));
  }
  auto persisted = run_persistence_sync(
      persistence,
      persistence->upsert_dag_info(dag_id, dag, existed_pre));
  if (!persisted) {
    return fail(persisted.error());
  }
  return ok(std::move(*persisted));
}

} // namespace

auto DAGInfo::prepare_runtime_artifacts() -> Result<void> {
  rebuild_task_index();

  auto graph = std::make_shared<DAG>();
  for (const auto &task : tasks) {
    if (auto r =
            graph->add_node(task.task_id, task.trigger_rule, task.is_branch);
        !r) {
      return fail(r.error());
    }
  }

  for (const auto &task : tasks) {
    for (const auto &dep : task.dependencies) {
      if (auto r = graph->add_edge(dep.task_id, task.task_id); !r) {
        return fail(r.error());
      }
    }
  }

  auto executor_configs = ExecutorConfigBuilder::build(*this, *graph);
  std::vector<TaskConfig::Compiled> indexed_task_configs(tasks.size());
  for (const auto &task : tasks) {
    NodeIndex idx = graph->get_index(task.task_id);
    if (idx != kInvalidNode && idx < indexed_task_configs.size()) {
      indexed_task_configs[idx] = task.compiled();
    }
  }

  compiled_graph = std::move(graph);
  compiled_executor_configs =
      std::make_shared<const std::vector<ExecutorConfig>>(
          std::move(executor_configs));
  compiled_indexed_task_configs =
      std::make_shared<const std::vector<TaskConfig::Compiled>>(
          std::move(indexed_task_configs));
  return ok();
}

auto DAGInfo::rebuild_task_index() -> void {
  task_index.clear();
  for (auto [i, task] : tasks | std::views::enumerate) {
    task_index.emplace(task.task_id, static_cast<std::size_t>(i));
  }
}

auto DAGInfo::compute_reverse_adj() const
    -> ankerl::unordered_dense::map<TaskId, std::vector<TaskId>> {
  ankerl::unordered_dense::map<TaskId, std::vector<TaskId>> result;
  for (const auto &task : tasks) {
    for (const auto &dep : task.dependencies) {
      result[dep.task_id].push_back(task.task_id);
    }
  }
  return result;
}

auto DAGInfo::find_task(const TaskId &task_id) -> TaskConfig * {
  auto it = task_index.find(task_id);
  return it != task_index.end() ? &tasks[it->second] : nullptr;
}

auto DAGInfo::find_task(const TaskId &task_id) const -> const TaskConfig * {
  auto it = task_index.find(task_id);
  return it != task_index.end() ? &tasks[it->second] : nullptr;
}

DAGManager::DAGManager(PersistenceService *persistence)
    : shard_slots_(1), // slot 0 always present; resized in set_runtime()
      persistence_(persistence) {
  shard_slots_[0].snapshot.store(std::make_shared<const State>(),
                                 std::memory_order_release);
}

auto DAGManager::set_runtime(Runtime *runtime) -> void {
  runtime_ = runtime;
  if (runtime_) {
    auto current = shard_slots_[0].snapshot.load(std::memory_order_acquire);
    shard_slots_.resize(runtime_->shard_count());
    for (auto &slot : shard_slots_)
      slot.snapshot.store(current, std::memory_order_release);
  }
}

auto DAGManager::local_state_snapshot() const noexcept
    -> std::shared_ptr<const State> {
  if (runtime_) {
    const shard_id sid = runtime_->current_shard();
    if (sid != kInvalidShard && sid < shard_slots_.size()) {
      auto snapshot =
          shard_slots_[sid].snapshot.load(std::memory_order_acquire);
      if (snapshot) {
        return snapshot;
      }
    }
  }
  auto snapshot = shard_slots_[0].snapshot.load(std::memory_order_acquire);
  if (snapshot) {
    return snapshot;
  }
  return std::make_shared<const State>();
}

auto DAGManager::copy_state() const -> State { return *local_state_snapshot(); }

auto DAGManager::broadcast_state(State next) -> void {
  auto snapshot = std::make_shared<const State>(std::move(next));
  if (!runtime_ || !runtime_->is_running()) {
    for (auto &slot : shard_slots_) {
      slot.snapshot.store(snapshot, std::memory_order_release);
    }
    return;
  }

  auto current = runtime_->current_shard();
  if (current != kInvalidShard && current < shard_slots_.size()) {
    shard_slots_[current].snapshot.store(snapshot, std::memory_order_release);
  } else {
    shard_slots_[0].snapshot.store(snapshot, std::memory_order_release);
  }

  for (unsigned i = 0; i < runtime_->shard_count(); ++i) {
    if (current != kInvalidShard && i == current) {
      continue;
    }
    runtime_->post_to(static_cast<shard_id>(i), [this, i, snapshot] {
      shard_slots_[i].snapshot.store(snapshot, std::memory_order_release);
    });
  }
}

auto DAGManager::store_local_state(State next) -> void {
  auto snapshot = std::make_shared<const State>(std::move(next));
  if (!runtime_ || !runtime_->is_running()) {
    shard_slots_[0].snapshot.store(snapshot, std::memory_order_release);
    return;
  }

  auto current = runtime_->current_shard();
  if (current != kInvalidShard && current < shard_slots_.size()) {
    shard_slots_[current].snapshot.store(snapshot, std::memory_order_release);
    return;
  }
  shard_slots_[0].snapshot.store(snapshot, std::memory_order_release);
}

auto DAGManager::mutable_dag_in(State &state, const DAGId &dag_id)
    -> DAGInfo * {
  return find_dag_in(state.dags, dag_id);
}

auto DAGManager::persistence_available() const noexcept -> bool {
  return persistence_ != nullptr && persistence_->is_open();
}

auto DAGManager::persist_dag_info(DAGId dag_id, DAGInfo dag, bool existed_pre,
                                  std::string_view action)
    -> Result<DAGInfo> {
  auto result =
      persist_dag_info_if_needed(persistence_, dag_id, std::move(dag),
                                 existed_pre);
  log_result_error(result, "Failed to {} DAG {}", action, dag_id);
  return result;
}

auto DAGManager::persist_dag_delete(const DAGId &dag_id) -> Result<void> {
  if (!persistence_available()) {
    return ok();
  }

  auto result =
      run_persistence_sync(persistence_, persistence_->delete_dag(dag_id));
  log_result_error(result, "Failed to delete DAG {}", dag_id);
  return result;
}

auto DAGManager::persist_task_if_needed(const DAGId &dag_id,
                                        const TaskConfig &task,
                                        std::string_view action)
    -> Result<int64_t> {
  if (!persistence_available()) {
    return ok(task.task_rowid);
  }

  auto result =
      run_persistence_sync(persistence_, persistence_->save_task(dag_id, task));
  log_result_error(result, "Failed to persist task {} {} in DAG {}", action,
                   task.task_id, dag_id);
  return result;
}

auto DAGManager::persist_task_delete(const DAGId &dag_id, const TaskId &task_id)
    -> Result<void> {
  if (!persistence_available()) {
    return ok();
  }

  auto result = run_persistence_sync(
      persistence_, persistence_->delete_task(dag_id, task_id));
  log_result_error(result, "Failed to delete task {} from DAG {}", task_id,
                   dag_id);
  return result;
}

auto DAGManager::begin_mutation(const DAGId &dag_id)
    -> Result<MutationContext> {
  auto current = local_state_snapshot();
  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  MutationContext ctx{
      .current = dag,
      .next = copy_state(),
      .mutated = nullptr,
  };
  ctx.mutated = mutable_dag_in(ctx.next, dag_id);
  if (!ctx.mutated) {
    return fail(Error::NotFound);
  }
  return ok(std::move(ctx));
}

auto DAGManager::publish_dag_snapshot(DAGId dag_id, DAGInfo dag,
                                      bool allow_replace) -> Result<bool> {
  const bool existed =
      find_dag_in(local_state_snapshot()->dags, dag_id) != nullptr;
  if (!allow_replace && existed) {
    return fail(Error::AlreadyExists);
  }

  State next = copy_state();
  if (allow_replace) {
    next.dags.insert_or_assign(dag_id, std::move(dag));
  } else {
    next.dags.emplace(dag_id, std::move(dag));
  }
  broadcast_state(std::move(next));
  return ok(existed);
}

auto DAGManager::erase_dag_snapshot(const DAGId &dag_id) -> Result<void> {
  if (!find_dag_in(local_state_snapshot()->dags, dag_id)) {
    return fail(Error::NotFound);
  }
  State next = copy_state();
  next.dags.erase(dag_id);
  broadcast_state(std::move(next));
  return ok();
}

auto DAGManager::validate_mutation(const DAGId &dag_id,
                                   const MutationContext &ctx,
                                   DagValidateFn &validate) -> Result<void> {
  (void)dag_id;
  if (!validate) {
    return ok();
  }
  if (ctx.current == nullptr) {
    return fail(Error::NotFound);
  }
  return validate(*ctx.current);
}

auto DAGManager::apply_mutation(const DAGId &dag_id, MutationContext &ctx,
                                DagMutateFn &mutate) -> Result<void> {
  (void)dag_id;
  if (!mutate) {
    return ok();
  }
  if (ctx.mutated == nullptr) {
    return fail(Error::NotFound);
  }
  return mutate(*ctx.mutated);
}

auto DAGManager::commit_mutation(const DAGId &dag_id, MutationContext ctx,
                                 DagMutationMode mode, bool touch_updated_at)
    -> Result<void> {
  if (ctx.mutated == nullptr) {
    return fail(Error::NotFound);
  }
  if (mode == DagMutationMode::RebuildArtifacts) {
    return rebuild_and_broadcast(std::move(ctx.next), dag_id, touch_updated_at);
  }
  if (mode == DagMutationMode::LocalRebuildArtifacts) {
    auto *dag = mutable_dag_in(ctx.next, dag_id);
    if (!dag) {
      return fail(Error::NotFound);
    }
    if (touch_updated_at) {
      dag->updated_at = std::chrono::system_clock::now();
    }
    if (auto r = dag->prepare_runtime_artifacts(); !r) {
      return fail(r.error());
    }
    store_local_state(std::move(ctx.next));
    return ok();
  }
  if (touch_updated_at) {
    ctx.mutated->updated_at = std::chrono::system_clock::now();
  }
  if (mode == DagMutationMode::LocalOnly) {
    store_local_state(std::move(ctx.next));
    return ok();
  }
  broadcast_state(std::move(ctx.next));
  return ok();
}

auto DAGManager::mutate_existing_dag(const DAGId &dag_id,
                                     DagValidateFn validate, DagMutateFn mutate,
                                     DagMutationMode mode,
                                     bool touch_updated_at) -> Result<void> {
  auto ctx = begin_mutation(dag_id);
  if (!ctx) {
    return fail(ctx.error());
  }
  if (auto r = validate_mutation(dag_id, *ctx, validate); !r) {
    return fail(r.error());
  }
  if (auto r = apply_mutation(dag_id, *ctx, mutate); !r) {
    return fail(r.error());
  }
  return commit_mutation(dag_id, std::move(*ctx), mode, touch_updated_at);
}

auto DAGManager::rebuild_and_broadcast(State next, const DAGId &dag_id,
                                       bool touch_updated_at) -> Result<void> {
  auto *dag = mutable_dag_in(next, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  if (touch_updated_at) {
    dag->updated_at = std::chrono::system_clock::now();
  }
  if (auto r = dag->prepare_runtime_artifacts(); !r) {
    return fail(r.error());
  }
  broadcast_state(std::move(next));
  return ok();
}

auto DAGManager::create_dag(DAGId dag_id, const DAGInfo &info) -> Result<void> {
  auto current = local_state_snapshot();
  auto dag_copy = prepare_dag_info(dag_id.clone(), info);
  if (!dag_copy) {
    return fail(dag_copy.error());
  }
  if (find_dag_in(current->dags, dag_id)) {
    return fail(Error::AlreadyExists);
  }

  auto persisted =
      persist_dag_info(dag_id.clone(), std::move(*dag_copy), false, "persist");
  if (!persisted) {
    return fail(persisted.error());
  }
  auto published =
      publish_dag_snapshot(dag_id.clone(), std::move(*persisted), false);
  if (!published) {
    return fail(published.error());
  }
  return ok();
}

auto DAGManager::patch_dag_state(const DAGId &dag_id,
                                 const DagStateRecord &state) -> void {
  (void)mutate_existing_dag(
      dag_id, {},
      [&state](DAGInfo &dag) -> Result<void> {
        dag.dag_rowid = state.dag_rowid;
        dag.version = state.version;
        dag.is_paused = state.is_paused;
        dag.updated_at = state.updated_at;
        for (auto &task : dag.tasks) {
          if (auto rid = state.task_rowids.find(task.task_id);
              rid != state.task_rowids.end()) {
            task.task_rowid = rid->second;
          }
        }
        return ok();
      },
      DagMutationMode::RebuildArtifacts, false);
}

auto DAGManager::patch_dag_state_local(const DAGId &dag_id,
                                       const DagStateRecord &state) -> void {
  (void)mutate_existing_dag(
      dag_id, {},
      [&state](DAGInfo &dag) -> Result<void> {
        dag.dag_rowid = state.dag_rowid;
        dag.version = state.version;
        dag.is_paused = state.is_paused;
        dag.updated_at = state.updated_at;
        for (auto &task : dag.tasks) {
          if (auto rid = state.task_rowids.find(task.task_id);
              rid != state.task_rowids.end()) {
            task.task_rowid = rid->second;
          }
        }
        return ok();
      },
      DagMutationMode::LocalRebuildArtifacts, false);
}

auto DAGManager::apply_dag_state(const DAGId &dag_id,
                                 const DagStateRecord &state) -> void {
  (void)mutate_existing_dag(
      dag_id, {},
      [&state](DAGInfo &dag) -> Result<void> {
        dag.is_paused = state.is_paused;
        dag.version = state.version;
        dag.updated_at = state.updated_at;
        return ok();
      },
      DagMutationMode::BroadcastOnly, false);
}

auto DAGManager::apply_dag_state_local(const DAGId &dag_id,
                                       const DagStateRecord &state) -> void {
  (void)mutate_existing_dag(
      dag_id, {},
      [&state](DAGInfo &dag) -> Result<void> {
        dag.is_paused = state.is_paused;
        dag.version = state.version;
        dag.updated_at = state.updated_at;
        return ok();
      },
      DagMutationMode::LocalOnly, false);
}

auto DAGManager::upsert_dag(DAGId dag_id, const DAGInfo &info) -> Result<void> {
  auto current = local_state_snapshot();
  auto dag_copy = prepare_dag_info(dag_id.clone(), info);
  if (!dag_copy) {
    return fail(dag_copy.error());
  }
  const bool existed_pre = find_dag_in(current->dags, dag_id) != nullptr;

  auto persisted = persist_dag_info(dag_id.clone(), std::move(*dag_copy),
                                    existed_pre, "upsert");
  if (!persisted) {
    return fail(persisted.error());
  }
  auto published =
      publish_dag_snapshot(dag_id.clone(), std::move(*persisted), true);
  if (!published) {
    return fail(published.error());
  }
  log::info("{} DAG: {}", *published ? "Updated" : "Created", dag_id);
  return ok();
}

auto DAGManager::get_dag(const DAGId &dag_id) const -> Result<DAGInfo> {
  auto current = local_state_snapshot();
  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  return ok(*dag);
}

auto DAGManager::list_dags() const -> std::vector<DAGInfo> {
  auto current = local_state_snapshot();
  return current->dags | std::views::values | std::ranges::to<std::vector>();
}

auto DAGManager::delete_dag(const DAGId &dag_id) -> Result<void> {
  auto current = local_state_snapshot();
  if (!find_dag_in(current->dags, dag_id)) {
    return fail(Error::NotFound);
  }

  if (auto persisted = persist_dag_delete(dag_id); !persisted) {
    return fail(persisted.error());
  }
  if (auto erased = erase_dag_snapshot(dag_id); !erased) {
    return fail(erased.error());
  }
  log::info("Deleted DAG: {}", dag_id);
  return ok();
}

auto DAGManager::replace_all(std::vector<DAGInfo> dags) -> Result<void> {
  State next;
  next.dags.reserve(dags.size());

  for (auto &dag : dags) {
    if (dag.dag_id.empty()) {
      return fail(Error::InvalidArgument);
    }
    if (auto r = dag.prepare_runtime_artifacts(); !r) {
      return fail(r.error());
    }

    auto dag_id = dag.dag_id.clone();
    auto [_, inserted] = next.dags.emplace(dag_id, std::move(dag));
    if (!inserted) {
      return fail(Error::AlreadyExists);
    }
  }

  broadcast_state(std::move(next));
  return ok();
}

auto DAGManager::clear_all() -> void { broadcast_state(State{}); }

auto DAGManager::add_task(const DAGId &dag_id, const TaskConfig &task)
    -> Result<void> {
  auto current = local_state_snapshot();

  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  if (dag->find_task(task.task_id)) {
    return fail(Error::AlreadyExists);
  }

  auto dep_ids = task.dependencies |
                 std::views::transform(&TaskDependency::task_id) |
                 std::ranges::to<std::vector>();

  if (would_create_cycle_internal(*dag, task.task_id, dep_ids)) {
    log::warn("Adding task {} would create a cycle in DAG {}", task.task_id,
              dag_id);
    return fail(Error::InvalidArgument);
  }

  if (std::ranges::any_of(task.dependencies, [&](const TaskDependency &dep) {
        return !dag->find_task(dep.task_id);
      })) {
    log::warn("Dependency not found in DAG {}", dag_id);
    return fail(Error::NotFound);
  }

  auto persisted_task_rowid = persist_task_if_needed(dag_id, task, "create");
  if (!persisted_task_rowid) {
    return fail(persisted_task_rowid.error());
  }

  auto saved_task = task;
  saved_task.task_rowid = *persisted_task_rowid;
  return mutate_existing_dag(
      dag_id, {},
      [task_id = task.task_id, saved_task = std::move(saved_task)](
          DAGInfo &dag_mut) mutable -> Result<void> {
        dag_mut.tasks.emplace_back(std::move(saved_task));
        dag_mut.task_index[task_id] = dag_mut.tasks.size() - 1;
        return ok();
      });
}

auto DAGManager::update_task(const DAGId &dag_id, const TaskId &task_id,
                             const TaskConfig &task) -> Result<void> {
  auto current = local_state_snapshot();

  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }
  if (!dag->find_task(task_id)) {
    return fail(Error::NotFound);
  }

  auto dep_ids = task.dependencies |
                 std::views::transform(&TaskDependency::task_id) |
                 std::ranges::to<std::vector>();

  if (would_create_cycle_internal(*dag, task_id, dep_ids)) {
    return fail(Error::InvalidArgument);
  }

  auto persisted_task_rowid = persist_task_if_needed(dag_id, task, "update");
  if (!persisted_task_rowid) {
    return fail(persisted_task_rowid.error());
  }

  if (auto r = mutate_existing_dag(
          dag_id, {},
          [task, task_id, task_rowid = *persisted_task_rowid](
              DAGInfo &dag_mut) -> Result<void> {
            auto *existing_ptr = dag_mut.find_task(task_id);
            if (!existing_ptr) {
              return fail(Error::NotFound);
            }
            *existing_ptr = task;
            existing_ptr->task_id = task_id;
            existing_ptr->task_rowid = task_rowid;
            return ok();
          });
      !r) {
    return fail(r.error());
  }

  log::info("Updated task {} in DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> Result<void> {
  auto current = local_state_snapshot();

  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  for (const auto &t : dag->tasks) {
    if (std::ranges::any_of(t.dependencies, [&](const TaskDependency &d) {
          return d.task_id == task_id;
        })) {
      log::warn("Cannot delete task {} - task {} depends on it", task_id,
                t.task_id);
      return fail(Error::InvalidArgument);
    }
  }

  auto idx_it = dag->task_index.find(task_id);
  if (idx_it == dag->task_index.end()) {
    return fail(Error::NotFound);
  }

  if (auto persisted = persist_task_delete(dag_id, task_id); !persisted) {
    return fail(persisted.error());
  }

  if (auto r = mutate_existing_dag(
          dag_id, {},
          [task_id](DAGInfo &dag_mut) -> Result<void> {
            auto idx_it_inner = dag_mut.task_index.find(task_id);
            if (idx_it_inner == dag_mut.task_index.end()) {
              return fail(Error::NotFound);
            }

            const std::size_t idx = idx_it_inner->second;
            const std::size_t last_idx = dag_mut.tasks.size() - 1;
            if (idx != last_idx) {
              dag_mut.task_index[dag_mut.tasks[last_idx].task_id] = idx;
              std::swap(dag_mut.tasks[idx], dag_mut.tasks[last_idx]);
            }

            dag_mut.tasks.pop_back();
            dag_mut.task_index.erase(idx_it_inner);
            return ok();
          });
      !r) {
    return fail(r.error());
  }
  log::info("Deleted task {} from DAG {}", task_id, dag_id);
  return ok();
}

auto DAGManager::get_task(const DAGId &dag_id, const TaskId &task_id) const
    -> Result<TaskConfig> {
  auto current = local_state_snapshot();
  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return fail(Error::NotFound);
  }

  const auto *task = dag->find_task(task_id);
  if (!task) {
    return fail(Error::NotFound);
  }

  return ok(*task);
}

auto DAGManager::validate_dag(const DAGId &dag_id) const -> Result<void> {
  return build_dag_graph(dag_id).and_then(
      [](const DAG &dag) { return dag.is_valid(); });
}

auto DAGManager::would_create_cycle_internal(
    const DAGInfo &dag, const TaskId &task_id,
    const std::vector<TaskId> &dependencies) const -> bool {
  if (dependencies.empty()) {
    return false;
  }

  const auto reverse_adj = dag.compute_reverse_adj();

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

    if (!visited.insert(current).second) {
      continue;
    }

    auto it = reverse_adj.find(current);
    if (it != reverse_adj.end()) {
      for (const auto &next : it->second) {
        q.push(next);
      }
    }
  }

  return false;
}

auto DAGManager::build_dag_graph(const DAGId &dag_id) const -> Result<DAG> {
  auto current = local_state_snapshot();
  const auto *dag_info = find_dag_in(current->dags, dag_id);
  if (!dag_info) {
    return fail(Error::NotFound);
  }

  if (dag_info->compiled_graph) {
    return ok(*dag_info->compiled_graph);
  }

  DAG dag;
  for (const auto &task : dag_info->tasks) {
    if (auto r = dag.add_node(task.task_id, task.trigger_rule, task.is_branch);
        !r) {
      return fail(r.error());
    }
  }

  for (const auto &task : dag_info->tasks) {
    for (const auto &dep : task.dependencies) {
      if (auto r = dag.add_edge(dep.task_id, task.task_id); !r) {
        return fail(r.error());
      }
    }
  }

  return ok(std::move(dag));
}

auto DAGManager::would_create_cycle(
    const DAGId &dag_id, const TaskId &task_id,
    const std::vector<TaskId> &dependencies) const -> bool {
  auto current = local_state_snapshot();
  const auto *dag = find_dag_in(current->dags, dag_id);
  if (!dag) {
    return false;
  }
  return would_create_cycle_internal(*dag, task_id, dependencies);
}

auto DAGManager::load_from_database() -> Result<void> {
  if (!persistence_) {
    return ok();
  }

  return run_persistence_sync(persistence_, persistence_->list_dags())
      .and_then([&](std::vector<DAGInfo> &&dags_result) -> Result<void> {
        State next = copy_state();
        std::size_t merged_into_file_dags = 0;
        std::size_t inserted_from_db = 0;

        for (auto &db_dag : dags_result) {
          if (auto r = db_dag.prepare_runtime_artifacts(); !r) {
            return fail(r.error());
          }
          DAGId id = db_dag.dag_id;

          auto it = next.dags.find(id);
          if (it == next.dags.end()) {
            // No file-loaded definition: use DB record as-is.
            next.dags.insert_or_assign(std::move(id), std::move(db_dag));
            ++inserted_from_db;
            continue;
          }

          // File definition exists: keep it as the structural truth and
          // merge only runtime state from the DB record.
          merge_db_state_into_dag_info(it->second, db_dag);
          if (auto r = it->second.prepare_runtime_artifacts(); !r) {
            return fail(r.error());
          }
          ++merged_into_file_dags;
        }

        broadcast_state(std::move(next));
        log::debug("Loaded {} DAGs from database (inserted={}, merged={})",
                   dags_result.size(), inserted_from_db, merged_into_file_dags);
        return ok();
      });
}

auto DAGManager::dag_count() const noexcept -> std::size_t {
  return local_state_snapshot()->dags.size();
}

auto DAGManager::has_dag(DAGId dag_id) const -> bool {
  return find_dag_in(local_state_snapshot()->dags, dag_id) != nullptr;
}

} // namespace dagforge
