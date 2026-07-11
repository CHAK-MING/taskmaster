#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_config.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/storage/dag_state_adapter.hpp"
#include "dagforge/util/id.hpp"
#endif

#include <ankerl/unordered_dense.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <vector>


namespace dagforge {

class PersistenceService;
class Runtime;

struct DAGInfo {
  int64_t dag_rowid{0};
  DAGId dag_id{};
  int version{1};
  std::string name;
  std::string description;
  std::string tags; // JSON array string
  std::string cron;
  std::string timezone{"UTC"};
  int max_concurrent_runs{1};
  std::optional<std::chrono::system_clock::time_point> start_date{};
  std::optional<std::chrono::system_clock::time_point> end_date{};
  bool catchup{false};
  bool is_paused{false};
  int retention_days{30};
  std::chrono::system_clock::time_point created_at{};
  std::chrono::system_clock::time_point updated_at{};
  std::vector<TaskConfig> tasks{};
  ankerl::unordered_dense::map<TaskId, std::size_t> task_index{};
  std::shared_ptr<const DAG> compiled_graph{};
  std::shared_ptr<const std::vector<ExecutorConfig>> compiled_executor_configs{};
  std::shared_ptr<const std::vector<TaskConfig::Compiled>>
      compiled_indexed_task_configs{};

  auto rebuild_task_index() -> void;

  [[nodiscard]] auto prepare_runtime_artifacts() -> Result<void>;

  [[nodiscard]] auto compute_reverse_adj() const
      -> ankerl::unordered_dense::map<TaskId, std::vector<TaskId>>;

  [[nodiscard]] auto find_task(const TaskId &task_id) -> TaskConfig *;

  [[nodiscard]] auto find_task(const TaskId &task_id) const
      -> const TaskConfig *;
};

class DAGManager {
public:
  explicit DAGManager(PersistenceService *persistence = nullptr);
  ~DAGManager() = default;

  DAGManager(const DAGManager &) = delete;
  auto operator=(const DAGManager &) -> DAGManager & = delete;
  DAGManager(DAGManager &&) = delete;
  auto operator=(DAGManager &&) -> DAGManager & = delete;

  auto set_persistence_service(PersistenceService *persistence) -> void {
    persistence_ = persistence;
  }

  auto set_runtime(Runtime *runtime) -> void;

  // DAG CRUD
  [[nodiscard]] auto create_dag(DAGId dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto upsert_dag(DAGId dag_id, const DAGInfo &info)
      -> Result<void>;
  [[nodiscard]] auto get_dag(const DAGId &dag_id) const -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() const -> std::vector<DAGInfo>;
  [[nodiscard]] auto delete_dag(const DAGId &dag_id) -> Result<void>;
  [[nodiscard]] auto replace_all(std::vector<DAGInfo> dags) -> Result<void>;
  auto clear_all() -> void;

  // Task CRUD within DAG
  [[nodiscard]] auto add_task(const DAGId &dag_id, const TaskConfig &task)
      -> Result<void>;
  [[nodiscard]] auto update_task(const DAGId &dag_id, const TaskId &task_id,
                                 const TaskConfig &task) -> Result<void>;
  [[nodiscard]] auto delete_task(const DAGId &dag_id, const TaskId &task_id)
      -> Result<void>;
  [[nodiscard]] auto get_task(const DAGId &dag_id, const TaskId &task_id) const
      -> Result<TaskConfig>;

  // Validation
  [[nodiscard]] auto validate_dag(const DAGId &dag_id) const -> Result<void>;
  [[nodiscard]] auto
  would_create_cycle(const DAGId &dag_id, const TaskId &task_id,
                     const std::vector<TaskId> &dependencies) const -> bool;

  // Build DAG graph for execution
  [[nodiscard]] auto build_dag_graph(const DAGId &dag_id) const -> Result<DAG>;

  [[nodiscard]] auto load_from_database() -> Result<void>;

  auto patch_dag_state(const DAGId &dag_id, const DagStateRecord &state)
      -> void;
  auto patch_dag_state_local(const DAGId &dag_id, const DagStateRecord &state)
      -> void;
  auto apply_dag_state(const DAGId &dag_id, const DagStateRecord &state)
      -> void;
  auto apply_dag_state_local(const DAGId &dag_id, const DagStateRecord &state)
      -> void;

  [[nodiscard]] auto dag_count() const noexcept -> std::size_t;
  [[nodiscard]] auto has_dag(DAGId dag_id) const -> bool;

private:
  using DagValidateFn = std::move_only_function<Result<void>(const DAGInfo &)>;
  using DagMutateFn = std::move_only_function<Result<void>(DAGInfo &)>;

  struct State {
    ankerl::unordered_dense::map<DAGId, DAGInfo> dags;
  };

  enum class DagMutationMode : std::uint8_t {
    RebuildArtifacts,
    BroadcastOnly,
    LocalRebuildArtifacts,
    LocalOnly,
  };

  struct alignas(64) ShardSlot {
    std::atomic<std::shared_ptr<const State>> snapshot;

    ShardSlot() = default;
    ShardSlot(const ShardSlot &) = delete;
    auto operator=(const ShardSlot &) -> ShardSlot & = delete;

    ShardSlot(ShardSlot &&other) noexcept {
      snapshot.store(other.snapshot.load(std::memory_order_acquire),
                     std::memory_order_release);
    }

    auto operator=(ShardSlot &&other) noexcept -> ShardSlot & {
      if (this != &other) {
        snapshot.store(other.snapshot.load(std::memory_order_acquire),
                       std::memory_order_release);
      }
      return *this;
    }
  };

  struct MutationContext {
    const DAGInfo *current{nullptr};
    State next;
    DAGInfo *mutated{nullptr};
  };

  [[nodiscard]] auto local_state_snapshot() const noexcept
      -> std::shared_ptr<const State>;
  auto broadcast_state(State next) -> void;
  auto store_local_state(State next) -> void;
  [[nodiscard]] auto copy_state() const -> State;
  [[nodiscard]] auto mutable_dag_in(State &state, const DAGId &dag_id)
      -> DAGInfo *;
  [[nodiscard]] auto persistence_available() const noexcept -> bool;
  [[nodiscard]] auto persist_dag_info(DAGId dag_id, DAGInfo dag,
                                      bool existed_pre,
                                      std::string_view action)
      -> Result<DAGInfo>;
  [[nodiscard]] auto persist_dag_delete(const DAGId &dag_id) -> Result<void>;
  [[nodiscard]] auto persist_task_if_needed(const DAGId &dag_id,
                                            const TaskConfig &task,
                                            std::string_view action)
      -> Result<int64_t>;
  [[nodiscard]] auto persist_task_delete(const DAGId &dag_id,
                                         const TaskId &task_id) -> Result<void>;
  [[nodiscard]] auto begin_mutation(const DAGId &dag_id)
      -> Result<MutationContext>;
  [[nodiscard]] auto publish_dag_snapshot(DAGId dag_id, DAGInfo dag,
                                          bool allow_replace) -> Result<bool>;
  [[nodiscard]] auto erase_dag_snapshot(const DAGId &dag_id) -> Result<void>;
  [[nodiscard]] auto validate_mutation(const DAGId &dag_id,
                                       const MutationContext &ctx,
                                       DagValidateFn &validate)
      -> Result<void>;
  [[nodiscard]] auto apply_mutation(const DAGId &dag_id,
                                    MutationContext &ctx,
                                    DagMutateFn &mutate)
      -> Result<void>;
  [[nodiscard]] auto commit_mutation(const DAGId &dag_id, MutationContext ctx,
                                     DagMutationMode mode,
                                     bool touch_updated_at) -> Result<void>;
  [[nodiscard]] auto mutate_existing_dag(
      const DAGId &dag_id, DagValidateFn validate, DagMutateFn mutate,
      DagMutationMode mode = DagMutationMode::RebuildArtifacts,
      bool touch_updated_at = true) -> Result<void>;
  [[nodiscard]] auto rebuild_and_broadcast(State next, const DAGId &dag_id,
                                           bool touch_updated_at = true)
      -> Result<void>;

  [[nodiscard]] auto generate_dag_id() const -> DAGId;
  [[nodiscard]] auto
  would_create_cycle_internal(const DAGInfo &dag, const TaskId &task_id,
                              const std::vector<TaskId> &dependencies) const
      -> bool;

  // Per-shard local state; index == shard_id.  Slot 0 is always present and
  // used as the fallback for single-threaded / test scenarios.
  std::vector<ShardSlot> shard_slots_;
  Runtime *runtime_{nullptr};
  PersistenceService *persistence_;
};

} // namespace dagforge
