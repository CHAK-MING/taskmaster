#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/storage/dag_state_adapter.hpp"
#include "dagforge/storage/orm_models.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/string_hash.hpp"
#endif

#include <ankerl/unordered_dense.h>

#include <chrono>
#include <string>
#include <unordered_map>
#include <memory>
#include <optional>
#include <vector>

namespace dagforge {

class DAGOrchestrator {
public:
  using RunConfMap =
      std::unordered_map<std::string, std::string, StringHash, StringEqual>;

  struct Dependencies {
    DAGManager &dag_manager;
    PersistenceService &persistence;
    ExecutionService &execution;
    SchedulerService &scheduler;
    Runtime &runtime;
    std::size_t owner_shard_count{1};
  };

  explicit DAGOrchestrator(Dependencies deps);

  [[nodiscard]] auto trigger_run(
      DAGId dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt,
      RunConfMap conf_values = RunConfMap{}) -> task<Result<DAGRunId>>;
  [[nodiscard]] auto
  trigger_scheduled(DAGId dag_id,
                    std::chrono::system_clock::time_point execution_date)
      -> spawn_task;
  [[nodiscard]] auto trigger_run_blocking(
      const DAGId &dag_id, TriggerType trigger = TriggerType::Manual,
      std::optional<std::chrono::system_clock::time_point> execution_date =
          std::nullopt) -> Result<DAGRunId>;
  [[nodiscard]] auto get_run_state(const DAGRunId &dag_run_id) const
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_run_state_async(const DAGRunId &dag_run_id) const
      -> task<Result<DAGRunState>>;
  [[nodiscard]] auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> task<Result<void>>;
  [[nodiscard]] auto register_dag_cron(DAGId dag_id, std::string_view cron_expr)
      -> Result<void>;
  auto unregister_dag_cron(const DAGId &dag_id) -> void;
  [[nodiscard]] auto update_dag_cron(const DAGId &dag_id,
                                     std::string_view cron_expr, bool is_active)
      -> Result<void>;
  [[nodiscard]] auto wait_for_completion_async(int timeout_ms = 60000)
      -> task<Result<void>>;
  [[nodiscard]] auto has_active_runs() const -> bool;

  [[nodiscard]] auto owner_shard(const DAGId &dag_id) const noexcept
      -> shard_id;
  [[nodiscard]] auto owner_shard(const DAGRunId &dag_run_id) const noexcept
      -> shard_id;
  [[nodiscard]] auto resolve_dag_id(const DAGRunId &dag_run_id) const
      -> std::optional<DAGId>;
  auto on_run_finished(const DAGRunId &dag_run_id, DAGRunState status) -> void;
  [[nodiscard]] auto get_max_retries(const DAGRunId &dag_run_id,
                                     NodeIndex idx) const -> int;
  [[nodiscard]] auto get_retry_interval(const DAGRunId &dag_run_id,
                                        NodeIndex idx) const
      -> std::chrono::seconds;

private:
  struct RunLaunchPlan {
    DAGId dag_id;
    int64_t dag_rowid{0};
    int version{1};
    std::shared_ptr<const DAG> graph;
    std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs;
    std::shared_ptr<const std::vector<TaskConfig::Compiled>>
        indexed_task_configs;
    RunConfMap conf_values;
  };

  struct DagOwnerState {
    int active_runs{0};
  };

  struct DagOwnerShardState {
    ankerl::unordered_dense::map<DAGId, DagOwnerState> dags;
  };

  auto trigger_scheduled_on_owner_shard(
      DAGId dag_id,
      std::chrono::system_clock::time_point execution_date) -> spawn_task;
  auto trigger_run_on_dag_owner_shard(
      DAGId dag_id, TriggerType trigger,
      std::optional<std::chrono::system_clock::time_point> execution_date,
      RunConfMap conf_values,
      std::chrono::system_clock::time_point request_now)
      -> task<Result<DAGRunId>>;
  auto trigger_run_on_owner_shard(
      RunLaunchPlan plan, TriggerType trigger,
      std::optional<std::chrono::system_clock::time_point> execution_date,
      DAGRunId dag_run_id,
      std::chrono::system_clock::time_point request_now)
      -> task<Result<DAGRunId>>;
  [[nodiscard]] auto try_acquire_dag_run_slot(const DAGInfo &info)
      -> Result<void>;
  auto release_dag_run_slot(const DAGId &dag_id) -> void;

  Dependencies deps_;
  std::vector<DagOwnerShardState> dag_owner_states_;
};

} // namespace dagforge
