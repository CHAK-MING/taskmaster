#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/scheduler/engine.hpp"
#endif

#include <atomic>
#include <cstdint>
#include <chrono>
#include <memory>
#include <unordered_map>


namespace dagforge {

class DAGManager;

using DAGTriggerCallback = std::move_only_function<void(
    const DAGId &, std::chrono::system_clock::time_point)>;
using RunExistsCallback = Engine::RunExistsCallback;
using ListRunExecutionDatesCallback = Engine::ListRunExecutionDatesCallback;
using GetWatermarkCallback = Engine::GetWatermarkCallback;
using SaveWatermarkCallback = Engine::SaveWatermarkCallback;
using ZombieReaperCallback =
    std::move_only_function<task<Result<std::size_t>>(std::int64_t)>;

class SchedulerService {
public:
  explicit SchedulerService(Runtime &runtime, unsigned scheduler_shards = 0);
  ~SchedulerService();

  SchedulerService(const SchedulerService &) = delete;
  auto operator=(const SchedulerService &) -> SchedulerService & = delete;

  auto set_on_dag_trigger(DAGTriggerCallback callback) -> void;
  auto set_run_exists_callback(RunExistsCallback callback) -> void;
  auto set_list_run_execution_dates_callback(
      ListRunExecutionDatesCallback callback) -> void;
  auto set_get_watermark_callback(GetWatermarkCallback callback) -> void;
  auto set_save_watermark_callback(SaveWatermarkCallback callback) -> void;
  auto set_zombie_reaper_callback(ZombieReaperCallback callback) -> void;
  auto set_zombie_reaper_config(int interval_sec, int heartbeat_timeout_sec)
      -> void;

  auto register_dag(DAGId dag_id, const DAGInfo &dag_info) -> void;

  auto unregister_dag(const DAGId &dag_id) -> void;

  auto start() -> void;
  [[nodiscard]] auto stop(std::chrono::steady_clock::time_point deadline =
                              std::chrono::steady_clock::time_point::max())
      -> Result<void>;
  [[nodiscard]] auto is_running() const -> bool;

  [[nodiscard]] auto queue_depth() const -> std::size_t;
  [[nodiscard]] auto missed_schedules_total() const -> std::uint64_t;
  [[nodiscard]] auto cron_parse_errors_total() const -> std::uint64_t;

private:
  [[nodiscard]] auto owner_engine_index(const DAGId &dag_id) const noexcept
      -> std::size_t;
  [[nodiscard]] auto owner_shard(const DAGId &dag_id) const noexcept -> shard_id;
  [[nodiscard]] auto owner_engine(const DAGId &dag_id) -> Engine &;
  [[nodiscard]] auto can_update_callbacks(std::string_view callback_name) const
      -> bool;
  auto refresh_engine_callbacks() -> void;

  struct ZombieReaperState {
    ZombieReaperCallback callback;
    int interval_sec{60};
    int heartbeat_timeout_sec{75};
    std::atomic<bool> running{false};
    std::atomic<int> inflight{0};
    std::atomic<std::uint64_t> generation{0};
  };

  Runtime &runtime_;
  shard_id zombie_reaper_shard_{0};
  std::vector<std::unique_ptr<Engine>> engines_;
  std::vector<shard_id> engine_shards_;
  DAGTriggerCallback on_dag_trigger_;
  RunExistsCallback run_exists_callback_;
  ListRunExecutionDatesCallback list_run_execution_dates_callback_;
  GetWatermarkCallback get_watermark_callback_;
  SaveWatermarkCallback save_watermark_callback_;
  std::shared_ptr<ZombieReaperState> zombie_reaper_state_{
      std::make_shared<ZombieReaperState>()};
  std::atomic<std::uint64_t> cron_parse_errors_total_{0};
  std::unordered_map<DAGId, TaskId> registered_root_tasks_;
};

} // namespace dagforge
