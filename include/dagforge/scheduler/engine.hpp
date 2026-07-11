#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/io/timing_wheel.hpp"
#include "dagforge/scheduler/execution_info.hpp"
#include "dagforge/scheduler/task.hpp"
#include "dagforge/util/id.hpp"
#endif

#include <boost/asio/awaitable.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

namespace dagforge {

class Runtime;

// Single-threaded scheduler owned by one Runtime shard.
// External calls route through Runtime so shard ownership and allocator
// integration remain explicit.
class Engine {
public:
  using TimePoint = std::chrono::system_clock::time_point;
  using DAGTriggerCallback =
      std::move_only_function<void(const DAGId &, TimePoint execution_date)>;
  using RunExistsCallback =
      std::move_only_function<boost::asio::awaitable<Result<bool>>(
          const DAGId &, TimePoint)>;
  using ListRunExecutionDatesCallback =
      std::move_only_function<boost::asio::awaitable<Result<std::vector<TimePoint>>>(
          const DAGId &, TimePoint, TimePoint)>;
  using GetWatermarkCallback = std::move_only_function<
      boost::asio::awaitable<Result<std::optional<TimePoint>>>(const DAGId &)>;
  using SaveWatermarkCallback =
      std::move_only_function<boost::asio::awaitable<Result<void>>(
          const DAGId &, TimePoint)>;

  explicit Engine(Runtime &runtime, shard_id owner_shard = shard_id{0});
  ~Engine();

  Engine(const Engine &) = delete;
  auto operator=(const Engine &) -> Engine & = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool {
    return running_.load();
  }

  [[nodiscard]] auto add_task(ExecutionInfo exec_info) -> Result<void>;
  [[nodiscard]] auto remove_task(DAGId dag_id, TaskId task_id) -> Result<void>;

  auto set_on_dag_trigger(DAGTriggerCallback cb) -> void;
  auto set_run_exists_callback(RunExistsCallback cb) -> void;
  auto set_list_run_execution_dates_callback(ListRunExecutionDatesCallback cb)
      -> void;
  auto set_get_watermark_callback(GetWatermarkCallback cb) -> void;
  auto set_save_watermark_callback(SaveWatermarkCallback cb) -> void;

  [[nodiscard]] auto scheduled_task_count() const -> std::size_t;
  [[nodiscard]] auto missed_schedules_total() const -> std::uint64_t;

private:
  auto run_cron_task(DAGTaskId dag_task_id, TimePoint execution_date,
                     std::uint64_t generation) -> boost::asio::awaitable<void>;
  auto schedule_task(const DAGTaskId &dag_task_id, TimePoint next_time) -> void;
  auto unschedule_task(const DAGTaskId &dag_task_id) -> void;
  auto stop_on_owner() -> void;

  auto add_task_on_owner(ExecutionInfo exec_info)
      -> boost::asio::awaitable<void>;
  auto remove_task_on_owner(DAGId dag_id, TaskId task_id) -> void;

  alignas(64) std::atomic<bool> running_{false};
  Runtime &runtime_;
  shard_id owner_shard_;

  // Owner-shard state.
  std::unordered_map<DAGTaskId, ExecutionInfo> tasks_;
  struct ScheduledTask {
    io::TimingWheel::Handle handle{};
    std::uint64_t generation{0};
  };
  std::unordered_map<DAGTaskId, ScheduledTask> scheduled_tasks_;
  std::uint64_t next_schedule_generation_{1};

  // Cross-thread metric projection; owner shard is the only writer.
  std::atomic<std::size_t> scheduled_task_count_snapshot_{0};

  DAGTriggerCallback on_dag_trigger_;
  RunExistsCallback run_exists_;
  ListRunExecutionDatesCallback list_run_execution_dates_;
  GetWatermarkCallback get_watermark_;
  SaveWatermarkCallback save_watermark_;
  std::atomic<std::uint64_t> missed_schedules_total_{0};
};

} // namespace dagforge
