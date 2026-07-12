#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/memory.hpp"
#include "dagforge/dag/dag.hpp"
#include "dagforge/dag/run_types.hpp"
#endif

#include <chrono>
#include <boost/container/small_vector.hpp>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge {

struct DAGRunPrivateTag {};

class DAGRun {
public:
  struct TransitionDelta {
    using NodeList = boost::container::small_vector<NodeIndex, 4>;

    NodeList ready_tasks;
    NodeList terminal_tasks;

    [[nodiscard]] auto has_ready_tasks() const noexcept -> bool {
      return !ready_tasks.empty();
    }
    [[nodiscard]] auto has_terminal_tasks() const noexcept -> bool {
      return !terminal_tasks.empty();
    }
  };

  [[nodiscard]] static auto create(DAGRunId dag_run_id,
                                   std::shared_ptr<const DAG> dag)
      -> Result<DAGRun>;

  ~DAGRun();
  DAGRun(DAGRun &&) noexcept;
  DAGRun &operator=(DAGRun &&) noexcept;
  DAGRun(const DAGRun &);
  DAGRun &operator=(const DAGRun &);

  [[nodiscard]] auto id() const noexcept -> const DAGRunId &;
  [[nodiscard]] auto state() const noexcept -> DAGRunState;
  [[nodiscard]] auto dag() const noexcept -> const DAG &;

  [[nodiscard]] auto get_ready_tasks(
      pmr::memory_resource *resource = pmr::get_default_resource()) const
      -> pmr::vector<NodeIndex>;
  auto copy_ready_tasks(pmr::vector<NodeIndex> &out) const -> void;
  [[nodiscard]] auto is_task_ready(NodeIndex task_idx) const noexcept -> bool;
  [[nodiscard]] auto ready_count() const noexcept -> size_t;

  [[nodiscard]] auto mark_task_started(NodeIndex task_idx,
                                       const InstanceId &instance_id)
      -> Result<void>;
  [[nodiscard]] auto
  mark_task_started(NodeIndex task_idx, const InstanceId &instance_id,
                    std::chrono::system_clock::time_point started_at)
      -> Result<void>;
  [[nodiscard]] auto mark_task_completed(NodeIndex task_idx, int exit_code)
      -> Result<TransitionDelta>;
  [[nodiscard]] auto mark_task_failed(NodeIndex task_idx,
                                      std::string_view error, int max_retries,
                                      int exit_code = 1)
      -> Result<TransitionDelta>;
  [[nodiscard]] auto mark_task_retry_ready(NodeIndex task_idx)
      -> Result<TransitionDelta>;
  [[nodiscard]] auto mark_task_skipped(NodeIndex task_idx)
      -> Result<TransitionDelta>;
  [[nodiscard]] auto mark_task_upstream_failed(NodeIndex task_idx)
      -> Result<TransitionDelta>;
  [[nodiscard]] auto restore_task_instance(const TaskInstanceInfo &info)
      -> Result<void>;

  [[nodiscard]] auto set_instance_id(NodeIndex task_idx,
                                     const InstanceId &instance_id)
      -> Result<void>;
  [[nodiscard]] auto set_task_rowid(NodeIndex task_idx, int64_t task_rowid)
      -> Result<void>;

  [[nodiscard]] auto is_complete() const noexcept -> bool;
  [[nodiscard]] auto has_failed() const noexcept -> bool;

  [[nodiscard]] auto get_task_info(NodeIndex task_idx) const
      -> Result<TaskInstanceInfo>;
  [[nodiscard]] auto all_task_info() const -> std::vector<TaskInstanceInfo>;

  [[nodiscard]] auto scheduled_at() const noexcept
      -> std::chrono::system_clock::time_point;
  [[nodiscard]] auto started_at() const noexcept
      -> std::chrono::system_clock::time_point;
  [[nodiscard]] auto finished_at() const noexcept
      -> std::chrono::system_clock::time_point;

  auto set_scheduled_at(std::chrono::system_clock::time_point t) noexcept
      -> void;
  auto set_started_at(std::chrono::system_clock::time_point t) noexcept -> void;
  auto set_finished_at(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto trigger_type() const noexcept -> TriggerType;
  auto set_trigger_type(TriggerType t) noexcept -> void;

  [[nodiscard]] auto execution_date() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_execution_date(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto data_interval_start() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_data_interval_start(std::chrono::system_clock::time_point t) noexcept
      -> void;

  [[nodiscard]] auto data_interval_end() const noexcept
      -> std::chrono::system_clock::time_point;
  auto set_data_interval_end(std::chrono::system_clock::time_point t) noexcept
      -> void;

  void set_run_rowid(int64_t rowid) noexcept;
  [[nodiscard]] auto run_rowid() const noexcept -> int64_t;

  void set_dag_rowid(int64_t rowid) noexcept;
  [[nodiscard]] auto dag_rowid() const noexcept -> int64_t;

  void set_dag_version(int version) noexcept;
  [[nodiscard]] auto dag_version() const noexcept -> int;

private:
  DAGRun(DAGRunPrivateTag, DAGRunId dag_run_id, std::shared_ptr<const DAG> dag);

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge
