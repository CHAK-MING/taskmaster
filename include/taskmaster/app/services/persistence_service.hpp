#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/util/id.hpp"

#include <memory>
#include <nlohmann/json.hpp>
#include <string_view>

namespace taskmaster {

class PersistenceService {
public:
  explicit PersistenceService(std::string_view db_path);
  ~PersistenceService();

  PersistenceService(const PersistenceService&) = delete;
  auto operator=(const PersistenceService&) -> PersistenceService& = delete;

  [[nodiscard]] auto open() -> Result<void>;
  auto close() -> void;
  [[nodiscard]] auto is_open() const -> bool;

  [[nodiscard]] auto persistence() -> Persistence*;

  [[nodiscard]] auto save_run(const DAGRun& run) -> Result<int64_t>;
  auto save_task(const DAGRunId& dag_run_id, const TaskInstanceInfo& info) -> void;
  auto save_log(const DAGRunId& dag_run_id, const TaskId& task, int attempt,
                std::string_view level, std::string_view msg) -> void;
  auto save_xcom(const DAGRunId& dag_run_id, const TaskId& task,
                 std::string_view key, const nlohmann::json& value) -> void;
  [[nodiscard]] auto get_xcom(const DAGRunId& dag_run_id, const TaskId& task,
                              std::string_view key) -> Result<nlohmann::json>;
  [[nodiscard]] auto get_previous_task_state(
      DAGId dag_id, NodeIndex task_idx,
      std::chrono::system_clock::time_point current_execution_date,
      std::string_view current_dag_run_id) const
      -> Result<std::optional<TaskState>>;

  // Transaction support
  [[nodiscard]] auto begin_transaction() -> Result<void>;
  [[nodiscard]] auto commit_transaction() -> Result<void>;
  [[nodiscard]] auto rollback_transaction() -> Result<void>;

  // Batch operations
  auto save_task_instances_batch(const DAGRunId& dag_run_id,
                                  const std::vector<TaskInstanceInfo>& instances) -> void;

private:
  std::unique_ptr<Persistence> db_;
};

}  // namespace taskmaster
