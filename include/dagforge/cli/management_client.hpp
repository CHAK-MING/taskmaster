#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/dag_manager.hpp"
#include "dagforge/dag/dag_run.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/storage/database_service.hpp"
#include "dagforge/storage/mysql_database.hpp"
#include "dagforge/storage/orm_models.hpp"
#endif

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>


namespace dagforge::cli {
class ManagementClient {
public:
  explicit ManagementClient(const DatabaseConfig &db_config);
  ~ManagementClient();

  ManagementClient(const ManagementClient &) = delete;
  auto operator=(const ManagementClient &) -> ManagementClient & = delete;

  [[nodiscard]] auto open() -> Result<void>;

  [[nodiscard]] auto list_dags() const -> Result<std::vector<DAGInfo>>;
  [[nodiscard]] auto get_dag(const DAGId &dag_id) const -> Result<DAGInfo>;

  using RunHistoryEntry = DatabaseService::RunHistoryEntry;

  [[nodiscard]] auto list_runs(std::size_t limit = 50) const
      -> Result<std::vector<RunHistoryEntry>>;
  [[nodiscard]] auto list_dag_runs(const DAGId &dag_id,
                                   std::size_t limit = 50) const
      -> Result<std::vector<RunHistoryEntry>>;
  [[nodiscard]] auto get_run(const DAGRunId &run_id) const
      -> Result<RunHistoryEntry>;

  [[nodiscard]] auto get_task_instances(const DAGRunId &run_id) const
      -> Result<std::vector<TaskInstanceInfo>>;

  [[nodiscard]] auto get_run_logs(const DAGRunId &run_id,
                                  std::size_t limit = 10000) const
      -> Result<std::vector<orm::TaskLogEntry>>;
  [[nodiscard]] auto get_task_logs(const DAGRunId &run_id,
                                   const TaskId &task_id, int attempt = 1,
                                   std::size_t limit = 5000) const
      -> Result<std::vector<orm::TaskLogEntry>>;
  [[nodiscard]] auto get_latest_run(const DAGId &dag_id) const
      -> Result<RunHistoryEntry>;

  [[nodiscard]] auto clear_failed_tasks(const DAGRunId &run_id) -> Result<void>;
  [[nodiscard]] auto clear_all_tasks(const DAGRunId &run_id) -> Result<void>;
  [[nodiscard]] auto clear_task(const DAGRunId &run_id, NodeIndex task_idx)
      -> Result<void>;
  [[nodiscard]] auto set_dag_active(const DAGId &dag_id, bool active)
      -> Result<void>;
  [[nodiscard]] auto set_dag_paused(const DAGId &dag_id, bool paused)
      -> Result<void>;
  [[nodiscard]] auto delete_dag(const DAGId &dag_id) -> Result<void>;

private:
  DatabaseConfig db_config_;
  mutable io::IoContext io_{1};
  mutable storage::MySQLDatabase db_;
};

} // namespace dagforge::cli
