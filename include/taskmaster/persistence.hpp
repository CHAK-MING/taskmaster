#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "taskmaster/config.hpp"
#include "taskmaster/dag_run.hpp"
#include "taskmaster/error.hpp"

struct sqlite3;

namespace taskmaster {

struct DAGInfo;

class Persistence {
public:
  explicit Persistence(std::string_view db_path);
  ~Persistence();

  Persistence(const Persistence &) = delete;
  Persistence &operator=(const Persistence &) = delete;

  [[nodiscard]] auto open() -> Result<void>;
  auto close() -> void;
  [[nodiscard]] auto is_open() const noexcept -> bool { return db_ != nullptr; }

  // DAG Run persistence
  [[nodiscard]] auto save_dag_run(const DAGRun &run) -> Result<void>;
  [[nodiscard]] auto update_dag_run_state(std::string_view dag_run_id,
                                          DAGRunState state) -> Result<void>;
  [[nodiscard]] auto save_task_instance(std::string_view dag_run_id,
                                        const TaskInstanceInfo &info)
      -> Result<void>;
  [[nodiscard]] auto update_task_instance(std::string_view dag_run_id,
                                          const TaskInstanceInfo &info)
      -> Result<void>;

  [[nodiscard]] auto get_dag_run_state(std::string_view dag_run_id)
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_incomplete_dag_runs()
      -> Result<std::vector<std::string>>;
  [[nodiscard]] auto get_task_instances(std::string_view dag_run_id)
      -> Result<std::vector<TaskInstanceInfo>>;

  // DAG persistence (for Server mode)
  [[nodiscard]] auto save_dag(const DAGInfo& dag) -> Result<void>;
  [[nodiscard]] auto delete_dag(std::string_view dag_id) -> Result<void>;
  [[nodiscard]] auto get_dag(std::string_view dag_id) -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() -> Result<std::vector<DAGInfo>>;
  
  // Task persistence (within DAG)
  [[nodiscard]] auto save_task(std::string_view dag_id, const TaskConfig& task) -> Result<void>;
  [[nodiscard]] auto delete_task(std::string_view dag_id, std::string_view task_id) -> Result<void>;
  [[nodiscard]] auto get_tasks(std::string_view dag_id) -> Result<std::vector<TaskConfig>>;

  [[nodiscard]] auto begin_transaction() -> Result<void>;
  [[nodiscard]] auto commit_transaction() -> Result<void>;
  [[nodiscard]] auto rollback_transaction() -> Result<void>;

private:
  [[nodiscard]] auto create_tables() -> Result<void>;
  [[nodiscard]] auto execute(std::string_view sql) -> Result<void>;

  struct Sqlite3Deleter {
    void operator()(sqlite3 *db) const;
  };

  std::string db_path_;
  std::unique_ptr<sqlite3, Sqlite3Deleter> db_{nullptr};
};

} // namespace taskmaster
