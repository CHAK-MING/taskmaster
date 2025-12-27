#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/config.hpp"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct sqlite3;
struct sqlite3_stmt;

namespace taskmaster {

struct DAGInfo;

class Persistence {
public:
  explicit Persistence(std::string_view db_path);
  ~Persistence();

  Persistence(const Persistence&) = delete;
  Persistence& operator=(const Persistence&) = delete;

  [[nodiscard]] auto open() -> Result<void>;
  auto close() -> void;
  [[nodiscard]] auto is_open() const noexcept -> bool {
    return db_ != nullptr;
  }

  // DAG Run persistence
  [[nodiscard]] auto save_dag_run(const DAGRun& run) -> Result<void>;
  [[nodiscard]] auto update_dag_run_state(std::string_view dag_run_id,
                                          DAGRunState state) -> Result<void>;
  [[nodiscard]] auto save_task_instance(std::string_view dag_run_id,
                                        const TaskInstanceInfo& info)
      -> Result<void>;
  [[nodiscard]] auto update_task_instance(std::string_view dag_run_id,
                                          const TaskInstanceInfo& info)
      -> Result<void>;

  [[nodiscard]] auto get_dag_run_state(std::string_view dag_run_id)
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_incomplete_dag_runs()
      -> Result<std::vector<std::string>>;
  [[nodiscard]] auto get_task_instances(std::string_view dag_run_id)
      -> Result<std::vector<TaskInstanceInfo>>;

  // Run History queries
  struct RunHistoryEntry {
    std::string run_id;
    std::string dag_id;
    DAGRunState state;
    TriggerType trigger_type;
    std::int64_t scheduled_at;
    std::int64_t started_at;
    std::int64_t finished_at;
  };
  [[nodiscard]] auto list_run_history(std::string_view dag_id = "",
                                      std::size_t limit = 50)
      -> Result<std::vector<RunHistoryEntry>>;
  [[nodiscard]] auto get_run_history(std::string_view run_id)
      -> Result<RunHistoryEntry>;

  // Task Log Entry
  struct TaskLogEntry {
    std::int64_t id;
    std::string run_id;
    std::string task_id;
    int attempt;
    std::int64_t timestamp;
    std::string level;
    std::string stream;
    std::string message;
  };
  [[nodiscard]] auto save_task_log(std::string_view run_id,
                                   std::string_view task_id, int attempt,
                                   std::string_view level,
                                   std::string_view stream,
                                   std::string_view message) -> Result<void>;
  [[nodiscard]] auto get_task_logs(std::string_view run_id,
                                   std::string_view task_id = "",
                                   int attempt = -1)
      -> Result<std::vector<TaskLogEntry>>;

  // DAG persistence (for Server mode)
  [[nodiscard]] auto save_dag(const DAGInfo& dag) -> Result<void>;
  [[nodiscard]] auto delete_dag(std::string_view dag_id) -> Result<void>;
  [[nodiscard]] auto get_dag(std::string_view dag_id) -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() -> Result<std::vector<DAGInfo>>;

  // Task persistence (within DAG)
  [[nodiscard]] auto save_task(std::string_view dag_id, const TaskConfig& task)
      -> Result<void>;
  [[nodiscard]] auto delete_task(std::string_view dag_id,
                                 std::string_view task_id) -> Result<void>;
  [[nodiscard]] auto get_tasks(std::string_view dag_id)
      -> Result<std::vector<TaskConfig>>;

  // Batch operations for better performance
  [[nodiscard]] auto save_tasks_batch(std::string_view dag_id,
                                      const std::vector<TaskConfig>& tasks)
      -> Result<void>;
  [[nodiscard]] auto
  save_task_instances_batch(std::string_view dag_run_id,
                            const std::vector<TaskInstanceInfo>& instances)
      -> Result<void>;

  [[nodiscard]] auto begin_transaction() -> Result<void>;
  [[nodiscard]] auto commit_transaction() -> Result<void>;
  [[nodiscard]] auto rollback_transaction() -> Result<void>;

private:
  [[nodiscard]] auto create_tables() -> Result<void>;
  [[nodiscard]] auto execute(std::string_view sql) -> Result<void>;
  [[nodiscard]] auto prepare(const char* sql) -> Result<sqlite3_stmt*>;

  struct DbDeleter {
    void operator()(sqlite3* db) const;
  };

  class Statement {
  public:
    explicit Statement(sqlite3_stmt* stmt = nullptr) noexcept : stmt_(stmt) {
    }
    ~Statement();
    Statement(Statement&& other) noexcept
        : stmt_(std::exchange(other.stmt_, nullptr)) {
    }
    Statement& operator=(Statement&& other) noexcept {
      if (this != &other) {
        reset();
        stmt_ = std::exchange(other.stmt_, nullptr);
      }
      return *this;
    }
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    [[nodiscard]] auto get() const noexcept -> sqlite3_stmt* {
      return stmt_;
    }
    [[nodiscard]] explicit operator bool() const noexcept {
      return stmt_ != nullptr;
    }
    auto reset() -> void;

  private:
    sqlite3_stmt* stmt_ = nullptr;
  };

  std::string db_path_;
  std::unique_ptr<sqlite3, DbDeleter> db_{nullptr};
};

}  // namespace taskmaster
