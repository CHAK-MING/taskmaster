#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/config/config.hpp"

#include <nlohmann/json.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct sqlite3;
struct sqlite3_stmt;

namespace taskmaster {

struct DAGInfo;
struct TaskConfig;

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
  [[nodiscard]] auto save_dag_run(const DAGRun& run) -> Result<int64_t>;
  [[nodiscard]] auto update_dag_run_state(const DAGRunId& dag_run_id, DAGRunState state)
      -> Result<void>;
  [[nodiscard]] auto save_task_instance(const DAGRunId& dag_run_id,
                                        const TaskInstanceInfo& info)
      -> Result<void>;
  [[nodiscard]] auto update_task_instance(const DAGRunId& dag_run_id,
                                          const TaskInstanceInfo& info)
      -> Result<void>;

  [[nodiscard]] auto get_dag_run_state(const DAGRunId& dag_run_id) const
      -> Result<DAGRunState>;
  [[nodiscard]] auto get_incomplete_dag_runs() const
      -> Result<std::vector<DAGRunId>>;
  [[nodiscard]] auto get_task_instances(const DAGRunId& dag_run_id) const
      -> Result<std::vector<TaskInstanceInfo>>;

  [[nodiscard]] auto clear_all_dag_data() -> Result<void>;

  // Run History queries
  struct RunHistoryEntry {
    DAGRunId dag_run_id;
    DAGId dag_id;
    DAGRunState state;
    TriggerType trigger_type;
    std::int64_t scheduled_at;
    std::int64_t started_at;
    std::int64_t finished_at;
    std::int64_t execution_date;
  };
  [[nodiscard]] auto list_run_history(std::optional<DAGId> dag_id = std::nullopt,
                                      std::size_t limit = 50) const
      -> Result<std::vector<RunHistoryEntry>>;
  [[nodiscard]] auto get_run_history(const DAGRunId& dag_run_id) const
      -> Result<RunHistoryEntry>;

  // Task Log Entry
  struct TaskLogEntry {
    std::int64_t id;
    DAGRunId dag_run_id;
    TaskId task_id;
    int attempt;
    std::int64_t timestamp;
    std::string level;
    std::string stream;
    std::string message;
  };
  [[nodiscard]] auto save_task_log(const DAGRunId& dag_run_id,
                                   const TaskId& task_id, int attempt,
                                   std::string_view level,
                                   std::string_view stream,
                                   std::string_view message) -> Result<void>;
  [[nodiscard]] auto get_task_logs(const DAGRunId& dag_run_id,
                                   const TaskId& task_id = TaskId{},
                                   int attempt = -1) const
      -> Result<std::vector<TaskLogEntry>>;

  // XCom persistence (cross-task communication)
  [[nodiscard]] auto save_xcom(const DAGRunId& dag_run_id, const TaskId& task_id,
                               std::string_view key,
                               const nlohmann::json& value) -> Result<void>;
  [[nodiscard]] auto get_xcom(const DAGRunId& dag_run_id, const TaskId& task_id,
                              std::string_view key) const
      -> Result<nlohmann::json>;
  [[nodiscard]] auto get_task_xcoms(const DAGRunId& dag_run_id,
                                    const TaskId& task_id) const
      -> Result<std::vector<std::pair<std::string, nlohmann::json>>>;
  [[nodiscard]] auto delete_run_xcoms(const DAGRunId& dag_run_id) -> Result<void>;

  // DAG persistence (for Server mode)
  [[nodiscard]] auto save_dag(const DAGInfo& dag) -> Result<void>;
  [[nodiscard]] auto delete_dag(const DAGId& dag_id) -> Result<void>;
  [[nodiscard]] auto get_dag(const DAGId& dag_id) const -> Result<DAGInfo>;
  [[nodiscard]] auto list_dags() const -> Result<std::vector<DAGInfo>>;

  [[nodiscard]] auto get_last_execution_date(const DAGId& dag_id) const
      -> Result<std::optional<std::chrono::system_clock::time_point>>;

  [[nodiscard]] auto run_exists(const DAGId& dag_id, const std::chrono::system_clock::time_point& execution_time) const -> bool;

  [[nodiscard]] auto has_dag_run(const DAGId& dag_id, std::chrono::system_clock::time_point execution_date) const
      -> Result<bool>;

  // ID mapping helpers for performance optimization
  [[nodiscard]] auto get_run_rowid(const DAGRunId& dag_run_id) const -> Result<int64_t>;

  // Watermark persistence (for scheduling catch-up)
  [[nodiscard]] auto save_watermark(const DAGId& dag_id,
                                    std::chrono::system_clock::time_point timestamp)
      -> Result<void>;
  [[nodiscard]] auto get_watermark(const DAGId& dag_id) const
      -> Result<std::optional<std::chrono::system_clock::time_point>>;

  // Get previous run's task state for depends_on_past feature
  [[nodiscard]] auto get_previous_task_state(
      const DAGId& dag_id, NodeIndex task_idx,
      std::chrono::system_clock::time_point current_execution_date,
      std::string_view current_dag_run_id) const
      -> Result<std::optional<TaskState>>;

  // Task persistence (within DAG)
  [[nodiscard]] auto save_task(const DAGId& dag_id, const TaskConfig& task)
      -> Result<void>;
  [[nodiscard]] auto delete_task(const DAGId& dag_id,
                                 const TaskId& task_id) -> Result<void>;
  [[nodiscard]] auto get_tasks(const DAGId& dag_id) const
      -> Result<std::vector<TaskConfig>>;

  [[nodiscard]] auto
  save_task_instances_batch(const DAGRunId& dag_run_id,
                            const std::vector<TaskInstanceInfo>& instances)
      -> Result<void>;

  [[nodiscard]] auto begin_transaction() -> Result<void>;
  [[nodiscard]] auto commit_transaction() -> Result<void>;
  [[nodiscard]] auto rollback_transaction() -> Result<void>;

private:
  [[nodiscard]] auto create_tables() -> Result<void>;
  [[nodiscard]] auto execute(std::string_view sql) -> Result<void>;
  [[nodiscard]] auto prepare(const char* sql) const -> Result<sqlite3_stmt*>;
  [[nodiscard]] auto get_all_tasks() const
      -> Result<std::unordered_map<std::string, std::vector<TaskConfig>, StringHash, StringEqual>>;

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
  
  // Cached prepared statements for hot paths
  mutable Statement save_dag_run_stmt_{};
  mutable Statement save_task_instance_stmt_{};
};

}  // namespace taskmaster
