#include "taskmaster/storage/persistence.hpp"

#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/storage/state_strings.hpp"
#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <charconv>
#include <chrono>
#include <string>
#include <vector>

namespace taskmaster {

namespace {

[[nodiscard]] auto to_timestamp(std::chrono::system_clock::time_point tp) -> uint64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

[[nodiscard]] auto from_timestamp(int64_t ts) -> std::chrono::system_clock::time_point {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(ts));
}

[[nodiscard]] auto col_text(sqlite3_stmt* stmt, int col) -> std::string {
  auto* p = reinterpret_cast<const char*>(sqlite3_column_text(stmt, col));
  return p ? p : "";
}

}  // namespace

auto Persistence::DbDeleter::operator()(sqlite3* db) const -> void {
  if (db)
    sqlite3_close(db);
}

Persistence::Statement::~Statement() {
  reset();
}

auto Persistence::Statement::reset() -> void {
  if (stmt_) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }
}

auto Persistence::prepare(const char* sql) const -> Result<sqlite3_stmt*> {
  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }
  return stmt;
}

namespace {

auto parse_task_from_row(sqlite3_stmt* stmt, int task_id_col, int name_col,
                         int command_col, int working_dir_col, int executor_col,
                         int deps_col, int timeout_col, int retry_interval_col,
                         int max_retries_col) -> TaskConfig {
  auto deps_str = col_text(stmt, deps_col);
  auto executor_str = col_text(stmt, executor_col);

  TaskConfig task{.task_id = TaskId{col_text(stmt, task_id_col)},
                  .name = col_text(stmt, name_col),
                  .command = col_text(stmt, command_col),
                  .working_dir = col_text(stmt, working_dir_col),
                  .dependencies = {},
                  .executor = parse<ExecutorType>(
                      executor_str.empty() ? "shell" : executor_str),
                  .execution_timeout = std::chrono::seconds(sqlite3_column_int(stmt, timeout_col)),
                  .retry_interval = std::chrono::seconds(sqlite3_column_int(stmt, retry_interval_col)),
                  .max_retries = sqlite3_column_int(stmt, max_retries_col),
                  .xcom_push = {},
                  .xcom_pull = {}};

  if (!deps_str.empty()) {
    auto deps_result = nlohmann::json::parse(deps_str, nullptr, false);
    if (deps_result.is_array()) {
      for (const auto& dep : deps_result) {
        if (dep.is_string()) {
          task.dependencies.push_back(TaskDependency{TaskId{dep.get<std::string>()}, ""});
        } else if (dep.is_object() && dep.contains("task")) {
          TaskDependency td;
          td.task_id = TaskId{dep["task"].get<std::string>()};
          td.label = dep.value("label", "");
          task.dependencies.push_back(td);
        }
      }
    }
  }
  return task;
}

}  // namespace

Persistence::Persistence(std::string_view db_path) : db_path_(db_path) {
}

Persistence::~Persistence() {
  close();
}

auto Persistence::open() -> Result<void> {
  if (db_) {
    return ok();
  }

  sqlite3* raw_db = nullptr;
  int rc = sqlite3_open(db_path_.c_str(), &raw_db);
  if (rc != SQLITE_OK) {
    log::error("Failed to open database: {}", sqlite3_errmsg(raw_db));
    if (raw_db) {
      sqlite3_close(raw_db);
    }
    return fail(Error::DatabaseOpenFailed);
  }
  db_.reset(raw_db);

  // PRAGMA statements may fail on some configurations, but we continue anyway
  if (auto r = execute("PRAGMA journal_mode=WAL;"); !r.has_value()) {
    log::warn("Failed to set WAL mode: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA synchronous=NORMAL;"); !r.has_value()) {
    log::warn("Failed to set synchronous mode: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA cache_size=-65536;"); !r.has_value()) {
    log::warn("Failed to set cache size: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA temp_store=MEMORY;"); !r.has_value()) {
    log::warn("Failed to set temp store: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA foreign_keys=ON;"); !r.has_value()) {
    log::warn("Failed to enable foreign keys: {}", r.error().message());
  }

  if (auto r = create_tables(); !r.has_value()) {
    close();
    return r;
  }

  log::info("Database opened: {}", db_path_);
  return ok();
}

auto Persistence::close() -> void {
  db_.reset();
}

auto Persistence::create_tables() -> Result<void> {
  const char* sql = R"(
    -- Optimized dag_runs table: INTEGER rowid + TINYINT states
    CREATE TABLE IF NOT EXISTS dag_runs (
      run_rowid INTEGER PRIMARY KEY AUTOINCREMENT,
      dag_run_id TEXT UNIQUE NOT NULL,
      state TINYINT NOT NULL DEFAULT 0,  -- 0=Running (initial state)
      trigger_type TINYINT NOT NULL DEFAULT 0,  -- 0=Manual (default trigger)
      scheduled_at INTEGER,
      started_at INTEGER,
      finished_at INTEGER,
      execution_date INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_dag_runs_state_scheduled
      ON dag_runs(state, scheduled_at DESC)
      WHERE state IN (0, 1, 2);  -- Running=0, Success=1, Failed=2

    -- Index for history queries and depends_on_past logic
    CREATE INDEX IF NOT EXISTS idx_dag_runs_execution
      ON dag_runs(dag_run_id, execution_date DESC);

    -- Optimized task_instances table: WITHOUT ROWID + TINYINT states
    CREATE TABLE IF NOT EXISTS task_instances (
      run_rowid INTEGER NOT NULL,
      task_idx INTEGER NOT NULL,
      state TINYINT NOT NULL DEFAULT 0,  -- 0=Pending (initial state)
      attempt INTEGER DEFAULT 0,
      started_at INTEGER,
      finished_at INTEGER,
      exit_code INTEGER DEFAULT 0,
      error_message TEXT,
      PRIMARY KEY (run_rowid, task_idx),
      FOREIGN KEY (run_rowid) REFERENCES dag_runs(run_rowid) ON DELETE CASCADE
    ) WITHOUT ROWID;

    CREATE INDEX IF NOT EXISTS idx_task_instances_state
      ON task_instances(run_rowid, state);

    -- Keep existing dags and dag_tasks tables unchanged for now
    CREATE TABLE IF NOT EXISTS dags (
      dag_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      cron TEXT DEFAULT '',
      max_concurrent_runs INTEGER DEFAULT 1,
      is_active INTEGER DEFAULT 1,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dag_tasks (
      dag_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      name TEXT DEFAULT '',
      command TEXT NOT NULL,
      working_dir TEXT DEFAULT '',
      executor TEXT DEFAULT 'shell',
      deps TEXT DEFAULT '[]',
      timeout INTEGER DEFAULT 300,
      retry_interval INTEGER DEFAULT 60,
      max_retries INTEGER DEFAULT 3,
      PRIMARY KEY (dag_id, task_id),
      FOREIGN KEY (dag_id) REFERENCES dags(dag_id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS task_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dag_run_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      attempt INTEGER DEFAULT 1,
      timestamp INTEGER NOT NULL,
      level TEXT DEFAULT 'INFO',
      stream TEXT DEFAULT 'stdout',
      message TEXT NOT NULL,
      FOREIGN KEY (dag_run_id) REFERENCES dag_runs(dag_run_id) ON DELETE CASCADE
    );

    -- REMOVED: idx_task_logs_run_task (redundant - prefix of idx_task_logs_attempt)
    CREATE INDEX IF NOT EXISTS idx_task_logs_attempt
      ON task_logs(dag_run_id, task_id, attempt);

    CREATE TABLE IF NOT EXISTS xcom_values (
      dag_run_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (dag_run_id, task_id, key),
      FOREIGN KEY (dag_run_id) REFERENCES dag_runs(dag_run_id) ON DELETE CASCADE
    );

    -- REMOVED: idx_xcom_run_task (redundant - prefix of PRIMARY KEY)

    CREATE TABLE IF NOT EXISTS dag_watermarks (
      dag_id TEXT PRIMARY KEY,
      last_scheduled_at INTEGER NOT NULL
    );
  )";

  return execute(sql);
}

auto Persistence::execute(std::string_view sql) -> Result<void> {
  char* err_msg = nullptr;
  std::string sql_str{sql};
  int rc = sqlite3_exec(db_.get(), sql_str.c_str(), nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    log::error("SQL error: {}", err_msg ? err_msg : sqlite3_errstr(rc));
    sqlite3_free(err_msg);
    return fail(Error::DatabaseQueryFailed);
  }
  return ok();
}

auto Persistence::save_dag_run(const DAGRun& run) -> Result<int64_t> {
  constexpr auto sql = R"(
    INSERT INTO dag_runs (dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at, execution_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_run_id) DO UPDATE SET
      state = excluded.state,
      trigger_type = excluded.trigger_type,
      scheduled_at = excluded.scheduled_at,
      started_at = excluded.started_at,
      finished_at = excluded.finished_at,
      execution_date = excluded.execution_date
    RETURNING run_rowid;
  )";

  // Use cached prepared statement for performance
  if (!save_dag_run_stmt_) {
    auto result = prepare(sql);
    if (!result)
      return std::unexpected(result.error());
    save_dag_run_stmt_ = Statement(*result);
  }

  // Reset the statement for reuse
  sqlite3_reset(save_dag_run_stmt_.get());
  sqlite3_clear_bindings(save_dag_run_stmt_.get());

  sqlite3_bind_text(save_dag_run_stmt_.get(), 1, run.id().c_str(), -1, SQLITE_TRANSIENT);
  // Use TINYINT for state (integer enum value)
  sqlite3_bind_int(save_dag_run_stmt_.get(), 2, std::to_underlying(run.state()));
  // Use TINYINT for trigger_type
  sqlite3_bind_int(save_dag_run_stmt_.get(), 3, std::to_underlying(run.trigger_type()));
  sqlite3_bind_int64(save_dag_run_stmt_.get(), 4, to_timestamp(run.scheduled_at()));
  sqlite3_bind_int64(save_dag_run_stmt_.get(), 5, to_timestamp(run.started_at()));
  sqlite3_bind_int64(save_dag_run_stmt_.get(), 6, to_timestamp(run.finished_at()));
  sqlite3_bind_int64(save_dag_run_stmt_.get(), 7, to_timestamp(run.execution_date()));

  int rc = sqlite3_step(save_dag_run_stmt_.get());
  if (rc != SQLITE_ROW) {
    log::error("Failed to save dag run {}: {}", run.id().value(), sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }

  // Read the returned rowid (works for both INSERT and UPDATE)
  int64_t rowid = sqlite3_column_int64(save_dag_run_stmt_.get(), 0);
  
  // CRITICAL: Reset statement after reading RETURNING result
  // Without this, the statement remains active and blocks transaction commit
  sqlite3_reset(save_dag_run_stmt_.get());
  
  return rowid;
}

auto Persistence::update_dag_run_state(const DAGRunId& dag_run_id,
                                       DAGRunState state) -> Result<void> {
  constexpr auto sql = "UPDATE dag_runs SET state = ? WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_int(stmt.get(), 1, std::to_underlying(state));
  sqlite3_bind_text(stmt.get(), 2, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    return fail(Error::DatabaseQueryFailed);
  }

  // Check if any rows were actually updated
  if (sqlite3_changes(db_.get()) == 0) {
    return fail(Error::DatabaseQueryFailed);
  }

  return ok();
}

auto Persistence::save_task_instance(const DAGRunId& dag_run_id,
                                     const TaskInstanceInfo& info)
    -> Result<void> {
  // Get run_rowid if not already set in info
  int64_t run_rowid = info.run_rowid;
  if (run_rowid < 0) {
    auto rowid_res = get_run_rowid(dag_run_id);
    if (!rowid_res) return std::unexpected(rowid_res.error());
    run_rowid = *rowid_res;
  }

  constexpr auto sql = R"(
    INSERT INTO task_instances
      (run_rowid, task_idx, state, attempt, started_at, finished_at,
       exit_code, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
  )";

  // Use cached prepared statement (NOTE: This is the SAME as save_task_instance_stmt_)
  // We reuse the batch insert statement since the SQL is identical
  if (!save_task_instance_stmt_) {
    auto result = prepare(sql);
    if (!result)
      return std::unexpected(result.error());
    save_task_instance_stmt_ = Statement(*result);
  }

  // Reset the statement for reuse
  sqlite3_reset(save_task_instance_stmt_.get());
  sqlite3_clear_bindings(save_task_instance_stmt_.get());

  sqlite3_bind_int64(save_task_instance_stmt_.get(), 1, run_rowid);
  sqlite3_bind_int(save_task_instance_stmt_.get(), 2, static_cast<int>(info.task_idx));
  sqlite3_bind_int(save_task_instance_stmt_.get(), 3, std::to_underlying(info.state));
  sqlite3_bind_int(save_task_instance_stmt_.get(), 4, info.attempt);
  sqlite3_bind_int64(save_task_instance_stmt_.get(), 5, to_timestamp(info.started_at));
  sqlite3_bind_int64(save_task_instance_stmt_.get(), 6, to_timestamp(info.finished_at));
  sqlite3_bind_int(save_task_instance_stmt_.get(), 7, info.exit_code);
  sqlite3_bind_text(save_task_instance_stmt_.get(), 8, info.error_message.c_str(), -1,
                    SQLITE_TRANSIENT);

  return sqlite3_step(save_task_instance_stmt_.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_task_instance(const DAGRunId& dag_run_id,
                                       const TaskInstanceInfo& info)
    -> Result<void> {
  // Get run_rowid if not already set in info
  int64_t run_rowid = info.run_rowid;
  if (run_rowid < 0) {
    auto rowid_res = get_run_rowid(dag_run_id);
    if (!rowid_res) return std::unexpected(rowid_res.error());
    run_rowid = *rowid_res;
  }

  constexpr auto sql = R"(
    UPDATE task_instances SET
      state = ?, attempt = ?, started_at = ?, finished_at = ?,
      exit_code = ?, error_message = ?
    WHERE run_rowid = ? AND task_idx = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_int(stmt.get(), 1, std::to_underlying(info.state));
  sqlite3_bind_int(stmt.get(), 2, info.attempt);
  sqlite3_bind_int64(stmt.get(), 3, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt.get(), 5, info.exit_code);
  sqlite3_bind_text(stmt.get(), 6, info.error_message.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 7, run_rowid);
  sqlite3_bind_int(stmt.get(), 8, static_cast<int>(info.task_idx));

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    return fail(Error::DatabaseQueryFailed);
  }

  // Check if any rows were actually updated
  if (sqlite3_changes(db_.get()) == 0) {
    return fail(Error::DatabaseQueryFailed);
  }

  return ok();
}

auto Persistence::get_dag_run_state(const DAGRunId& dag_run_id) const
    -> Result<DAGRunState> {
  constexpr auto sql = "SELECT state FROM dag_runs WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);
  
  int state_int = sqlite3_column_int(stmt.get(), 0);
  return static_cast<DAGRunState>(state_int);
}

auto Persistence::get_run_rowid(const DAGRunId& dag_run_id) const -> Result<int64_t> {
  constexpr auto sql = "SELECT run_rowid FROM dag_runs WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);
  
  return sqlite3_column_int64(stmt.get(), 0);
}

auto Persistence::get_incomplete_dag_runs() const
    -> Result<std::vector<DAGRunId>> {
  // State is now TINYINT: 0=Running, 1=Success, 2=Failed
  // We only want incomplete runs (Running state = 0)
  constexpr auto sql =
      "SELECT dag_run_id FROM dag_runs WHERE state = 0;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::vector<DAGRunId> ids;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    if (auto id = col_text(stmt.get(), 0); !id.empty()) {
      ids.emplace_back(std::move(id));
    }
  }
  return ids;
}

auto Persistence::get_task_instances(const DAGRunId& dag_run_id) const
    -> Result<std::vector<TaskInstanceInfo>> {
  // Get run_rowid from dag_run_id
  auto rowid_res = get_run_rowid(dag_run_id);
  if (!rowid_res) {
    // If DAG run doesn't exist, return empty list instead of error
    if (rowid_res.error() == make_error_code(Error::NotFound)) {
      return std::vector<TaskInstanceInfo>{};
    }
    return std::unexpected(rowid_res.error());
  }
  int64_t run_rowid = *rowid_res;

  constexpr auto sql = R"(
    SELECT task_idx, state, attempt, started_at, finished_at,
           exit_code, error_message
    FROM task_instances WHERE run_rowid = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_int64(stmt.get(), 1, run_rowid);

  std::vector<TaskInstanceInfo> instances;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto task_idx = static_cast<NodeIndex>(sqlite3_column_int(stmt.get(), 0));
    auto state_int = sqlite3_column_int(stmt.get(), 1);
    TaskState state = static_cast<TaskState>(state_int);
    
    instances.push_back(
        {.instance_id = InstanceId{},  // No longer stored in DB
         .task_idx = task_idx,
         .state = state,
         .attempt = sqlite3_column_int(stmt.get(), 2),
         .started_at = from_timestamp(sqlite3_column_int64(stmt.get(), 3)),
         .finished_at = from_timestamp(sqlite3_column_int64(stmt.get(), 4)),
         .exit_code = sqlite3_column_int(stmt.get(), 5),
         .error_message = col_text(stmt.get(), 6),
         .run_rowid = run_rowid});
  }
  return instances;
}

auto Persistence::list_run_history(std::optional<DAGId> dag_id, std::size_t limit) const
    -> Result<std::vector<RunHistoryEntry>> {
  std::string sql;
  if (!dag_id.has_value() || dag_id->value().empty()) {
    sql = R"(
      SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at, execution_date
      FROM dag_runs ORDER BY scheduled_at DESC LIMIT ?;
    )";
  } else {
    sql = R"(
      SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at, execution_date
      FROM dag_runs WHERE dag_run_id LIKE ? ORDER BY scheduled_at DESC LIMIT ?;
    )";
  }

  auto result = prepare(sql.c_str());
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  if (!dag_id.has_value() || dag_id->value().empty()) {
    sqlite3_bind_int64(stmt.get(), 1, static_cast<sqlite3_int64>(limit));
  } else {
  std::string pattern = std::format("{}_%", dag_id->value());
    sqlite3_bind_text(stmt.get(), 1, pattern.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt.get(), 2, static_cast<sqlite3_int64>(limit));
  }

  std::vector<RunHistoryEntry> entries;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto dag_run_id_str = col_text(stmt.get(), 0);
    std::string extracted_dag_id_str;
    if (auto pos = dag_run_id_str.rfind('_'); pos != std::string::npos) {
      extracted_dag_id_str = dag_run_id_str.substr(0, pos);
    }

    DAGRunId run_id(std::move(dag_run_id_str));
    DAGId extracted_dag_id(std::move(extracted_dag_id_str));
    // State and trigger_type are now TINYINT, not TEXT
    int state_int = sqlite3_column_int(stmt.get(), 1);
    int trigger_int = sqlite3_column_int(stmt.get(), 2);
    entries.push_back(
        {.dag_run_id = std::move(run_id),
         .dag_id = std::move(extracted_dag_id),
         .state = static_cast<DAGRunState>(state_int),
         .trigger_type = static_cast<TriggerType>(trigger_int),
         .scheduled_at = sqlite3_column_int64(stmt.get(), 3),
         .started_at = sqlite3_column_int64(stmt.get(), 4),
         .finished_at = sqlite3_column_int64(stmt.get(), 5),
         .execution_date = sqlite3_column_int64(stmt.get(), 6)});
  }
  return entries;
}

auto Persistence::get_run_history(const DAGRunId& dag_run_id) const
    -> Result<RunHistoryEntry> {
  constexpr auto sql = R"(
    SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at, execution_date
    FROM dag_runs WHERE dag_run_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
    return std::unexpected(make_error_code(Error::NotFound));
  }

  auto fetched_run_id_str = col_text(stmt.get(), 0);
  std::string extracted_dag_id_str;
  if (auto pos = fetched_run_id_str.rfind('_'); pos != std::string::npos) {
    extracted_dag_id_str = fetched_run_id_str.substr(0, pos);
  }

  DAGRunId run_id(std::move(fetched_run_id_str));
  DAGId dag_id(std::move(extracted_dag_id_str));
  // State and trigger_type are now TINYINT, not TEXT
  int state_int = sqlite3_column_int(stmt.get(), 1);
  int trigger_int = sqlite3_column_int(stmt.get(), 2);
  return RunHistoryEntry{.dag_run_id = std::move(run_id),
                         .dag_id = std::move(dag_id),
                         .state = static_cast<DAGRunState>(state_int),
                         .trigger_type = static_cast<TriggerType>(trigger_int),
                         .scheduled_at = sqlite3_column_int64(stmt.get(), 3),
                         .started_at = sqlite3_column_int64(stmt.get(), 4),
                         .finished_at = sqlite3_column_int64(stmt.get(), 5),
                         .execution_date = sqlite3_column_int64(stmt.get(), 6)};
}

auto Persistence::begin_transaction() -> Result<void> {
  return execute("BEGIN TRANSACTION;");
}

auto Persistence::commit_transaction() -> Result<void> {
  return execute("COMMIT;");
}

auto Persistence::rollback_transaction() -> Result<void> {
  return execute("ROLLBACK;");
}

auto Persistence::save_dag(const DAGInfo& info) -> Result<void> {
  if (auto r = begin_transaction(); !r.has_value()) {
    return r;
  }

  constexpr auto sql = R"(
    INSERT INTO dags (dag_id, name, description, cron, max_concurrent_runs, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_id) DO UPDATE SET
      name = excluded.name, description = excluded.description, cron = excluded.cron,
      max_concurrent_runs = excluded.max_concurrent_runs, updated_at = excluded.updated_at;
  )";

  auto result = prepare(sql);
  if (!result) {
    if (auto r = rollback_transaction(); !r.has_value()) {
      return r;
    }
    return std::unexpected(result.error());
  }
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, info.dag_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, info.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, info.description.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, info.cron.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 5, info.max_concurrent_runs);
  sqlite3_bind_int64(stmt.get(), 6, to_timestamp(info.created_at));
  sqlite3_bind_int64(stmt.get(), 7, to_timestamp(info.updated_at));

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    if (auto r = rollback_transaction(); !r.has_value()) {
      log::error("Failed to rollback transaction: {}", r.error().message());
      return r;
    }
    return fail(Error::DatabaseQueryFailed);
  }

  if (auto r = commit_transaction(); !r.has_value()) {
    log::error("Failed to commit transaction: {}", r.error().message());
    return r;
  }
  return ok();
}

auto Persistence::delete_dag(const DAGId& dag_id) -> Result<void> {
  constexpr auto sql = "DELETE FROM dags WHERE dag_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_dag(const DAGId& dag_id) const -> Result<DAGInfo> {
  constexpr auto sql =
      "SELECT dag_id, name, description, cron, max_concurrent_runs, "
      "created_at, updated_at FROM dags WHERE dag_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);

  DAGInfo dag{.dag_id = DAGId{col_text(stmt.get(), 0)},
              .name = col_text(stmt.get(), 1),
              .description = col_text(stmt.get(), 2),
              .cron = col_text(stmt.get(), 3),
              .max_concurrent_runs = sqlite3_column_int(stmt.get(), 4),
              .created_at = from_timestamp(sqlite3_column_int64(stmt.get(), 5)),
              .updated_at = from_timestamp(sqlite3_column_int64(stmt.get(), 6)),
              .tasks = {},
              .task_index = {},
              .reverse_adj_cache = {}};

  if (auto tasks_result = get_tasks(dag_id))
    dag.tasks = std::move(*tasks_result);
  return dag;
}

auto Persistence::list_dags() const -> Result<std::vector<DAGInfo>> {
  constexpr auto sql =
      "SELECT dag_id, name, description, cron, max_concurrent_runs, "
      "created_at, updated_at FROM dags;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  // Batch load all tasks to avoid N+1 queries
  auto all_tasks_result = get_all_tasks();
  if (!all_tasks_result)
    return std::unexpected(all_tasks_result.error());
  auto& all_tasks = *all_tasks_result;

  std::vector<DAGInfo> dags;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto dag_id_str = col_text(stmt.get(), 0);
    DAGInfo dag{
        .dag_id = DAGId{dag_id_str},
        .name = col_text(stmt.get(), 1),
        .description = col_text(stmt.get(), 2),
        .cron = col_text(stmt.get(), 3),
        .max_concurrent_runs = sqlite3_column_int(stmt.get(), 4),
        .created_at = from_timestamp(sqlite3_column_int64(stmt.get(), 5)),
        .updated_at = from_timestamp(sqlite3_column_int64(stmt.get(), 6)),
        .tasks = {},
        .task_index = {},
        .reverse_adj_cache = {}};
    if (auto it = all_tasks.find(dag_id_str); it != all_tasks.end()) {
      dag.tasks = std::move(it->second);
    }
    dags.push_back(std::move(dag));
  }

  return dags;
}

auto Persistence::save_task(const DAGId& dag_id, const TaskConfig& task)
    -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO dag_tasks (dag_id, task_id, name, command, working_dir, executor, deps, timeout, retry_interval, max_retries)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_id, task_id) DO UPDATE SET
      name = excluded.name, command = excluded.command, executor = excluded.executor,
      working_dir = excluded.working_dir, deps = excluded.deps, timeout = excluded.timeout,
      retry_interval = excluded.retry_interval, max_retries = excluded.max_retries
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  nlohmann::json deps_json = nlohmann::json::array();
  for (const auto& dep : task.dependencies) {
    if (dep.label.empty()) {
      deps_json.push_back(dep.task_id.value());
    } else {
      deps_json.push_back({{"task", dep.task_id.value()}, {"label", dep.label}});
    }
  }
  std::string deps_str = deps_json.dump();
  std::string executor_str(to_string_view(task.executor));

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task.task_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, task.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, task.command.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 5, task.working_dir.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 6, executor_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 7, deps_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 8, static_cast<int>(task.execution_timeout.count()));
  sqlite3_bind_int(stmt.get(), 9,
                   static_cast<int>(task.retry_interval.count()));
  sqlite3_bind_int(stmt.get(), 10, task.max_retries);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::delete_task(const DAGId& dag_id, const TaskId& task_id)
    -> Result<void> {
  constexpr auto sql =
      "DELETE FROM dag_tasks WHERE dag_id = ? AND task_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_id.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_tasks(const DAGId& dag_id) const
    -> Result<std::vector<TaskConfig>> {
  constexpr auto sql = R"(
    SELECT task_id, name, command, working_dir, executor, deps, timeout, retry_interval, max_retries
    FROM dag_tasks WHERE dag_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<TaskConfig> tasks;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    tasks.push_back(parse_task_from_row(stmt.get(), 0, 1, 2, 4, 3, 5, 6, 7, 8));
  }
  return tasks;
}

auto Persistence::get_all_tasks() const
    -> Result<std::unordered_map<std::string, std::vector<TaskConfig>, StringHash, StringEqual>> {
  constexpr auto sql = R"(
    SELECT dag_id, task_id, name, command, working_dir, executor, deps, timeout, retry_interval, max_retries
    FROM dag_tasks;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::unordered_map<std::string, std::vector<TaskConfig>, StringHash, StringEqual> tasks_by_dag;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto dag_id_str = col_text(stmt.get(), 0);
    auto task = parse_task_from_row(stmt.get(), 1, 2, 3, 4, 5, 6, 7, 8, 9);
    tasks_by_dag[dag_id_str].push_back(std::move(task));
  }
  return tasks_by_dag;
}


auto Persistence::save_task_instances_batch(
    const DAGRunId& dag_run_id, const std::vector<TaskInstanceInfo>& instances)
    -> Result<void> {
  if (instances.empty()) {
    return ok();
  }

  // Get run_rowid ONCE before the loop (optimization!)
  auto rowid_res = get_run_rowid(dag_run_id);
  if (!rowid_res) return std::unexpected(rowid_res.error());
  int64_t run_rowid = *rowid_res;

  if (auto r = begin_transaction(); !r.has_value()) {
    return r;
  }

  constexpr auto sql = R"(
    INSERT INTO task_instances
      (run_rowid, task_idx, state, attempt, started_at, finished_at,
       exit_code, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
  )";

  // Use cached prepared statement for performance
  if (!save_task_instance_stmt_) {
    auto result = prepare(sql);
    if (!result) {
      (void)rollback_transaction();
      return std::unexpected(result.error());
    }
    save_task_instance_stmt_ = Statement(*result);
  }

  for (const auto& info : instances) {
    // Reset and clear bindings before each use
    sqlite3_reset(save_task_instance_stmt_.get());
    sqlite3_clear_bindings(save_task_instance_stmt_.get());

    sqlite3_bind_int64(save_task_instance_stmt_.get(), 1, run_rowid);
    sqlite3_bind_int(save_task_instance_stmt_.get(), 2, static_cast<int>(info.task_idx));
    sqlite3_bind_int(save_task_instance_stmt_.get(), 3, std::to_underlying(info.state));
    sqlite3_bind_int(save_task_instance_stmt_.get(), 4, info.attempt);
    sqlite3_bind_int64(save_task_instance_stmt_.get(), 5, to_timestamp(info.started_at));
    sqlite3_bind_int64(save_task_instance_stmt_.get(), 6, to_timestamp(info.finished_at));
    sqlite3_bind_int(save_task_instance_stmt_.get(), 7, info.exit_code);
    sqlite3_bind_text(save_task_instance_stmt_.get(), 8, info.error_message.c_str(), -1,
                      SQLITE_TRANSIENT);

    if (sqlite3_step(save_task_instance_stmt_.get()) != SQLITE_DONE) {
      log::error("Failed to save task instance at task_idx {} in batch: {}",
                 info.task_idx, sqlite3_errmsg(db_.get()));
      (void)rollback_transaction();
      return fail(Error::DatabaseQueryFailed);
    }
  }

  if (auto r = commit_transaction(); !r.has_value()) {
    return r;
  }

  return ok();
}

auto Persistence::save_task_log(const DAGRunId& dag_run_id,
                                const TaskId& task_id, int attempt,
                                std::string_view level, std::string_view stream,
                                std::string_view message) -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO task_logs (dag_run_id, task_id, attempt, timestamp, level, stream, message)
    VALUES (?, ?, ?, ?, ?, ?, ?);
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                 std::chrono::system_clock::now().time_since_epoch())
                 .count();

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(),
                    static_cast<int>(dag_run_id.size()), SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_id.c_str(),
                    static_cast<int>(task_id.size()), SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 3, attempt);
  sqlite3_bind_int64(stmt.get(), 4, now);
  sqlite3_bind_text(stmt.get(), 5, level.data(), static_cast<int>(level.size()),
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 6, stream.data(),
                    static_cast<int>(stream.size()), SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 7, message.data(),
                    static_cast<int>(message.size()), SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    log::error("Failed to save task log: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }
  return ok();
}

auto Persistence::get_task_logs(const DAGRunId& dag_run_id,
                                const TaskId& task_id, int attempt) const
    -> Result<std::vector<TaskLogEntry>> {
  std::string sql;
  if (task_id.empty()) {
    sql = R"(
      SELECT id, dag_run_id, task_id, attempt, timestamp, level, stream, message
      FROM task_logs WHERE dag_run_id = ? ORDER BY attempt ASC, timestamp ASC;
    )";
  } else if (attempt < 0) {
    sql = R"(
      SELECT id, dag_run_id, task_id, attempt, timestamp, level, stream, message
      FROM task_logs WHERE dag_run_id = ? AND task_id = ? ORDER BY attempt ASC, timestamp ASC;
    )";
  } else {
    sql = R"(
      SELECT id, dag_run_id, task_id, attempt, timestamp, level, stream, message
      FROM task_logs WHERE dag_run_id = ? AND task_id = ? AND attempt = ? ORDER BY timestamp ASC;
    )";
  }

  auto result = prepare(sql.c_str());
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  int param_idx = 1;
  sqlite3_bind_text(stmt.get(), param_idx++, dag_run_id.c_str(),
                    static_cast<int>(dag_run_id.size()), SQLITE_TRANSIENT);
  if (!task_id.empty()) {
    sqlite3_bind_text(stmt.get(), param_idx++, task_id.c_str(),
                      static_cast<int>(task_id.size()), SQLITE_TRANSIENT);
    if (attempt >= 0) {
      sqlite3_bind_int(stmt.get(), param_idx++, attempt);
    }
  }

  std::vector<TaskLogEntry> logs;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    TaskLogEntry entry;
    entry.id = sqlite3_column_int64(stmt.get(), 0);
    // Reuse the parameters instead of reconstructing from DB
    entry.dag_run_id = dag_run_id;
    entry.task_id = task_id.empty() ? TaskId{col_text(stmt.get(), 2)} : task_id;
    entry.attempt = sqlite3_column_int(stmt.get(), 3);
    entry.timestamp = sqlite3_column_int64(stmt.get(), 4);
    auto lvl = col_text(stmt.get(), 5);
    auto strm = col_text(stmt.get(), 6);
    entry.level = lvl.empty() ? "INFO" : std::move(lvl);
    entry.stream = strm.empty() ? "stdout" : std::move(strm);
    entry.message = col_text(stmt.get(), 7);
    logs.push_back(std::move(entry));
  }
  return logs;
}

auto Persistence::clear_all_dag_data() -> Result<void> {
  if (auto r = begin_transaction(); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM xcom_values;"); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM task_logs;"); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM task_instances;"); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM dag_runs;"); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM dag_tasks;"); !r.has_value()) return r;
  if (auto r = execute("DELETE FROM dags;"); !r.has_value()) return r;
  return commit_transaction();
}

auto Persistence::save_xcom(const DAGRunId& dag_run_id, const TaskId& task_id,
                            std::string_view key,
                            const nlohmann::json& value) -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO xcom_values (dag_run_id, task_id, key, value, created_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(dag_run_id, task_id, key) DO UPDATE SET
      value = excluded.value, created_at = excluded.created_at;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  auto now = to_timestamp(std::chrono::system_clock::now());
  std::string value_str = value.dump();

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, key.data(), static_cast<int>(key.size()),
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, value_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 5, now);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_xcom(const DAGRunId& dag_run_id, const TaskId& task_id,
                           std::string_view key) const
    -> Result<nlohmann::json> {
  constexpr auto sql =
      "SELECT value FROM xcom_values WHERE dag_run_id = ? AND task_id = ? AND key = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, key.data(), static_cast<int>(key.size()),
                    SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);

  auto value_str = col_text(stmt.get(), 0);
  auto parsed = nlohmann::json::parse(value_str, nullptr, false);
  if (parsed.is_discarded())
    return fail(Error::InvalidArgument);

  return parsed;
}

auto Persistence::get_task_xcoms(const DAGRunId& dag_run_id, const TaskId& task_id) const
    -> Result<std::vector<std::pair<std::string, nlohmann::json>>> {
  constexpr auto sql =
      "SELECT key, value FROM xcom_values WHERE dag_run_id = ? AND task_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_id.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<std::pair<std::string, nlohmann::json>> xcoms;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto key = col_text(stmt.get(), 0);
    auto value_str = col_text(stmt.get(), 1);
    auto parsed = nlohmann::json::parse(value_str, nullptr, false);
    if (!parsed.is_discarded()) {
      xcoms.emplace_back(std::move(key), std::move(parsed));
    }
  }
  return xcoms;
}

auto Persistence::delete_run_xcoms(const DAGRunId& dag_run_id) -> Result<void> {
  constexpr auto sql = "DELETE FROM xcom_values WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_last_execution_date(const DAGId& dag_id) const
    -> Result<std::optional<std::chrono::system_clock::time_point>> {
  const char* sql = R"(
    SELECT MAX(execution_date) FROM dag_runs WHERE dag_run_id LIKE ?
  )";

  auto stmt = prepare(sql);
  if (!stmt) return fail(Error::DatabaseQueryFailed);

  std::string pattern = std::format("{}_%", dag_id.value());
  auto raw_stmt = *stmt;
  auto rc = sqlite3_bind_text(raw_stmt, 1, pattern.c_str(),
                              static_cast<int>(pattern.size()),
                              SQLITE_TRANSIENT);
  if (rc != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  rc = sqlite3_step(raw_stmt);
  if (rc == SQLITE_ROW) {
    if (sqlite3_column_type(raw_stmt, 0) == SQLITE_NULL) {
      return std::nullopt;
    }
    auto timestamp = sqlite3_column_int64(raw_stmt, 0);
    return from_timestamp(timestamp);
  }
  
  if (rc == SQLITE_DONE) {
    return std::nullopt;
  }

  return fail(Error::DatabaseQueryFailed);
}

auto Persistence::run_exists(const DAGId& dag_id,
                             const std::chrono::system_clock::time_point& execution_time) const -> bool {
  auto result = has_dag_run(dag_id, execution_time);
  return result.value_or(false);
}

auto Persistence::has_dag_run(const DAGId& dag_id,
                              std::chrono::system_clock::time_point execution_date) const
    -> Result<bool> {
  constexpr auto sql =
      "SELECT 1 FROM dag_runs WHERE dag_run_id LIKE ? AND execution_date = ? LIMIT 1;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string pattern = std::format("{}_%", dag_id.value());
  sqlite3_bind_text(stmt.get(), 1, pattern.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 2, to_timestamp(execution_date));

  return sqlite3_step(stmt.get()) == SQLITE_ROW;
}

auto Persistence::get_previous_task_state(
    const DAGId& dag_id, NodeIndex task_idx,
    std::chrono::system_clock::time_point current_execution_date,
    std::string_view current_dag_run_id) const
    -> Result<std::optional<TaskState>> {
  const char* sql = R"(
    SELECT ti.state FROM task_instances ti
    JOIN dag_runs dr ON ti.run_rowid = dr.run_rowid
    WHERE dr.dag_run_id LIKE ? AND ti.task_idx = ? AND dr.execution_date <= ? AND dr.dag_run_id != ?
    ORDER BY dr.execution_date DESC, dr.dag_run_id DESC LIMIT 1
  )";

  auto stmt_result = prepare(sql);
  if (!stmt_result) return fail(Error::DatabaseQueryFailed);
  Statement stmt(*stmt_result);

  std::string pattern = std::format("{}_%", dag_id.value());
  auto current_ts = to_timestamp(current_execution_date);

  sqlite3_bind_text(stmt.get(), 1, pattern.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 2, static_cast<int>(task_idx));
  sqlite3_bind_int64(stmt.get(), 3, current_ts);
  sqlite3_bind_text(stmt.get(), 4, current_dag_run_id.data(), static_cast<int>(current_dag_run_id.size()), SQLITE_TRANSIENT);

  auto rc = sqlite3_step(stmt.get());
  if (rc == SQLITE_ROW) {
    int state_int = sqlite3_column_int(stmt.get(), 0);
    return static_cast<TaskState>(state_int);
  }
  
  if (rc == SQLITE_DONE) {
    return std::nullopt;
  }

  return fail(Error::DatabaseQueryFailed);
}

auto Persistence::save_watermark(const DAGId& dag_id,
                                 std::chrono::system_clock::time_point timestamp)
    -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO dag_watermarks (dag_id, last_scheduled_at)
    VALUES (?, ?)
    ON CONFLICT(dag_id) DO UPDATE SET
      last_scheduled_at = excluded.last_scheduled_at;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 2, to_timestamp(timestamp));

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_watermark(const DAGId& dag_id) const
    -> Result<std::optional<std::chrono::system_clock::time_point>> {
  constexpr auto sql =
      "SELECT last_scheduled_at FROM dag_watermarks WHERE dag_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return std::nullopt;

  return from_timestamp(sqlite3_column_int64(stmt.get(), 0));
}

}  // namespace taskmaster
