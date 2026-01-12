#include "taskmaster/storage/persistence.hpp"

#include "taskmaster/dag/dag_manager.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/util/id.hpp"
#include "taskmaster/util/log.hpp"

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <utility>

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

constexpr std::array kDagRunStateNames = {"running", "success", "failed"};
constexpr std::array kTaskStateNames = {
    "pending", "running", "success", "failed", "upstream_failed", "retrying"};

[[nodiscard]] auto dag_run_state_to_string(DAGRunState state) -> const char* {
  auto idx = std::to_underlying(state);
  return idx < kDagRunStateNames.size() ? kDagRunStateNames[idx] : "unknown";
}

[[nodiscard]] auto string_to_dag_run_state(std::string_view s) -> DAGRunState {
  auto it = std::ranges::find(kDagRunStateNames, s);
  if (it != kDagRunStateNames.end()) {
    return static_cast<DAGRunState>(
        std::ranges::distance(kDagRunStateNames.begin(), it));
  }
  return DAGRunState::Running;
}

[[nodiscard]] auto task_state_to_string(TaskState state) -> const char* {
  auto idx = std::to_underlying(state);
  return idx < kTaskStateNames.size() ? kTaskStateNames[idx] : "unknown";
}

[[nodiscard]] auto string_to_task_state(std::string_view s) -> TaskState {
  auto it = std::ranges::find(kTaskStateNames, s);
  if (it != kTaskStateNames.end()) {
    return static_cast<TaskState>(
        std::ranges::distance(kTaskStateNames.begin(), it));
  }
  return TaskState::Pending;
}

// Helper to safely get text from sqlite column
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

auto Persistence::prepare(const char* sql) -> Result<sqlite3_stmt*> {
  sqlite3_stmt* stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }
  return stmt;
}

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
  if (auto r = execute("PRAGMA journal_mode=WAL;"); !r) {
    log::warn("Failed to set WAL mode: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA synchronous=NORMAL;"); !r) {
    log::warn("Failed to set synchronous mode: {}", r.error().message());
  }
  if (auto r = execute("PRAGMA foreign_keys=ON;"); !r) {
    log::warn("Failed to enable foreign keys: {}", r.error().message());
  }

  if (auto r = create_tables(); !r) {
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
    CREATE TABLE IF NOT EXISTS dag_runs (
      dag_run_id TEXT PRIMARY KEY,
      state TEXT NOT NULL DEFAULT 'pending',
      trigger_type TEXT NOT NULL DEFAULT 'manual',
      scheduled_at INTEGER,
      started_at INTEGER,
      finished_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS task_instances (
      id TEXT PRIMARY KEY,
      dag_run_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'pending',
      attempt INTEGER DEFAULT 0,
      started_at INTEGER,
      finished_at INTEGER,
      exit_code INTEGER DEFAULT 0,
      error_message TEXT,
      FOREIGN KEY (dag_run_id) REFERENCES dag_runs(dag_run_id)
    );

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

    CREATE INDEX IF NOT EXISTS idx_task_instances_dag_run
      ON task_instances(dag_run_id);
    CREATE INDEX IF NOT EXISTS idx_dag_runs_state
      ON dag_runs(state);

    CREATE TABLE IF NOT EXISTS task_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      dag_run_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      attempt INTEGER DEFAULT 1,
      timestamp INTEGER NOT NULL,
      level TEXT DEFAULT 'INFO',
      stream TEXT DEFAULT 'stdout',
      message TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_task_logs_run_task
      ON task_logs(dag_run_id, task_id);
    CREATE INDEX IF NOT EXISTS idx_task_logs_attempt
      ON task_logs(dag_run_id, task_id, attempt);
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

auto Persistence::save_dag_run(const DAGRun& run) -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO dag_runs (dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_run_id) DO UPDATE SET
      state = excluded.state,
      trigger_type = excluded.trigger_type,
      scheduled_at = excluded.scheduled_at,
      started_at = excluded.started_at,
      finished_at = excluded.finished_at;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, run.id().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, dag_run_state_to_string(run.state()), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3,
                    std::string(trigger_type_to_string(run.trigger_type())).c_str(),
                    -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(run.scheduled_at()));
  sqlite3_bind_int64(stmt.get(), 5, to_timestamp(run.started_at()));
  sqlite3_bind_int64(stmt.get(), 6, to_timestamp(run.finished_at()));

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_dag_run_state(DAGRunId dag_run_id,
                                       DAGRunState state) -> Result<void> {
  constexpr auto sql = "UPDATE dag_runs SET state = ? WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, dag_run_state_to_string(state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, id_str.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    return fail(Error::DatabaseQueryFailed);
  }

  // Check if any rows were actually updated
  if (sqlite3_changes(db_.get()) == 0) {
    return fail(Error::DatabaseQueryFailed);
  }

  return ok();
}

auto Persistence::save_task_instance(DAGRunId dag_run_id,
                                     const TaskInstanceInfo& info)
    -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO task_instances
      (id, dag_run_id, task_id, state, attempt, started_at, finished_at,
       exit_code, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  if(info.instance_id.empty()) {
    return fail(Error::InvalidArgument);
  }

  sqlite3_bind_text(stmt.get(), 1, info.instance_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
  auto task_idx_str = std::to_string(info.task_idx);
  sqlite3_bind_text(stmt.get(), 3, task_idx_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, task_state_to_string(info.state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 5, info.attempt);
  sqlite3_bind_int64(stmt.get(), 6, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt.get(), 7, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt.get(), 8, info.exit_code);
  sqlite3_bind_text(stmt.get(), 9, info.error_message.c_str(), -1,
                    SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_task_instance(DAGRunId dag_run_id,
                                       const TaskInstanceInfo& info)
    -> Result<void> {
  constexpr auto sql = R"(
    UPDATE task_instances SET
      id = ?, state = ?, attempt = ?, started_at = ?, finished_at = ?,
      exit_code = ?, error_message = ?
    WHERE dag_run_id = ? AND task_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  if (info.instance_id.empty()) {
    return fail(Error::InvalidArgument);
  }

  sqlite3_bind_text(stmt.get(), 1, info.instance_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_state_to_string(info.state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 3, info.attempt);
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt.get(), 5, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt.get(), 6, info.exit_code);
  sqlite3_bind_text(stmt.get(), 7, info.error_message.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 8, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
  auto task_idx_str = std::to_string(info.task_idx);
  sqlite3_bind_text(stmt.get(), 9, task_idx_str.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    return fail(Error::DatabaseQueryFailed);
  }

  // Check if any rows were actually updated
  if (sqlite3_changes(db_.get()) == 0) {
    return fail(Error::DatabaseQueryFailed);
  }

  return ok();
}

auto Persistence::get_dag_run_state(DAGRunId dag_run_id)
    -> Result<DAGRunState> {
  constexpr auto sql = "SELECT state FROM dag_runs WHERE dag_run_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);
  return string_to_dag_run_state(col_text(stmt.get(), 0));
}

auto Persistence::get_incomplete_dag_runs()
    -> Result<std::vector<DAGRunId>> {
  constexpr auto sql =
      "SELECT dag_run_id FROM dag_runs WHERE state IN ('pending', 'running');";

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

auto Persistence::get_task_instances(DAGRunId dag_run_id)
    -> Result<std::vector<TaskInstanceInfo>> {
  constexpr auto sql = R"(
    SELECT id, task_id, state, attempt, started_at, finished_at,
           exit_code, error_message
    FROM task_instances WHERE dag_run_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<TaskInstanceInfo> instances;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto task_id_str = col_text(stmt.get(), 1);
    NodeIndex task_idx = INVALID_NODE;
    if (!task_id_str.empty()) {
      auto [ptr, ec] = std::from_chars(
          task_id_str.data(), task_id_str.data() + task_id_str.size(),
          task_idx);
      if (ec != std::errc{}) {
        task_idx = INVALID_NODE;
      }
    }
    instances.push_back(
        {.instance_id = InstanceId{col_text(stmt.get(), 0)},
         .task_idx = task_idx,
         .state = string_to_task_state(col_text(stmt.get(), 2)),
         .attempt = sqlite3_column_int(stmt.get(), 3),
         .started_at = from_timestamp(sqlite3_column_int64(stmt.get(), 4)),
         .finished_at = from_timestamp(sqlite3_column_int64(stmt.get(), 5)),
         .exit_code = sqlite3_column_int(stmt.get(), 6),
         .error_message = col_text(stmt.get(), 7)});
  }
  return instances;
}

auto Persistence::list_run_history(std::optional<DAGId> dag_id, std::size_t limit)
    -> Result<std::vector<RunHistoryEntry>> {
  std::string sql;
  if (!dag_id.has_value() || dag_id->value().empty()) {
    sql = R"(
      SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at
      FROM dag_runs ORDER BY scheduled_at DESC LIMIT ?;
    )";
  } else {
    sql = R"(
      SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at
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
    std::string pattern = std::string(dag_id->value()) + "_%";
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
    auto trigger_str = col_text(stmt.get(), 2);
    entries.push_back(
        {.dag_run_id = std::move(run_id),
         .dag_id = std::move(extracted_dag_id),
         .state = string_to_dag_run_state(col_text(stmt.get(), 1)),
         .trigger_type = string_to_trigger_type(trigger_str),
         .scheduled_at = sqlite3_column_int64(stmt.get(), 3),
         .started_at = sqlite3_column_int64(stmt.get(), 4),
         .finished_at = sqlite3_column_int64(stmt.get(), 5)});
  }
  return entries;
}

auto Persistence::get_run_history(DAGRunId dag_run_id)
    -> Result<RunHistoryEntry> {
  constexpr auto sql = R"(
    SELECT dag_run_id, state, trigger_type, scheduled_at, started_at, finished_at
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
  auto trigger_str = col_text(stmt.get(), 2);
  return RunHistoryEntry{.dag_run_id = std::move(run_id),
                         .dag_id = std::move(dag_id),
                         .state =
                             string_to_dag_run_state(col_text(stmt.get(), 1)),
                         .trigger_type = string_to_trigger_type(trigger_str),
                         .scheduled_at = sqlite3_column_int64(stmt.get(), 3),
                         .started_at = sqlite3_column_int64(stmt.get(), 4),
                         .finished_at = sqlite3_column_int64(stmt.get(), 5)};
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
  if (auto r = begin_transaction(); !r) {
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
    if (auto r = rollback_transaction(); !r) {
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
    if (auto r = rollback_transaction(); !r) {
      log::error("Failed to rollback transaction: {}", r.error().message());
      return r;
    }
    return fail(Error::DatabaseQueryFailed);
  }

  if (auto r = commit_transaction(); !r) {
    log::error("Failed to commit transaction: {}", r.error().message());
    return r;
  }
  return ok();
}

auto Persistence::delete_dag(DAGId dag_id) -> Result<void> {
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

auto Persistence::get_dag(DAGId dag_id) -> Result<DAGInfo> {
  constexpr auto sql =
      "SELECT dag_id, name, description, cron, max_concurrent_runs, "
      "created_at, updated_at FROM dags WHERE dag_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_id);
  sqlite3_bind_text(stmt.get(), 1, id_str.c_str(), -1, SQLITE_TRANSIENT);

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

auto Persistence::list_dags() -> Result<std::vector<DAGInfo>> {
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

auto Persistence::save_task(DAGId dag_id, const TaskConfig& task)
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

  std::string deps_str = nlohmann::json(task.dependencies).dump();
  std::string executor_str(executor_type_to_string(task.executor));

  sqlite3_bind_text(stmt.get(), 1, dag_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task.task_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, task.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, task.command.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 5, task.working_dir.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 6, executor_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 7, deps_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 8, static_cast<int>(task.timeout.count()));
  sqlite3_bind_int(stmt.get(), 9,
                   static_cast<int>(task.retry_interval.count()));
  sqlite3_bind_int(stmt.get(), 10, task.max_retries);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::delete_task(DAGId dag_id, TaskId task_id)
    -> Result<void> {
  constexpr auto sql =
      "DELETE FROM dag_tasks WHERE dag_id = ? AND task_id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string dag_str(dag_id), task_str(task_id);
  sqlite3_bind_text(stmt.get(), 1, dag_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task_str.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_tasks(DAGId dag_id)
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
    auto deps_str = col_text(stmt.get(), 5);
    auto executor_str = col_text(stmt.get(), 3);

    TaskConfig task{.task_id = TaskId{col_text(stmt.get(), 0)},
                    .name = col_text(stmt.get(), 1),
                    .command = col_text(stmt.get(), 2),
                    .working_dir = col_text(stmt.get(), 4),
                    .dependencies = {},
                    .executor = string_to_executor_type(
                        executor_str.empty() ? "shell" : executor_str),
                    .timeout =
                        std::chrono::seconds(sqlite3_column_int(stmt.get(), 6)),
                    .retry_interval =
                        std::chrono::seconds(sqlite3_column_int(stmt.get(), 7)),
                    .max_retries = sqlite3_column_int(stmt.get(), 8)};

    if (!deps_str.empty()) {
      auto deps_result = nlohmann::json::parse(deps_str, nullptr, false);
      if (deps_result.is_array()) {
        for (const auto& dep : deps_result) {
          if (dep.is_string()) {
            task.dependencies.push_back(TaskId{dep.get<std::string>()});
          }
        }
      } else {
        log::warn("Invalid deps JSON format for task {}: {}", task.task_id, deps_str);
      }
    }
    tasks.push_back(std::move(task));
  }
  return tasks;
}

auto Persistence::get_all_tasks()
    -> Result<std::unordered_map<std::string, std::vector<TaskConfig>>> {
  constexpr auto sql = R"(
    SELECT dag_id, task_id, name, command, working_dir, executor, deps, timeout, retry_interval, max_retries
    FROM dag_tasks;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::unordered_map<std::string, std::vector<TaskConfig>> tasks_by_dag;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto dag_id_str = col_text(stmt.get(), 0);
    auto deps_str = col_text(stmt.get(), 6);
    auto executor_str = col_text(stmt.get(), 5);

    TaskConfig task{.task_id = TaskId{col_text(stmt.get(), 1)},
                    .name = col_text(stmt.get(), 2),
                    .command = col_text(stmt.get(), 3),
                    .working_dir = col_text(stmt.get(), 4),
                    .dependencies = {},
                    .executor = string_to_executor_type(
                        executor_str.empty() ? "shell" : executor_str),
                    .timeout =
                        std::chrono::seconds(sqlite3_column_int(stmt.get(), 7)),
                    .retry_interval =
                        std::chrono::seconds(sqlite3_column_int(stmt.get(), 8)),
                    .max_retries = sqlite3_column_int(stmt.get(), 9)};

    if (!deps_str.empty()) {
      auto deps_result = nlohmann::json::parse(deps_str, nullptr, false);
      if (deps_result.is_array()) {
        for (const auto& dep : deps_result) {
          if (dep.is_string()) {
            task.dependencies.push_back(TaskId{dep.get<std::string>()});
          }
        }
      }
    }
    tasks_by_dag[dag_id_str].push_back(std::move(task));
  }
  return tasks_by_dag;
}


auto Persistence::save_task_instances_batch(
    DAGRunId dag_run_id, const std::vector<TaskInstanceInfo>& instances)
    -> Result<void> {
  if (instances.empty()) {
    return ok();
  }

  if (auto r = begin_transaction(); !r) {
    return r;
  }

  constexpr auto sql = R"(
    INSERT INTO task_instances
      (id, dag_run_id, task_id, state, attempt, started_at, finished_at,
       exit_code, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  )";

  auto result = prepare(sql);
  if (!result) {
    (void)rollback_transaction();
    return std::unexpected(result.error());
  }
  Statement stmt(*result);

  for (const auto& info : instances) {
    sqlite3_clear_bindings(stmt.get());

    if (info.instance_id.empty()) {
      log::error("Task instance missing instance_id at task_idx {}", info.task_idx);
      (void)rollback_transaction();
      return fail(Error::InvalidArgument);
    }

    sqlite3_bind_text(stmt.get(), 1, info.instance_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 2, dag_run_id.c_str(), -1, SQLITE_TRANSIENT);
    auto task_idx_str = std::to_string(info.task_idx);
    sqlite3_bind_text(stmt.get(), 3, task_idx_str.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 4, task_state_to_string(info.state), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt.get(), 5, info.attempt);
    sqlite3_bind_int64(stmt.get(), 6, to_timestamp(info.started_at));
    sqlite3_bind_int64(stmt.get(), 7, to_timestamp(info.finished_at));
    sqlite3_bind_int(stmt.get(), 8, info.exit_code);
    sqlite3_bind_text(stmt.get(), 9, info.error_message.c_str(), -1,
                      SQLITE_TRANSIENT);

    if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
      log::error("Failed to save task instance {} in batch: {}",
                 info.instance_id.value(), sqlite3_errmsg(db_.get()));
      (void)rollback_transaction();
      return fail(Error::DatabaseQueryFailed);
    }
  }

  if (auto r = commit_transaction(); !r) {
    log::error("Failed to commit transaction: {}", r.error().message());
    return r;
  }
  return ok();
}

auto Persistence::save_task_log(DAGRunId dag_run_id,
                                TaskId task_id, int attempt,
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

auto Persistence::get_task_logs(DAGRunId dag_run_id,
                                TaskId task_id, int attempt)
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
    entry.dag_run_id = DAGRunId{col_text(stmt.get(), 1)};
    entry.task_id = TaskId{col_text(stmt.get(), 2)};
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
  if (auto r = begin_transaction(); !r) return r;
  if (auto r = execute("DELETE FROM task_logs;"); !r) return r;
  if (auto r = execute("DELETE FROM task_instances;"); !r) return r;
  if (auto r = execute("DELETE FROM dag_runs;"); !r) return r;
  if (auto r = execute("DELETE FROM dag_tasks;"); !r) return r;
  if (auto r = execute("DELETE FROM dags;"); !r) return r;
  return commit_transaction();
}

}  // namespace taskmaster
