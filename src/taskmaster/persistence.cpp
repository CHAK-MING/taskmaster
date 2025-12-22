#include "taskmaster/persistence.hpp"
#include "taskmaster/dag_manager.hpp"

#include <nlohmann/json.hpp>
#include "taskmaster/log.hpp"
#include <sqlite3.h>

namespace taskmaster {

namespace {

int64_t to_timestamp(std::chrono::system_clock::time_point tp) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

std::chrono::system_clock::time_point from_timestamp(int64_t ts) {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(ts));
}

constexpr std::array kDagRunStateNames = {"pending", "running", "success",
                                          "failed"};
constexpr std::array kTaskStateNames = {"pending", "running", "success",
                                        "failed"};

const char *dag_run_state_to_string(DAGRunState state) {
  auto idx = static_cast<std::size_t>(state);
  return idx < kDagRunStateNames.size() ? kDagRunStateNames[idx] : "unknown";
}

DAGRunState string_to_dag_run_state(std::string_view s) {
  auto it = std::ranges::find(kDagRunStateNames, s);
  if (it != kDagRunStateNames.end()) {
    return static_cast<DAGRunState>(
        std::ranges::distance(kDagRunStateNames.begin(), it));
  }
  return DAGRunState::Pending;
}

const char *task_state_to_string(TaskState state) {
  auto idx = static_cast<std::size_t>(state);
  return idx < kTaskStateNames.size() ? kTaskStateNames[idx] : "unknown";
}

TaskState string_to_task_state(std::string_view s) {
  auto it = std::ranges::find(kTaskStateNames, s);
  if (it != kTaskStateNames.end()) {
    return static_cast<TaskState>(
        std::ranges::distance(kTaskStateNames.begin(), it));
  }
  return TaskState::Pending;
}

} // namespace

void Persistence::Sqlite3Deleter::operator()(sqlite3 *db) const {
  if (db) {
    sqlite3_close(db);
  }
}

Persistence::Persistence(std::string_view db_path) : db_path_(db_path) {}

Persistence::~Persistence() { close(); }

auto Persistence::open() -> Result<void> {
  if (db_) {
    return ok();
  }

  sqlite3 *raw_db = nullptr;
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

auto Persistence::close() -> void { db_.reset(); }

auto Persistence::create_tables() -> Result<void> {
  const char *sql = R"(
    CREATE TABLE IF NOT EXISTS dag_runs (
      id TEXT PRIMARY KEY,
      state TEXT NOT NULL DEFAULT 'pending',
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
      FOREIGN KEY (dag_run_id) REFERENCES dag_runs(id)
    );

    CREATE TABLE IF NOT EXISTS dags (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT DEFAULT '',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      from_config INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS dag_tasks (
      dag_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      name TEXT DEFAULT '',
      command TEXT NOT NULL,
      cron TEXT DEFAULT '',
      working_dir TEXT DEFAULT '',
      deps TEXT DEFAULT '[]',
      timeout INTEGER DEFAULT 300,
      max_retries INTEGER DEFAULT 3,
      enabled INTEGER DEFAULT 1,
      PRIMARY KEY (dag_id, task_id),
      FOREIGN KEY (dag_id) REFERENCES dags(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_task_instances_dag_run
      ON task_instances(dag_run_id);
    CREATE INDEX IF NOT EXISTS idx_dag_runs_state
      ON dag_runs(state);
  )";

  return execute(sql);
}

auto Persistence::execute(std::string_view sql) -> Result<void> {
  char *err_msg = nullptr;
  std::string sql_str{sql};
  int rc = sqlite3_exec(db_.get(), sql_str.c_str(), nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    log::error("SQL error: {}", err_msg);
    sqlite3_free(err_msg);
    return fail(Error::DatabaseQueryFailed);
  }
  return ok();
}

auto Persistence::save_dag_run(const DAGRun &run) -> Result<void> {
  const char *sql = R"(
    INSERT INTO dag_runs (id, state, scheduled_at, started_at, finished_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      state = excluded.state,
      scheduled_at = excluded.scheduled_at,
      started_at = excluded.started_at,
      finished_at = excluded.finished_at;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, run.id().c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, dag_run_state_to_string(run.state()), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 3, to_timestamp(run.scheduled_at()));
  sqlite3_bind_int64(stmt, 4, to_timestamp(run.started_at()));
  sqlite3_bind_int64(stmt, 5, to_timestamp(run.finished_at()));

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);
  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_dag_run_state(std::string_view dag_run_id,
                                       DAGRunState state) -> Result<void> {
  const char *sql = "UPDATE dag_runs SET state = ? WHERE id = ?;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, dag_run_state_to_string(state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, std::string(dag_run_id).c_str(), -1,
                    SQLITE_TRANSIENT);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);
  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::save_task_instance(std::string_view dag_run_id,
                                     const TaskInstanceInfo &info)
    -> Result<void> {
  const char *sql = R"(
    INSERT INTO task_instances
      (id, dag_run_id, task_id, state, attempt, started_at, finished_at,
       exit_code, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }

  std::string id = info.instance_id.empty() ? std::to_string(info.task_idx)
                                            : info.instance_id;
  sqlite3_bind_text(stmt, 1, id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, std::string(dag_run_id).c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 3, static_cast<int>(info.task_idx));
  sqlite3_bind_text(stmt, 4, task_state_to_string(info.state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 5, info.attempt);
  sqlite3_bind_int64(stmt, 6, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt, 7, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt, 8, info.exit_code);
  sqlite3_bind_text(stmt, 9, info.error_message.c_str(), -1, SQLITE_TRANSIENT);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);
  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_task_instance(std::string_view dag_run_id,
                                       const TaskInstanceInfo &info)
    -> Result<void> {
  const char *sql = R"(
    UPDATE task_instances SET
      state = ?,
      attempt = ?,
      started_at = ?,
      finished_at = ?,
      exit_code = ?,
      error_message = ?
    WHERE dag_run_id = ? AND task_id = ?;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, task_state_to_string(info.state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 2, info.attempt);
  sqlite3_bind_int64(stmt, 3, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt, 4, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt, 5, info.exit_code);
  sqlite3_bind_text(stmt, 6, info.error_message.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 7, std::string(dag_run_id).c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 8, static_cast<int>(info.task_idx));

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);
  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_dag_run_state(std::string_view dag_run_id)
    -> Result<DAGRunState> {
  const char *sql = "SELECT state FROM dag_runs WHERE id = ?;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_run_id).c_str(), -1,
                    SQLITE_TRANSIENT);

  Result<DAGRunState> result = fail(Error::NotFound);
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *state_str =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    result = ok(string_to_dag_run_state(state_str ? state_str : ""));
  }

  sqlite3_finalize(stmt);
  return result;
}

auto Persistence::get_incomplete_dag_runs()
    -> Result<std::vector<std::string>> {
  const char *sql =
      "SELECT id FROM dag_runs WHERE state IN ('pending', 'running');";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  std::vector<std::string> result;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const char *id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    if (id)
      result.emplace_back(id);
  }

  sqlite3_finalize(stmt);
  return ok(std::move(result));
}

auto Persistence::get_task_instances(std::string_view dag_run_id)
    -> Result<std::vector<TaskInstanceInfo>> {
  const char *sql = R"(
    SELECT id, task_id, state, attempt, started_at, finished_at,
           exit_code, error_message
    FROM task_instances
    WHERE dag_run_id = ?;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_run_id).c_str(), -1,
                    SQLITE_TRANSIENT);

  std::vector<TaskInstanceInfo> result;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    TaskInstanceInfo info;
    const char *id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 0));
    const char *task_id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1));
    const char *state =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2));
    const char *error =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt, 7));

    info.instance_id = id ? id : "";
    info.task_idx =
        task_id ? static_cast<NodeIndex>(std::stoi(task_id)) : INVALID_NODE;
    info.state = string_to_task_state(state ? state : "");
    info.attempt = sqlite3_column_int(stmt, 3);
    info.started_at = from_timestamp(sqlite3_column_int64(stmt, 4));
    info.finished_at = from_timestamp(sqlite3_column_int64(stmt, 5));
    info.exit_code = sqlite3_column_int(stmt, 6);
    info.error_message = error ? error : "";

    result.push_back(std::move(info));
  }

  sqlite3_finalize(stmt);
  return ok(std::move(result));
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

// DAG persistence implementations

auto Persistence::save_dag(const DAGInfo& dag) -> Result<void> {
  const char *sql = R"(
    INSERT INTO dags (id, name, description, created_at, updated_at, from_config)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name,
      description = excluded.description,
      updated_at = excluded.updated_at;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare save_dag statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, dag.id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, dag.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, dag.description.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt, 4, to_timestamp(dag.created_at));
  sqlite3_bind_int64(stmt, 5, to_timestamp(dag.updated_at));
  sqlite3_bind_int(stmt, 6, dag.from_config ? 1 : 0);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);

  if (!success) {
    return fail(Error::DatabaseQueryFailed);
  }

  // Save all tasks for this DAG
  for (const auto& task : dag.tasks) {
    if (auto r = save_task(dag.id, task); !r) {
      return r;
    }
  }

  return ok();
}

auto Persistence::delete_dag(std::string_view dag_id) -> Result<void> {
  // Tasks are deleted automatically via ON DELETE CASCADE
  const char *sql = "DELETE FROM dags WHERE id = ?;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_id).c_str(), -1, SQLITE_TRANSIENT);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);

  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_dag(std::string_view dag_id) -> Result<DAGInfo> {
  const char *sql = "SELECT id, name, description, created_at, updated_at, from_config FROM dags WHERE id = ?;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_id).c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt) != SQLITE_ROW) {
    sqlite3_finalize(stmt);
    return fail(Error::NotFound);
  }

  DAGInfo dag;
  const char* id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
  const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
  const char* desc = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

  dag.id = id ? id : "";
  dag.name = name ? name : "";
  dag.description = desc ? desc : "";
  dag.created_at = from_timestamp(sqlite3_column_int64(stmt, 3));
  dag.updated_at = from_timestamp(sqlite3_column_int64(stmt, 4));
  dag.from_config = sqlite3_column_int(stmt, 5) != 0;

  sqlite3_finalize(stmt);

  // Load tasks for this DAG
  auto tasks_result = get_tasks(dag_id);
  if (tasks_result) {
    dag.tasks = std::move(*tasks_result);
  }

  return ok(std::move(dag));
}

auto Persistence::list_dags() -> Result<std::vector<DAGInfo>> {
  const char *sql = "SELECT id, name, description, created_at, updated_at, from_config FROM dags;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  std::vector<DAGInfo> result;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    DAGInfo dag;
    const char* id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
    const char* desc = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));

    dag.id = id ? id : "";
    dag.name = name ? name : "";
    dag.description = desc ? desc : "";
    dag.created_at = from_timestamp(sqlite3_column_int64(stmt, 3));
    dag.updated_at = from_timestamp(sqlite3_column_int64(stmt, 4));
    dag.from_config = sqlite3_column_int(stmt, 5) != 0;

    // Load tasks for this DAG
    auto tasks_result = get_tasks(dag.id);
    if (tasks_result) {
      dag.tasks = std::move(*tasks_result);
    }

    result.push_back(std::move(dag));
  }

  sqlite3_finalize(stmt);
  return ok(std::move(result));
}

auto Persistence::save_task(std::string_view dag_id, const TaskConfig& task) -> Result<void> {
  const char *sql = R"(
    INSERT INTO dag_tasks (dag_id, task_id, name, command, cron, working_dir, deps, timeout, max_retries, enabled)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_id, task_id) DO UPDATE SET
      name = excluded.name,
      command = excluded.command,
      cron = excluded.cron,
      working_dir = excluded.working_dir,
      deps = excluded.deps,
      timeout = excluded.timeout,
      max_retries = excluded.max_retries,
      enabled = excluded.enabled;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare save_task statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }

  // Serialize deps to JSON array
  nlohmann::json deps_json = task.deps;
  std::string deps_str = deps_json.dump();

  sqlite3_bind_text(stmt, 1, std::string(dag_id).c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, task.id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 3, task.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 4, task.command.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 5, task.cron.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 6, task.working_dir.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 7, deps_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt, 8, static_cast<int>(task.timeout.count()));
  sqlite3_bind_int(stmt, 9, task.max_retries);
  sqlite3_bind_int(stmt, 10, task.enabled ? 1 : 0);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);

  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::delete_task(std::string_view dag_id, std::string_view task_id) -> Result<void> {
  const char *sql = "DELETE FROM dag_tasks WHERE dag_id = ? AND task_id = ?;";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_id).c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt, 2, std::string(task_id).c_str(), -1, SQLITE_TRANSIENT);

  bool success = sqlite3_step(stmt) == SQLITE_DONE;
  sqlite3_finalize(stmt);

  return success ? ok() : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_tasks(std::string_view dag_id) -> Result<std::vector<TaskConfig>> {
  const char *sql = R"(
    SELECT task_id, name, command, cron, working_dir, deps, timeout, max_retries, enabled
    FROM dag_tasks WHERE dag_id = ?;
  )";

  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    return fail(Error::DatabaseQueryFailed);
  }

  sqlite3_bind_text(stmt, 1, std::string(dag_id).c_str(), -1, SQLITE_TRANSIENT);

  std::vector<TaskConfig> result;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    TaskConfig task;
    const char* id = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
    const char* name = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
    const char* command = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
    const char* cron = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
    const char* working_dir = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 4));
    const char* deps_str = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 5));

    task.id = id ? id : "";
    task.name = name ? name : "";
    task.command = command ? command : "";
    task.cron = cron ? cron : "";
    task.working_dir = working_dir ? working_dir : "";
    task.timeout = std::chrono::seconds(sqlite3_column_int(stmt, 6));
    task.max_retries = sqlite3_column_int(stmt, 7);
    task.enabled = sqlite3_column_int(stmt, 8) != 0;

    // Parse deps from JSON array
    if (deps_str && deps_str[0] != '\0') {
      try {
        auto deps_json = nlohmann::json::parse(deps_str);
        if (deps_json.is_array()) {
          for (const auto& dep : deps_json) {
            if (dep.is_string()) {
              task.deps.push_back(dep.get<std::string>());
            }
          }
        }
      } catch (const nlohmann::json::exception& e) {
        log::warn("Failed to parse deps JSON for task {}: {}", task.id, e.what());
      }
    }

    result.push_back(std::move(task));
  }

  sqlite3_finalize(stmt);
  return ok(std::move(result));
}

} // namespace taskmaster
