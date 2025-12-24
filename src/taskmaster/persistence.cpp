#include "taskmaster/persistence.hpp"
#include "taskmaster/dag_manager.hpp"

#include "taskmaster/log.hpp"
#include <nlohmann/json.hpp>
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

void Persistence::DbDeleter::operator()(sqlite3 *db) const {
  if (db)
    sqlite3_close(db);
}

Persistence::Statement::~Statement() { reset(); }

auto Persistence::Statement::reset() -> void {
  if (stmt_) {
    sqlite3_finalize(stmt_);
    stmt_ = nullptr;
  }
}

auto Persistence::prepare(const char *sql) -> Result<sqlite3_stmt *> {
  sqlite3_stmt *stmt = nullptr;
  if (sqlite3_prepare_v2(db_.get(), sql, -1, &stmt, nullptr) != SQLITE_OK) {
    log::error("Failed to prepare statement: {}", sqlite3_errmsg(db_.get()));
    return fail(Error::DatabaseQueryFailed);
  }
  return stmt;
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
  constexpr auto sql = R"(
    INSERT INTO dag_runs (id, state, scheduled_at, started_at, finished_at)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      state = excluded.state,
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
  sqlite3_bind_int64(stmt.get(), 3, to_timestamp(run.scheduled_at()));
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(run.started_at()));
  sqlite3_bind_int64(stmt.get(), 5, to_timestamp(run.finished_at()));

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::update_dag_run_state(std::string_view dag_run_id,
                                       DAGRunState state) -> Result<void> {
  constexpr auto sql = "UPDATE dag_runs SET state = ? WHERE id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, dag_run_state_to_string(state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, id_str.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::save_task_instance(std::string_view dag_run_id,
                                     const TaskInstanceInfo &info)
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

  std::string id = info.instance_id.empty() ? std::to_string(info.task_idx)
                                            : info.instance_id;
  std::string run_id(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, run_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 3, static_cast<int>(info.task_idx));
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

auto Persistence::update_task_instance(std::string_view dag_run_id,
                                       const TaskInstanceInfo &info)
    -> Result<void> {
  constexpr auto sql = R"(
    UPDATE task_instances SET
      state = ?, attempt = ?, started_at = ?, finished_at = ?,
      exit_code = ?, error_message = ?
    WHERE dag_run_id = ? AND task_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string run_id(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, task_state_to_string(info.state), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 2, info.attempt);
  sqlite3_bind_int64(stmt.get(), 3, to_timestamp(info.started_at));
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(info.finished_at));
  sqlite3_bind_int(stmt.get(), 5, info.exit_code);
  sqlite3_bind_text(stmt.get(), 6, info.error_message.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 7, run_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 8, static_cast<int>(info.task_idx));

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_dag_run_state(std::string_view dag_run_id)
    -> Result<DAGRunState> {
  constexpr auto sql = "SELECT state FROM dag_runs WHERE id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, id_str.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);
  auto *state_str =
      reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 0));
  return string_to_dag_run_state(state_str ? state_str : "");
}

auto Persistence::get_incomplete_dag_runs()
    -> Result<std::vector<std::string>> {
  constexpr auto sql =
      "SELECT id FROM dag_runs WHERE state IN ('pending', 'running');";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::vector<std::string> ids;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    if (auto *id = reinterpret_cast<const char *>(
            sqlite3_column_text(stmt.get(), 0))) {
      ids.emplace_back(id);
    }
  }
  return ids;
}

auto Persistence::get_task_instances(std::string_view dag_run_id)
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

  std::string run_id(dag_run_id);
  sqlite3_bind_text(stmt.get(), 1, run_id.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<TaskInstanceInfo> instances;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto *id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 0));
    auto *task_id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 1));
    auto *state =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 2));
    auto *error =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 7));

    instances.push_back(
        {.instance_id = id ? id : "",
         .task_idx = task_id ? static_cast<NodeIndex>(std::stoi(task_id))
                             : INVALID_NODE,
         .state = string_to_task_state(state ? state : ""),
         .attempt = sqlite3_column_int(stmt.get(), 3),
         .started_at = from_timestamp(sqlite3_column_int64(stmt.get(), 4)),
         .finished_at = from_timestamp(sqlite3_column_int64(stmt.get(), 5)),
         .exit_code = sqlite3_column_int(stmt.get(), 6),
         .error_message = error ? error : ""});
  }
  return instances;
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

auto Persistence::save_dag(const DAGInfo &dag) -> Result<void> {
  if (auto r = begin_transaction(); !r)
    return r;

  constexpr auto sql = R"(
    INSERT INTO dags (id, name, description, created_at, updated_at, from_config)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      name = excluded.name, description = excluded.description, updated_at = excluded.updated_at;
  )";

  auto result = prepare(sql);
  if (!result) {
    (void)rollback_transaction();
    return std::unexpected(result.error());
  }
  Statement stmt(*result);

  sqlite3_bind_text(stmt.get(), 1, dag.id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, dag.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, dag.description.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_int64(stmt.get(), 4, to_timestamp(dag.created_at));
  sqlite3_bind_int64(stmt.get(), 5, to_timestamp(dag.updated_at));
  sqlite3_bind_int(stmt.get(), 6, dag.from_config ? 1 : 0);

  if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
    (void)rollback_transaction();
    return fail(Error::DatabaseQueryFailed);
  }

  if (auto r = save_tasks_batch(dag.id, dag.tasks); !r) {
    (void)rollback_transaction();
    return r;
  }
  return commit_transaction();
}

auto Persistence::delete_dag(std::string_view dag_id) -> Result<void> {
  constexpr auto sql = "DELETE FROM dags WHERE id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_id);
  sqlite3_bind_text(stmt.get(), 1, id_str.c_str(), -1, SQLITE_TRANSIENT);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::get_dag(std::string_view dag_id) -> Result<DAGInfo> {
  constexpr auto sql = "SELECT id, name, description, created_at, updated_at, "
                       "from_config FROM dags WHERE id = ?;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string id_str(dag_id);
  sqlite3_bind_text(stmt.get(), 1, id_str.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(stmt.get()) != SQLITE_ROW)
    return fail(Error::NotFound);

  auto *id = reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 0));
  auto *name =
      reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 1));
  auto *desc =
      reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 2));

  DAGInfo dag{.id = id ? id : "",
              .name = name ? name : "",
              .description = desc ? desc : "",
              .created_at = from_timestamp(sqlite3_column_int64(stmt.get(), 3)),
              .updated_at = from_timestamp(sqlite3_column_int64(stmt.get(), 4)),
              .tasks = {},
              .task_index = {},
              .reverse_adj_cache = {},
              .from_config = sqlite3_column_int(stmt.get(), 5) != 0};

  if (auto tasks_result = get_tasks(dag_id))
    dag.tasks = std::move(*tasks_result);
  return dag;
}

auto Persistence::list_dags() -> Result<std::vector<DAGInfo>> {
  constexpr auto sql = "SELECT id, name, description, created_at, updated_at, "
                       "from_config FROM dags;";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::vector<DAGInfo> dags;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto *id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 0));
    auto *name =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 1));
    auto *desc =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 2));

    DAGInfo dag{
        .id = id ? id : "",
        .name = name ? name : "",
        .description = desc ? desc : "",
        .created_at = from_timestamp(sqlite3_column_int64(stmt.get(), 3)),
        .updated_at = from_timestamp(sqlite3_column_int64(stmt.get(), 4)),
        .tasks = {},
        .task_index = {},
        .reverse_adj_cache = {},
        .from_config = sqlite3_column_int(stmt.get(), 5) != 0};
    if (auto tasks_result = get_tasks(dag.id))
      dag.tasks = std::move(*tasks_result);
    dags.push_back(std::move(dag));
  }
  return dags;
}

auto Persistence::save_task(std::string_view dag_id, const TaskConfig &task)
    -> Result<void> {
  constexpr auto sql = R"(
    INSERT INTO dag_tasks (dag_id, task_id, name, command, cron, working_dir, deps, timeout, max_retries, enabled)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_id, task_id) DO UPDATE SET
      name = excluded.name, command = excluded.command, cron = excluded.cron,
      working_dir = excluded.working_dir, deps = excluded.deps, timeout = excluded.timeout,
      max_retries = excluded.max_retries, enabled = excluded.enabled;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string dag_id_str(dag_id);
  std::string deps_str = nlohmann::json(task.deps).dump();

  sqlite3_bind_text(stmt.get(), 1, dag_id_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 2, task.id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 3, task.name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 4, task.command.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 5, task.cron.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 6, task.working_dir.c_str(), -1,
                    SQLITE_TRANSIENT);
  sqlite3_bind_text(stmt.get(), 7, deps_str.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(stmt.get(), 8, static_cast<int>(task.timeout.count()));
  sqlite3_bind_int(stmt.get(), 9, task.max_retries);
  sqlite3_bind_int(stmt.get(), 10, task.enabled ? 1 : 0);

  return sqlite3_step(stmt.get()) == SQLITE_DONE
             ? ok()
             : fail(Error::DatabaseQueryFailed);
}

auto Persistence::delete_task(std::string_view dag_id, std::string_view task_id)
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

auto Persistence::get_tasks(std::string_view dag_id)
    -> Result<std::vector<TaskConfig>> {
  constexpr auto sql = R"(
    SELECT task_id, name, command, cron, working_dir, deps, timeout, max_retries, enabled
    FROM dag_tasks WHERE dag_id = ?;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string dag_str(dag_id);
  sqlite3_bind_text(stmt.get(), 1, dag_str.c_str(), -1, SQLITE_TRANSIENT);

  std::vector<TaskConfig> tasks;
  while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
    auto *id =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 0));
    auto *name =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 1));
    auto *command =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 2));
    auto *cron =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 3));
    auto *working_dir =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 4));
    auto *deps_str =
        reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), 5));

    TaskConfig task{.id = id ? id : "",
                    .name = name ? name : "",
                    .cron = cron ? cron : "",
                    .command = command ? command : "",
                    .working_dir = working_dir ? working_dir : "",
                    .deps = {},
                    .timeout =
                        std::chrono::seconds(sqlite3_column_int(stmt.get(), 6)),
                    .max_retries = sqlite3_column_int(stmt.get(), 7),
                    .enabled = sqlite3_column_int(stmt.get(), 8) != 0};

    if (deps_str && deps_str[0] != '\0') {
      try {
        if (auto deps_json = nlohmann::json::parse(deps_str);
            deps_json.is_array()) {
          for (const auto &dep : deps_json) {
            if (dep.is_string())
              task.deps.push_back(dep.get<std::string>());
          }
        }
      } catch (const nlohmann::json::exception &e) {
        log::warn("Failed to parse deps JSON for task {}: {}", task.id,
                  e.what());
      }
    }
    tasks.push_back(std::move(task));
  }
  return tasks;
}

auto Persistence::save_tasks_batch(std::string_view dag_id,
                                   const std::vector<TaskConfig> &tasks)
    -> Result<void> {
  if (tasks.empty())
    return ok();

  constexpr auto sql = R"(
    INSERT INTO dag_tasks (dag_id, task_id, name, command, cron, working_dir, deps, timeout, max_retries, enabled)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(dag_id, task_id) DO UPDATE SET
      name = excluded.name, command = excluded.command, cron = excluded.cron,
      working_dir = excluded.working_dir, deps = excluded.deps, timeout = excluded.timeout,
      max_retries = excluded.max_retries, enabled = excluded.enabled;
  )";

  auto result = prepare(sql);
  if (!result)
    return std::unexpected(result.error());
  Statement stmt(*result);

  std::string dag_str(dag_id);
  for (const auto &task : tasks) {
    sqlite3_reset(stmt.get());
    sqlite3_clear_bindings(stmt.get());

    std::string deps_str = nlohmann::json(task.deps).dump();
    sqlite3_bind_text(stmt.get(), 1, dag_str.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 2, task.id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 3, task.name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 4, task.command.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 5, task.cron.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 6, task.working_dir.c_str(), -1,
                      SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 7, deps_str.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt.get(), 8, static_cast<int>(task.timeout.count()));
    sqlite3_bind_int(stmt.get(), 9, task.max_retries);
    sqlite3_bind_int(stmt.get(), 10, task.enabled ? 1 : 0);

    if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
      log::error("Failed to save task {} in batch: {}", task.id,
                 sqlite3_errmsg(db_.get()));
      return fail(Error::DatabaseQueryFailed);
    }
  }
  return ok();
}

auto Persistence::save_task_instances_batch(
    std::string_view dag_run_id, const std::vector<TaskInstanceInfo> &instances)
    -> Result<void> {
  if (instances.empty())
    return ok();
  if (auto r = begin_transaction(); !r)
    return r;

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

  std::string run_id(dag_run_id);
  for (const auto &info : instances) {
    sqlite3_reset(stmt.get());
    sqlite3_clear_bindings(stmt.get());

    std::string id = info.instance_id.empty() ? std::to_string(info.task_idx)
                                              : info.instance_id;
    sqlite3_bind_text(stmt.get(), 1, id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 2, run_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt.get(), 3, static_cast<int>(info.task_idx));
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
                 info.instance_id, sqlite3_errmsg(db_.get()));
      (void)rollback_transaction();
      return fail(Error::DatabaseQueryFailed);
    }
  }
  return commit_transaction();
}

} // namespace taskmaster
