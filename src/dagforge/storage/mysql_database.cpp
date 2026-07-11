#include "dagforge/storage/mysql_database.hpp"
#include "dagforge/storage/mysql_schema.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/enum_mysql_formatter.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/time.hpp"
#include "dagforge/xcom/xcom_codec.hpp"

#include <boost/asio/cancel_after.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/connect_params.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/format_sql.hpp>
#include <boost/mysql/is_fatal_error.hpp>
#include <boost/mysql/pipeline.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/sequence.hpp>
#include <boost/mysql/with_params.hpp>

#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <memory_resource>
#include <mutex>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::storage {
namespace {

#define DAGFORGE_TRY_MYSQL(expr)                                               \
  do {                                                                         \
    auto dagforge_mysql_result__ = co_await (expr);                            \
    if (!dagforge_mysql_result__) {                                            \
      co_return fail(dagforge_mysql_result__.error());                         \
    }                                                                          \
  } while (false)

constexpr std::array<std::uint64_t, 15> kConnectionAcquireBucketsNs{
    100'000ULL,       250'000ULL,       500'000ULL,       1'000'000ULL,
    2'500'000ULL,     5'000'000ULL,     10'000'000ULL,    25'000'000ULL,
    50'000'000ULL,    100'000'000ULL,   250'000'000ULL,   500'000'000ULL,
    1'000'000'000ULL, 2'500'000'000ULL, 10'000'000'000ULL};

[[nodiscard]] auto as_i64(const boost::mysql::field_view &f) -> std::int64_t;
[[nodiscard]] auto as_str(const boost::mysql::field_view &f) -> std::string;

[[nodiscard]] auto to_millis(std::chrono::system_clock::time_point tp)
    -> std::int64_t {
  return util::to_unix_millis(tp);
}

[[nodiscard]] auto sanitize_mysql_message(std::string_view message,
                                          std::size_t max_len = 160)
    -> std::string {
  std::string out;
  out.reserve(std::min(message.size(), max_len));
  for (char ch : message) {
    if (out.size() >= max_len) {
      break;
    }
    const auto uch = static_cast<unsigned char>(ch);
    if (uch >= 0x20 && uch != 0x7f) {
      out.push_back(ch);
    } else if (ch == '\n' || ch == '\r' || ch == '\t') {
      out.push_back(' ');
    } else {
      out.push_back('?');
    }
  }
  return out;
}

[[nodiscard]] auto
describe_mysql_diagnostics(const boost::mysql::diagnostics &diag)
    -> std::string {
  const auto client = sanitize_mysql_message(diag.client_message());
  const auto server = sanitize_mysql_message(diag.server_message());
  if (!server.empty()) {
    return std::format("server='{}'", server);
  }
  if (!client.empty()) {
    return std::format("client='{}'", client);
  }
  return "no diagnostics";
}

[[nodiscard]] auto map_mysql_error_code(const boost::mysql::error_code &ec)
    -> Error {
  return boost::mysql::is_fatal_error(ec) ? Error::DatabaseOpenFailed
                                          : Error::DatabaseQueryFailed;
}

[[nodiscard]] auto from_millis(std::int64_t ts)
    -> std::chrono::system_clock::time_point {
  return std::chrono::system_clock::time_point(std::chrono::milliseconds(ts));
}

[[nodiscard]] auto executor_config_from_json(std::string_view json,
                                             ExecutorType executor)
    -> Result<ExecutorConfig> {
  auto parsed =
      ExecutorRegistry::instance().parse_persisted_config(executor, json);
  if (parsed) {
    return std::move(*parsed);
  }
  log::error("Invalid executor_config for {}: {}", to_string_view(executor),
             json);
  return fail(parsed.error());
}

[[nodiscard]] auto split_sql_statements(std::string_view input)
    -> std::vector<std::string> {
  std::vector<std::string> out;
  std::string current;
  current.reserve(input.size());

  bool in_single = false;
  bool in_double = false;
  for (char c : input) {
    if (c == '\'' && !in_double) {
      in_single = !in_single;
    } else if (c == '"' && !in_single) {
      in_double = !in_double;
    }

    if (c == ';' && !in_single && !in_double) {
      auto first = current.find_first_not_of(" \n\r\t");
      if (first != std::string::npos) {
        auto last = current.find_last_not_of(" \n\r\t");
        out.emplace_back(current.substr(first, last - first + 1));
      }
      current.clear();
      continue;
    }
    current.push_back(c);
  }

  auto first = current.find_first_not_of(" \n\r\t");
  if (first != std::string::npos) {
    auto last = current.find_last_not_of(" \n\r\t");
    out.emplace_back(current.substr(first, last - first + 1));
  }

  return out;
}

[[nodiscard]] auto is_stringish_sql_type(std::string_view data_type) -> bool {
  return data_type == "char" || data_type == "varchar" ||
         data_type == "tinytext" || data_type == "text" ||
         data_type == "mediumtext" || data_type == "longtext" ||
         data_type == "enum";
}

[[nodiscard]] auto is_integerish_sql_type(std::string_view data_type) -> bool {
  return data_type == "tinyint" || data_type == "smallint" ||
         data_type == "mediumint" || data_type == "int" ||
         data_type == "bigint";
}

auto fetch_column_data_type(boost::mysql::any_connection &conn,
                            std::string_view table_name,
                            std::string_view column_name)
    -> task<Result<std::string>> {
  boost::mysql::results res;
  auto execute_res = co_await co_as_result(conn.async_execute(
      boost::mysql::with_params(
          "SELECT DATA_TYPE "
          "FROM information_schema.columns "
          "WHERE table_schema = DATABASE() AND table_name = {} "
          "AND column_name = {}",
          table_name, column_name),
      res, dagforge::use_nothrow));
  if (!execute_res) {
    co_return fail(execute_res.error());
  }
  if (res.rows().empty()) {
    co_return fail(Error::NotFound);
  }
  co_return ok(as_str(res.rows().at(0).at(0)));
}

template <typename Query>
auto execute_mysql(boost::mysql::any_connection &conn, Query query,
                   boost::mysql::results &res) -> task<Result<void>> {
  auto execute_res = co_await co_as_result(
      conn.async_execute(std::move(query), res, dagforge::use_nothrow));
  if (!execute_res) {
    co_return fail(execute_res.error());
  }
  co_return ok();
}

auto run_pipeline_mysql(
    boost::mysql::any_connection &conn, boost::mysql::pipeline_request &req,
    std::vector<boost::mysql::stage_response> &stage_responses)
    -> task<Result<void>> {
  auto pipeline_res = co_await co_as_result(
      conn.async_run_pipeline(req, stage_responses, dagforge::use_nothrow));
  if (!pipeline_res) {
    co_return fail(pipeline_res.error());
  }
  co_return ok();
}

template <typename CompletionToken>
auto acquire_pooled_connection(
    boost::mysql::connection_pool &pool, boost::mysql::diagnostics &diag,
    CompletionToken token) -> task<Result<boost::mysql::pooled_connection>> {
  auto conn_res = co_await co_as_result(
      pool.async_get_connection(diag, std::move(token)));
  if (!conn_res) {
    co_return fail(conn_res.error());
  }
  co_return ok(std::move(*conn_res));
}

auto validate_integer_enum_column(boost::mysql::any_connection &conn,
                                  std::string_view table_name,
                                  std::string_view column_name)
    -> task<Result<void>> {
  auto type_res = co_await fetch_column_data_type(conn, table_name, column_name);
  if (!type_res) {
    co_return fail(type_res.error());
  }

  const auto data_type = std::string_view(*type_res);
  if (is_integerish_sql_type(data_type)) {
    co_return ok();
  }

  if (!is_stringish_sql_type(data_type)) {
    log::error("Unsupported enum storage SQL type for {}.{}: {}", table_name,
               column_name, data_type);
    co_return fail(Error::DatabaseOpenFailed);
  }

  log::error(
      "Legacy string/enum schema for {}.{} is no longer supported; "
      "expected an integer-backed column but found {}",
      table_name, column_name, data_type);
  co_return fail(Error::DatabaseOpenFailed);
}

[[nodiscard]] auto
summarize_task_instance_batch(std::span<const TaskInstanceInfo> instances)
    -> std::string {
  if (instances.empty()) {
    return "batch_size=0";
  }

  std::size_t missing_run_rowid = 0;
  std::size_t missing_task_rowid = 0;
  std::size_t distinct_task_rowids = 0;
  std::unordered_set<std::int64_t> task_rowids;
  task_rowids.reserve(instances.size());
  for (const auto &ti : instances) {
    if (ti.run_rowid <= 0) {
      ++missing_run_rowid;
    }
    if (ti.task_rowid <= 0) {
      ++missing_task_rowid;
    }
    if (ti.task_rowid > 0) {
      task_rowids.insert(ti.task_rowid);
    }
  }
  distinct_task_rowids = task_rowids.size();

  const auto &first = instances.front();
  const auto &last = instances.back();
  return std::format(
      "batch_size={} missing_run_rowid={} missing_task_rowid={} "
      "distinct_task_rowids={} first_task_rowid={} first_attempt={} "
      "first_state={} last_task_rowid={} last_attempt={} last_state={}",
      instances.size(), missing_run_rowid, missing_task_rowid,
      distinct_task_rowids, first.task_rowid, first.attempt,
      enum_to_string(first.state), last.task_rowid, last.attempt,
      enum_to_string(last.state));
}

[[nodiscard]] auto make_pool_params(const DatabaseConfig &cfg)
    -> boost::mysql::pool_params {
  boost::mysql::pool_params params;
  params.server_address.emplace_host_and_port(cfg.host, cfg.port);
  params.username = cfg.username;
  params.password = cfg.password;
  params.database = cfg.database;
  params.initial_size = 1;
  params.max_size = std::max<std::size_t>(1, cfg.pool_size);
  params.thread_safe = true;
  params.connect_timeout = std::chrono::seconds(cfg.connect_timeout);
  params.ssl = boost::mysql::ssl_mode::disable;
  return params;
}

[[nodiscard]] auto as_i64(const boost::mysql::field_view &f) -> std::int64_t {
  if (f.is_int64()) {
    return f.as_int64();
  }
  if (f.is_uint64()) {
    return static_cast<std::int64_t>(f.as_uint64());
  }
  if (f.is_string()) {
    return std::stoll(std::string(f.as_string()));
  }
  return 0;
}

[[nodiscard]] auto as_bool(const boost::mysql::field_view &f) -> bool {
  return as_i64(f) != 0;
}

[[nodiscard]] auto as_sv(const boost::mysql::field_view &f)
    -> std::string_view {
  if (!f.is_string()) {
    return {};
  }
  auto s = f.as_string();
  return std::string_view(s.data(), s.size());
}

[[nodiscard]] auto as_str(const boost::mysql::field_view &f) -> std::string {
  if (f.is_string()) {
    return std::string(as_sv(f));
  }
  if (f.is_int64()) {
    return std::to_string(f.as_int64());
  }
  if (f.is_uint64()) {
    return std::to_string(f.as_uint64());
  }
  return {};
}

[[nodiscard]] auto decode_dag_run_state(const boost::mysql::field_view &f)
    -> DAGRunState {
  return util::parse_enum_code(static_cast<int>(as_i64(f)),
                               DAGRunState::Running);
}

[[nodiscard]] auto decode_task_state(const boost::mysql::field_view &f)
    -> TaskState {
  return util::parse_enum_code(static_cast<int>(as_i64(f)),
                               TaskState::Pending);
}

[[nodiscard]] auto decode_trigger_type(const boost::mysql::field_view &f)
    -> TriggerType {
  return util::parse_enum_code(static_cast<int>(as_i64(f)),
                               TriggerType::Manual);
}

[[nodiscard]] auto to_task_config(const boost::mysql::row_view &row)
    -> Result<TaskConfig> {
  auto exec_type = parse<ExecutorType>(as_sv(row.at(6)));
  auto exec_cfg = executor_config_from_json(as_sv(row.at(7)), exec_type);
  if (!exec_cfg) {
    return fail(exec_cfg.error());
  }
  auto builder = TaskConfig::builder()
                     .id(as_str(row.at(1)))
                     .name(as_str(row.at(2)))
                     .command(as_str(row.at(3)))
                     .working_dir(as_str(row.at(4)))
                     .executor(exec_type)
                     .config(std::move(*exec_cfg))
                     .timeout(std::chrono::seconds(as_i64(row.at(8))))
                     .retry(static_cast<int>(as_i64(row.at(10))),
                            std::chrono::seconds(as_i64(row.at(9))));
  auto task_res = std::move(builder).build();
  if (!task_res) {
    return fail(task_res.error());
  }
  auto task = std::move(*task_res);
  task.task_rowid = as_i64(row.at(0));
  task.trigger_rule = parse<TriggerRule>(as_sv(row.at(11)));
  task.is_branch = as_bool(row.at(12));
  task.branch_xcom_key = as_str(row.at(13));
  task.depends_on_past = as_bool(row.at(14));
  if (auto pushes = xcom::parse_push_configs(as_sv(row.at(15))); pushes) {
    task.xcom_push = std::move(*pushes);
  } else {
    return fail(pushes.error());
  }
  if (auto pulls = xcom::parse_pull_configs(as_sv(row.at(16))); pulls) {
    task.xcom_pull = std::move(*pulls);
  } else {
    return fail(pulls.error());
  }
  return ok(std::move(task));
}

[[nodiscard]] auto to_dag_info(const boost::mysql::row_view &row) -> DAGInfo {
  return DAGInfo{
      .dag_rowid = as_i64(row.at(0)),
      .dag_id = DAGId{as_str(row.at(1))},
      .version = static_cast<int>(as_i64(row.at(2))),
      .name = as_str(row.at(3)),
      .description = as_str(row.at(4)),
      .tags = as_str(row.at(5)),
      .cron = as_str(row.at(6)),
      .timezone = as_str(row.at(7)),
      .max_concurrent_runs = static_cast<int>(as_i64(row.at(8))),
      .start_date = as_i64(row.at(12)) > 0
                        ? std::optional{from_millis(as_i64(row.at(12)))}
                        : std::nullopt,
      .end_date = as_i64(row.at(13)) > 0
                      ? std::optional{from_millis(as_i64(row.at(13)))}
                      : std::nullopt,
      .catchup = as_bool(row.at(9)),
      .is_paused = as_bool(row.at(11)),
      .retention_days = static_cast<int>(as_i64(row.at(16))),
      .created_at = from_millis(as_i64(row.at(14))),
      .updated_at = from_millis(as_i64(row.at(15))),
      .tasks = {},
      .task_index = {},
  };
}

[[nodiscard]] auto to_run_history_entry(const boost::mysql::row_view &row)
    -> DatabaseService::RunHistoryEntry {
  return DatabaseService::RunHistoryEntry{
      .dag_run_id = DAGRunId{as_str(row.at(0))},
      .dag_id = DAGId{as_str(row.at(1))},
      .dag_rowid = as_i64(row.at(2)),
      .run_rowid = as_i64(row.at(3)),
      .dag_version = static_cast<int>(as_i64(row.at(4))),
      .state = decode_dag_run_state(row.at(5)),
      .trigger_type = decode_trigger_type(row.at(6)),
      .scheduled_at = from_millis(as_i64(row.at(7))),
      .started_at = from_millis(as_i64(row.at(8))),
      .finished_at = from_millis(as_i64(row.at(9))),
      .execution_date = from_millis(as_i64(row.at(10))),
  };
}

// Coroutines must own temporary callables across suspension points.
template <typename F>
auto mysql_try(F f, std::string_view op_name = {}, std::string context = {})
    -> task<typename std::invoke_result_t<F &>::value_type> {
  try {
    auto result = co_await f();
    if (!result) {
      co_return fail(result.error());
    }
    if constexpr (std::is_void_v<typename decltype(result)::value_type>) {
      co_return ok();
    } else {
      co_return ok(std::move(*result));
    }
  } catch (const boost::mysql::error_with_diagnostics &e) {
    if (op_name.empty()) {
      log::error("MySQL operation failed: {} ({})", e.what(),
                 describe_mysql_diagnostics(e.get_diagnostics()));
    } else {
      log::error("MySQL operation '{}' failed [{}]: {} ({})", op_name, context,
                 e.what(), describe_mysql_diagnostics(e.get_diagnostics()));
    }
    co_return fail(map_mysql_error_code(e.code()));
  } catch (const std::exception &e) {
    if (op_name.empty()) {
      log::error("MySQL operation failed: {}", e.what());
    } else {
      log::error("MySQL operation '{}' failed [{}]: {}", op_name, context,
                 e.what());
    }
    co_return fail(Error::DatabaseQueryFailed);
  } catch (...) {
    if (op_name.empty()) {
      log::error("MySQL operation failed: unknown exception");
    } else {
      log::error("MySQL operation '{}' failed [{}]: unknown exception", op_name,
                 context);
    }
    co_return fail(Error::DatabaseQueryFailed);
  }
}

[[nodiscard]] auto normalized_task_attempt(const TaskInstanceInfo &info)
    -> int {
  return std::max(info.attempt, 1);
}

} // namespace

struct MySQLDatabase::PoolRunState {
  std::mutex mutex;
  std::condition_variable completed;
  std::size_t active_runs{0};
};

MySQLDatabase::MySQLDatabase(boost::asio::any_io_executor executor,
                             const DatabaseConfig &config)
    : cfg_(config), executor_(executor),
      pool_(executor_, make_pool_params(config)),
      pool_run_state_(std::make_shared<PoolRunState>()),
      connection_acquire_histogram_(
          std::span<const std::uint64_t>(kConnectionAcquireBucketsNs)) {}

MySQLDatabase::~MySQLDatabase() {
  shutdown();
  wait_for_shutdown();
}

auto MySQLDatabase::ensure_database_exists() -> task<Result<void>> {
  auto ex = co_await boost::asio::this_coro::executor;
  std::string direct_connect_error;
  try {
    // Prefer connecting to the configured database directly.
    // This avoids requiring CREATE DATABASE privilege for normal
    // operation/tests.
    boost::mysql::connect_params params;
    params.server_address.emplace_host_and_port(cfg_.host, cfg_.port);
    params.username = cfg_.username;
    params.password = cfg_.password;
    params.database = cfg_.database;
    params.ssl = boost::mysql::ssl_mode::disable;
    params.multi_queries = false;

    boost::mysql::any_connection conn(ex);
    auto connect_res = co_await co_as_result(conn.async_connect(
        params, boost::asio::cancel_after(
                    std::chrono::seconds(cfg_.connect_timeout),
                    dagforge::use_nothrow)));
    if (!connect_res) {
      direct_connect_error = connect_res.error().message();
    } else {
      auto close_res =
          co_await co_as_result(conn.async_close(dagforge::use_nothrow));
      if (!close_res) {
        co_return fail(close_res.error());
      }
      co_return ok();
    }
  } catch (const boost::mysql::error_with_diagnostics &e) {
    direct_connect_error = std::format(
        "{} ({})", e.what(), describe_mysql_diagnostics(e.get_diagnostics()));
  } catch (const std::exception &e) {
    direct_connect_error = e.what();
  }

  try {
    // Fallback for first run: connect without a default DB and create it.
    boost::mysql::connect_params params;
    params.server_address.emplace_host_and_port(cfg_.host, cfg_.port);
    params.username = cfg_.username;
    params.password = cfg_.password;
    params.database.clear();
    params.ssl = boost::mysql::ssl_mode::disable;
    params.multi_queries = false;

    boost::mysql::any_connection conn(ex);
    auto connect_res = co_await co_as_result(conn.async_connect(
        params, boost::asio::cancel_after(
                    std::chrono::seconds(cfg_.connect_timeout),
                    dagforge::use_nothrow)));
    if (!connect_res) {
      log::error("MySQL ensure database failed: direct_connect='{}', "
                 "connect_without_db='{}'",
                 direct_connect_error, connect_res.error().message());
      co_return fail(connect_res.error());
    }

    boost::mysql::results res;
    auto execute_res = co_await co_as_result(conn.async_execute(
        boost::mysql::with_params("CREATE DATABASE IF NOT EXISTS {:i}",
                                  cfg_.database),
        res, dagforge::use_nothrow));
    if (!execute_res) {
      log::error("MySQL ensure database failed: direct_connect='{}', "
                 "create_db='{}'",
                 direct_connect_error, execute_res.error().message());
      co_return fail(execute_res.error());
    }
    auto close_res =
        co_await co_as_result(conn.async_close(dagforge::use_nothrow));
    if (!close_res) {
      log::error("MySQL ensure database failed: direct_connect='{}', "
                 "close_after_create='{}'",
                 direct_connect_error, close_res.error().message());
      co_return fail(close_res.error());
    }
    co_return ok();
  } catch (const boost::mysql::error_with_diagnostics &e) {
    log::error("MySQL ensure database failed: direct_connect='{}', "
               "create_db='{} ({})'",
               direct_connect_error, e.what(),
               describe_mysql_diagnostics(e.get_diagnostics()));
    co_return fail(map_mysql_error_code(e.code()));
  } catch (const std::exception &e) {
    log::error(
        "MySQL ensure database failed: direct_connect='{}', create_db='{}'",
        direct_connect_error, e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::open() -> task<Result<void>> {
  if (open_) {
    co_return ok();
  }

  // connection_pool cannot be restarted after cancel(); recreate it so a
  // close()->open() cycle behaves like a fresh service instance.
  pool_ = boost::mysql::connection_pool(executor_, make_pool_params(cfg_));

  auto db_res = co_await ensure_database_exists();
  if (!db_res) {
    co_return fail(db_res.error());
  }

  open_ = true;
  auto pool_run_state = pool_run_state_;
  {
    std::lock_guard lock(pool_run_state->mutex);
    ++pool_run_state->active_runs;
  }
  pool_.async_run([pool_run_state](boost::mysql::error_code) {
    std::lock_guard lock(pool_run_state->mutex);
    --pool_run_state->active_runs;
    pool_run_state->completed.notify_all();
  });

  auto conn_res = co_await get_connection();
  if (!conn_res) {
    const auto open_error = conn_res.error();
    if (auto close_res = co_await close(); !close_res) {
      log::warn("MySQL pool shutdown after connection failure failed: {}",
                close_res.error().message());
    }
    co_return fail(open_error);
  }

  auto schema_res = co_await ensure_schema(conn_res->get());
  if (!schema_res) {
    const auto schema_error = schema_res.error();
    if (auto close_res = co_await close(); !close_res) {
      log::warn("MySQL pool shutdown after schema failure failed: {}",
                close_res.error().message());
    }
    co_return fail(schema_error);
  }

  conn_res->return_without_reset();

  log::debug("MySQL database opened: {}:{} / {}", cfg_.host, cfg_.port,
             cfg_.database);
  co_return ok();
}

auto MySQLDatabase::close() -> task<Result<void>> {
  shutdown();

  auto executor = co_await boost::asio::this_coro::executor;
  boost::asio::steady_timer timer(executor);
  while (true) {
    {
      std::lock_guard lock(pool_run_state_->mutex);
      if (pool_run_state_->active_runs == 0) {
        co_return ok();
      }
    }

    timer.expires_after(std::chrono::milliseconds(1));
    auto wait_res = co_await co_as_result(timer.async_wait(use_nothrow));
    if (!wait_res) {
      co_return fail(wait_res.error());
    }
  }
}

auto MySQLDatabase::shutdown() noexcept -> void {
  pool_.cancel();
  open_ = false;
}

auto MySQLDatabase::wait_for_shutdown() noexcept -> void {
  std::unique_lock lock(pool_run_state_->mutex);
  pool_run_state_->completed.wait(
      lock, [this] { return pool_run_state_->active_runs == 0; });
}

auto MySQLDatabase::is_open() const noexcept -> bool { return open_; }

auto MySQLDatabase::get_connection()
    -> task<Result<boost::mysql::pooled_connection>> {
  if (!open_) {
    co_return fail(Error::SystemNotRunning);
  }

  const auto started_at = std::chrono::steady_clock::now();
  struct ConnectionAcquireMetricsGuard {
    MySQLDatabase &db;
    std::chrono::steady_clock::time_point started_at;
    ~ConnectionAcquireMetricsGuard() {
      const auto elapsed_ns =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now() - started_at)
              .count();
      db.connection_acquire_histogram_.observe_ns(
          static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
    }
  } guard{*this, started_at};

  try {
    boost::mysql::diagnostics diag;
    auto conn_res = co_await acquire_pooled_connection(
        pool_, diag,
        boost::asio::cancel_after(std::chrono::seconds(cfg_.connect_timeout),
                                  dagforge::use_nothrow));
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    co_return conn_res;
  } catch (const boost::mysql::error_with_diagnostics &e) {
    connection_acquire_failures_total_.fetch_add(1, std::memory_order_relaxed);
    log::error("MySQL get connection failed: {} ({})", e.what(),
               describe_mysql_diagnostics(e.get_diagnostics()));
    co_return fail(map_mysql_error_code(e.code()));
  } catch (const std::exception &e) {
    connection_acquire_failures_total_.fetch_add(1, std::memory_order_relaxed);
    log::error("MySQL get connection failed: {}", e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::connection_acquire_histogram() const
    -> metrics::Histogram::Snapshot {
  return connection_acquire_histogram_.snapshot();
}

auto MySQLDatabase::connection_acquire_failures_total() const noexcept
    -> std::uint64_t {
  return connection_acquire_failures_total_.load(std::memory_order_relaxed);
}

auto MySQLDatabase::ensure_schema(boost::mysql::any_connection &conn)
    -> task<Result<void>> {
  try {
    const auto stmts = split_sql_statements(schema::V1_SCHEMA);
    boost::mysql::pipeline_request req;
    for (const auto &stmt_sql : stmts) {
      if (stmt_sql.empty()) {
        continue;
      }
      req.add_execute(stmt_sql);
    }

    req.add_execute("INSERT IGNORE INTO schema_version(version) VALUES (" +
                    std::to_string(schema::CURRENT_SCHEMA_VERSION) + ")");

    std::vector<boost::mysql::stage_response> stage_responses;
    DAGFORGE_TRY_MYSQL(run_pipeline_mysql(conn, req, stage_responses));

    boost::mysql::results alter_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE task_instances "
        "ADD COLUMN IF NOT EXISTS execution_date BIGINT NOT NULL DEFAULT 0 "
        "AFTER last_heartbeat",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE task_instances "
        "ADD INDEX IF NOT EXISTS idx_ti_queue (state, execution_date)",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE task_instances "
        "ADD INDEX IF NOT EXISTS idx_ti_history "
        "(task_rowid, execution_date, attempt)",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE dag_tasks "
        "ADD COLUMN IF NOT EXISTS is_branch TINYINT NOT NULL DEFAULT 0 "
        "AFTER trigger_rule",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE dag_tasks "
        "ADD COLUMN IF NOT EXISTS branch_xcom_key VARCHAR(255) NOT NULL "
        "DEFAULT 'branch' AFTER is_branch",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE dag_tasks "
        "ADD COLUMN IF NOT EXISTS depends_on_past TINYINT NOT NULL DEFAULT 0 "
        "AFTER branch_xcom_key",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE dag_tasks "
        "ADD COLUMN IF NOT EXISTS xcom_push JSON NOT NULL DEFAULT ('[]') "
        "AFTER depends_on_past",
        alter_res));
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "ALTER TABLE dag_tasks "
        "ADD COLUMN IF NOT EXISTS xcom_pull JSON NOT NULL DEFAULT ('[]') "
        "AFTER xcom_push",
        alter_res));
    if (auto r =
            co_await validate_integer_enum_column(conn, "dag_runs", "state");
        !r) {
      co_return fail(r.error());
    }
    if (auto r = co_await validate_integer_enum_column(conn, "dag_runs",
                                                       "trigger_type");
        !r) {
      co_return fail(r.error());
    }
    if (auto r = co_await validate_integer_enum_column(conn, "task_instances",
                                                       "state");
        !r) {
      co_return fail(r.error());
    }
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        "UPDATE task_instances ti "
        "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
        "SET ti.execution_date = r.execution_date "
        "WHERE ti.execution_date = 0",
        alter_res));

    co_return ok();
  } catch (const boost::mysql::error_with_diagnostics &e) {
    log::error("MySQL schema ensure failed: {} ({})", e.what(),
               describe_mysql_diagnostics(e.get_diagnostics()));
    co_return fail(map_mysql_error_code(e.code()));
  } catch (const std::exception &e) {
    log::error("MySQL schema ensure failed: {}", e.what());
    co_return fail(Error::DatabaseOpenFailed);
  }
}

auto MySQLDatabase::get_dag_rowid(boost::mysql::any_connection &conn,
                                  const DAGId &dag_id)
    -> task<Result<int64_t>> {
  try {
    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        boost::mysql::with_params(
            "SELECT dag_rowid FROM dags WHERE dag_id = {}", dag_id.str()),
        res));
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    co_return ok(as_i64(res.rows().at(0).at(0)));
  } catch (const std::exception &e) {
    log::error("MySQL get_dag_rowid failed: {}", e.what());
    co_return fail(Error::DatabaseQueryFailed);
  }
}

auto MySQLDatabase::save_dag_run_on_connection(
    boost::mysql::any_connection &conn, const DAGRun &run)
    -> task<Result<int64_t>> {
  boost::mysql::results res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO dag_runs(dag_run_id, dag_rowid, dag_version, state, "
          "trigger_type, scheduled_at, started_at, finished_at, "
          "execution_date) "
          "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}) "
          "ON DUPLICATE KEY UPDATE run_rowid=LAST_INSERT_ID(run_rowid), "
          "state=VALUES(state), trigger_type=VALUES(trigger_type), "
          "scheduled_at=VALUES(scheduled_at), started_at=VALUES(started_at), "
          "finished_at=VALUES(finished_at), "
          "execution_date=VALUES(execution_date)",
          run.id().str(), run.dag_rowid(), run.dag_version(),
          util::enum_to_code(run.state()),
          util::enum_to_code(run.trigger_type()), to_millis(run.scheduled_at()),
          to_millis(run.started_at()), to_millis(run.finished_at()),
          to_millis(run.execution_date())),
      res));
  co_return ok(static_cast<int64_t>(res.last_insert_id()));
}

auto MySQLDatabase::save_task_instances_batch_on_connection(
    boost::mysql::any_connection &conn, const DAGRunId &run_id,
    const std::vector<TaskInstanceInfo> &instances, int64_t run_rowid,
    std::int64_t execution_date_ms) -> task<Result<void>> {
  if (instances.empty()) {
    co_return ok();
  }

  if (run_rowid <= 0 || execution_date_ms < 0) {
    boost::mysql::results run_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        boost::mysql::with_params(
            "SELECT run_rowid, execution_date FROM dag_runs "
            "WHERE dag_run_id = {}",
            run_id.str()),
        run_res));
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    run_rowid = as_i64(run_res.rows().at(0).at(0));
    execution_date_ms = as_i64(run_res.rows().at(0).at(1));
  }

  const auto now_ms = to_millis(std::chrono::system_clock::now());
  auto format_ti = [run_rowid, execution_date_ms,
                    now_ms](const TaskInstanceInfo &ti,
                            boost::mysql::format_context_base &ctx) {
    const auto attempt = normalized_task_attempt(ti);
    const auto heartbeat_ms =
        ti.state == TaskState::Running
            ? std::max<std::int64_t>(to_millis(ti.started_at), now_ms)
            : 0;
    boost::mysql::format_sql_to(
        ctx, "({}, {}, {}, {}, '', {}, {}, {}, {}, {}, {}, {}, {})", run_rowid,
        ti.task_rowid, attempt, util::enum_to_code(ti.state), heartbeat_ms,
        execution_date_ms, to_millis(ti.started_at), to_millis(ti.finished_at),
        now_ms, ti.exit_code, ti.error_message, ti.error_type);
  };

  boost::mysql::results upsert_res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO task_instances(run_rowid, task_rowid, attempt, state, "
          "worker_id, last_heartbeat, execution_date, started_at, finished_at, "
          "updated_at, exit_code, error_message, error_type) VALUES {} "
          "ON DUPLICATE KEY UPDATE state=VALUES(state), "
          "last_heartbeat=IF(VALUES(state)={}, VALUES(last_heartbeat), "
          "last_heartbeat), execution_date=VALUES(execution_date), "
          "started_at=VALUES(started_at), "
          "finished_at=VALUES(finished_at), updated_at=VALUES(updated_at), "
          "exit_code=VALUES(exit_code), error_message=VALUES(error_message), "
          "error_type=VALUES(error_type)",
          boost::mysql::sequence(std::cref(instances), format_ti),
          util::enum_to_code(TaskState::Running)),
      upsert_res));
  co_return ok();
}

auto MySQLDatabase::save_dag(const DAGInfo &dag) -> task<Result<int64_t>> {
  co_return co_await mysql_try([&]() -> task<Result<int64_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto rowid_res = co_await save_dag_on_connection(conn_res->get(), dag);
    if (!rowid_res) {
      co_return fail(rowid_res.error());
    }
    conn_res->return_without_reset();
    co_return rowid_res;
  });
}

auto MySQLDatabase::save_dag_on_connection(boost::mysql::any_connection &conn,
                                           const DAGInfo &dag)
    -> task<Result<int64_t>> {
  boost::mysql::results res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO dags(dag_id, version, name, description, tags, cron, "
          "timezone, "
          "max_concurrent_runs, catchup, is_active, is_paused, start_date, "
          "end_date, "
          "created_at, updated_at, retention_days) "
          "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "
          "{}, {}) "
          "ON DUPLICATE KEY UPDATE "
          "dag_rowid=LAST_INSERT_ID(dag_rowid), "
          "version=VALUES(version), name=VALUES(name), "
          "description=VALUES(description), "
          "tags=VALUES(tags), cron=VALUES(cron), timezone=VALUES(timezone), "
          "max_concurrent_runs=VALUES(max_concurrent_runs), "
          "catchup=VALUES(catchup), "
          "is_paused=VALUES(is_paused), start_date=VALUES(start_date), "
          "end_date=VALUES(end_date), "
          "updated_at=VALUES(updated_at), "
          "retention_days=VALUES(retention_days)",
          dag.dag_id.str(), dag.version, dag.name, dag.description,
          dag.tags.empty() ? "[]" : dag.tags, dag.cron, dag.timezone,
          dag.max_concurrent_runs, dag.catchup ? 1 : 0, 1,
          dag.is_paused ? 1 : 0,
          dag.start_date ? to_millis(*dag.start_date) : 0,
          dag.end_date ? to_millis(*dag.end_date) : 0,
          to_millis(dag.created_at), to_millis(dag.updated_at),
          dag.retention_days),
      res));
  co_return ok(static_cast<int64_t>(res.last_insert_id()));
}

auto MySQLDatabase::set_dag_active(const DAGId &dag_id, bool active)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE dags SET is_active = {} WHERE dag_id = {}", active ? 1 : 0,
            dag_id.str()),
        res));

    if (res.affected_rows() == 0) {
      boost::mysql::results exists_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn_res->get(),
          boost::mysql::with_params(
              "SELECT EXISTS(SELECT 1 FROM dags WHERE dag_id = {})",
              dag_id.str()),
          exists_res));
      if (!as_bool(exists_res.rows().at(0).at(0))) {
        co_return fail(Error::NotFound);
      }
    }
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::set_dag_paused(const DAGId &dag_id, bool paused)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE dags SET is_paused = {} WHERE dag_id = {}", paused ? 1 : 0,
            dag_id.str()),
        res));

    if (res.affected_rows() == 0) {
      boost::mysql::results exists_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn_res->get(),
          boost::mysql::with_params(
              "SELECT EXISTS(SELECT 1 FROM dags WHERE dag_id = {})",
              dag_id.str()),
          exists_res));
      if (!as_bool(exists_res.rows().at(0).at(0))) {
        co_return fail(Error::NotFound);
      }
    }
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_dag_active(const DAGId &dag_id) -> task<Result<bool>> {
  co_return co_await mysql_try([&]() -> task<Result<bool>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT is_active FROM dags WHERE dag_id = {}", dag_id.str()),
        res));
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto active = as_bool(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(active);
  });
}

auto MySQLDatabase::delete_dag(const DAGId &dag_id) -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params("DELETE FROM dags WHERE dag_id = {}",
                                  dag_id.str()),
        res));
    conn_res->return_without_reset();
    if (res.affected_rows() == 0) {
      co_return fail(Error::NotFound);
    }
    co_return ok();
  });
}

auto MySQLDatabase::get_tasks_on_connection(
    boost::mysql::any_connection &conn, int64_t dag_rowid)
    -> task<Result<std::vector<TaskConfig>>> {
  boost::mysql::results res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "SELECT task_rowid, task_id, name, command, working_dir, "
          "dag_rowid, executor, "
          "executor_config, timeout, retry_interval, max_retries, "
          "trigger_rule, is_branch, branch_xcom_key, depends_on_past, "
          "xcom_push, xcom_pull "
          "FROM dag_tasks WHERE dag_rowid = {} ORDER BY task_rowid",
          dag_rowid),
      res));

  std::vector<TaskConfig> out;
  out.reserve(res.rows().size());
  for (auto row : res.rows()) {
    auto task = to_task_config(row);
    if (!task) {
      co_return fail(task.error());
    }
    out.emplace_back(std::move(*task));
  }
  co_return ok(std::move(out));
}

auto MySQLDatabase::get_tasks(const DAGId &dag_id)
    -> task<Result<std::vector<TaskConfig>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<TaskConfig>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    auto tasks_res =
        co_await get_tasks_on_connection(conn_res->get(), *dag_rowid_res);
    if (!tasks_res) {
      co_return fail(tasks_res.error());
    }
    conn_res->return_without_reset();
    co_return ok(std::move(*tasks_res));
  });
}

auto MySQLDatabase::get_task_dependencies(const DAGId &dag_id)
    -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<std::pair<TaskId, TaskId>>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
        if (!dag_rowid_res) {
          co_return fail(dag_rowid_res.error());
        }

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT t.task_id, u.task_id "
                "FROM task_dependencies d "
                "JOIN dag_tasks t ON t.task_rowid = d.task_rowid "
                "JOIN dag_tasks u ON u.task_rowid = d.depends_on_task_rowid "
                "WHERE d.dag_rowid = {}",
                *dag_rowid_res),
            res));

        std::vector<std::pair<TaskId, TaskId>> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(TaskId{as_str(row.at(0))},
                           TaskId{as_str(row.at(1))});
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::get_dag(const DAGId &dag_id) -> task<Result<DAGInfo>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGInfo>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params("SELECT dag_rowid, dag_id, version, name, "
                                  "description, tags, cron, timezone, "
                                  "max_concurrent_runs, catchup, is_active, "
                                  "is_paused, start_date, end_date, "
                                  "created_at, updated_at, retention_days "
                                  "FROM dags WHERE dag_id = {}",
                                  dag_id.str()),
        res));

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    auto dag = to_dag_info(res.rows().at(0));

    auto tasks_res = co_await get_tasks(dag_id);
    if (!tasks_res) {
      co_return fail(tasks_res.error());
    }
    dag.tasks = std::move(*tasks_res);

    auto deps_res = co_await get_task_dependencies(dag_id);
    if (deps_res) {
      Arena<16384> arena;
      pmr::unordered_map<TaskId, std::size_t> idx(arena.resource());
      idx.reserve(dag.tasks.size());
      for (std::size_t i = 0; i < dag.tasks.size(); ++i) {
        idx.emplace(dag.tasks[i].task_id, i);
      }
      for (const auto &[task_id, dep_id] : *deps_res) {
        auto it = idx.find(task_id);
        if (it != idx.end()) {
          dag.tasks[it->second].dependencies.emplace_back(
              TaskDependency{.task_id = dep_id.clone(), .label = ""});
        }
      }
    }

    dag.rebuild_task_index();
    conn_res->return_without_reset();
    co_return ok(std::move(dag));
  });
}

auto MySQLDatabase::get_dag_by_rowid(int64_t dag_rowid)
    -> task<Result<DAGInfo>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGInfo>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT dag_id FROM dags WHERE dag_rowid = {}", dag_rowid),
        res));
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto dag_id = DAGId{as_str(res.rows().at(0).at(0))};
    conn_res->return_without_reset();
    auto dag_res = co_await get_dag(dag_id);
    if (!dag_res) {
      co_return fail(dag_res.error());
    }
    co_return ok(std::move(*dag_res));
  });
}

auto MySQLDatabase::list_dags() -> task<Result<std::vector<DAGInfo>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<DAGInfo>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        "SELECT dag_rowid, dag_id, version, name, description, tags, cron, "
        "timezone, "
        "max_concurrent_runs, catchup, is_active, is_paused, start_date, "
        "end_date, "
        "created_at, updated_at, retention_days "
        "FROM dags ORDER BY dag_id",
        res));

    std::vector<DAGInfo> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(to_dag_info(row));
    }

    if (!out.empty()) {
      std::unordered_map<std::int64_t, std::size_t> dag_index_by_rowid;
      dag_index_by_rowid.reserve(out.size());
      for (std::size_t i = 0; i < out.size(); ++i) {
        dag_index_by_rowid.emplace(out[i].dag_rowid, i);
      }

      boost::mysql::results task_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn_res->get(),
          "SELECT task_rowid, task_id, name, command, working_dir, "
          "dag_rowid, executor, executor_config, timeout, retry_interval, "
          "max_retries, trigger_rule, is_branch, branch_xcom_key, "
          "depends_on_past, xcom_push, xcom_pull "
          "FROM dag_tasks ORDER BY dag_rowid, task_rowid",
          task_res));

      for (auto row : task_res.rows()) {
        const auto dag_rowid = as_i64(row.at(5));
        auto dag_it = dag_index_by_rowid.find(dag_rowid);
        if (dag_it != dag_index_by_rowid.end()) {
          auto task = to_task_config(row);
          if (!task) {
            co_return fail(task.error());
          }
          out[dag_it->second].tasks.emplace_back(std::move(*task));
        }
      }

      boost::mysql::results dep_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn_res->get(),
          "SELECT d.dag_rowid, t.task_id, u.task_id "
          "FROM task_dependencies d "
          "JOIN dag_tasks t ON t.task_rowid = d.task_rowid "
          "JOIN dag_tasks u ON u.task_rowid = d.depends_on_task_rowid "
          "ORDER BY d.dag_rowid, d.task_rowid",
          dep_res));

      std::vector<std::unordered_map<TaskId, std::size_t>> task_indexes;
      task_indexes.reserve(out.size());
      for (std::size_t i = 0; i < out.size(); ++i) {
        task_indexes.emplace_back();
        task_indexes.back().reserve(out[i].tasks.size());
        for (std::size_t task_idx = 0; task_idx < out[i].tasks.size();
             ++task_idx) {
          task_indexes.back().emplace(out[i].tasks[task_idx].task_id, task_idx);
        }
      }

      for (auto row : dep_res.rows()) {
        const auto dag_rowid = as_i64(row.at(0));
        auto dag_it = dag_index_by_rowid.find(dag_rowid);
        if (dag_it == dag_index_by_rowid.end()) {
          continue;
        }
        const auto dag_idx = dag_it->second;
        auto task_it = task_indexes[dag_idx].find(TaskId{as_str(row.at(1))});
        if (task_it == task_indexes[dag_idx].end()) {
          continue;
        }
        out[dag_idx].tasks[task_it->second].dependencies.emplace_back(
            TaskDependency{.task_id = TaskId{as_str(row.at(2))}, .label = ""});
      }
    }

    for (auto &dag : out) {
      dag.rebuild_task_index();
    }

    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::list_dag_states()
    -> task<Result<std::vector<DagStateRecord>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<DagStateRecord>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            "SELECT dag_rowid, dag_id, version, is_paused, created_at, "
            "updated_at "
            "FROM dags ORDER BY dag_id",
            res));

        std::vector<DagStateRecord> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(DagStateRecord{
              .dag_id = DAGId{as_str(row.at(1))},
              .dag_rowid = as_i64(row.at(0)),
              .version = static_cast<int>(as_i64(row.at(2))),
              .is_paused = as_bool(row.at(3)),
              .created_at = from_millis(as_i64(row.at(4))),
              .updated_at = from_millis(as_i64(row.at(5))),
          });
        }

        if (!out.empty()) {
          std::unordered_map<std::int64_t, std::size_t> dag_index_by_rowid;
          dag_index_by_rowid.reserve(out.size());
          for (std::size_t i = 0; i < out.size(); ++i) {
            dag_index_by_rowid.emplace(out[i].dag_rowid, i);
          }

          boost::mysql::results task_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(
              conn_res->get(),
              "SELECT dag_rowid, task_id, task_rowid "
              "FROM dag_tasks ORDER BY dag_rowid, task_rowid",
              task_res));

          for (auto row : task_res.rows()) {
            const auto dag_rowid = as_i64(row.at(0));
            auto dag_it = dag_index_by_rowid.find(dag_rowid);
            if (dag_it != dag_index_by_rowid.end()) {
              out[dag_it->second].task_rowids.insert_or_assign(
                  TaskId{as_str(row.at(1))}, as_i64(row.at(2)));
            }
          }
        }

        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::save_task(const DAGId &dag_id, const TaskConfig &t)
    -> task<Result<int64_t>> {
  co_return co_await mysql_try([&]() -> task<Result<int64_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }
    auto rowid = co_await save_task_on_connection(conn_res->get(), *dag_rowid_res,
                                                  t);
    if (!rowid) {
      co_return fail(rowid.error());
    }
    conn_res->return_without_reset();
    co_return rowid;
  });
}

auto MySQLDatabase::save_task_on_connection(boost::mysql::any_connection &conn,
                                            int64_t dag_rowid,
                                            const TaskConfig &task_cfg)
    -> task<Result<int64_t>> {
  boost::mysql::results res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO dag_tasks(dag_rowid, task_id, name, command, "
          "working_dir, executor, "
          "executor_config, timeout, retry_interval, max_retries, "
          "trigger_rule, is_branch, branch_xcom_key, depends_on_past, "
          "xcom_push, xcom_pull) "
          "VALUES({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "
          "{}, {}) "
          "ON DUPLICATE KEY UPDATE "
          "task_rowid=LAST_INSERT_ID(task_rowid), "
          "name=VALUES(name), command=VALUES(command), "
          "working_dir=VALUES(working_dir), executor=VALUES(executor), "
          "executor_config=VALUES(executor_config), timeout=VALUES(timeout), "
          "retry_interval=VALUES(retry_interval), "
          "max_retries=VALUES(max_retries), "
          "trigger_rule=VALUES(trigger_rule), "
          "is_branch=VALUES(is_branch), "
          "branch_xcom_key=VALUES(branch_xcom_key), "
          "depends_on_past=VALUES(depends_on_past), "
          "xcom_push=VALUES(xcom_push), xcom_pull=VALUES(xcom_pull)",
          dag_rowid, task_cfg.task_id.str(), task_cfg.name, task_cfg.command,
          task_cfg.working_dir, task_cfg.executor,
          ExecutorRegistry::instance().serialize_config(task_cfg.executor_config),
          static_cast<int>(task_cfg.execution_timeout.count()),
          static_cast<int>(task_cfg.retry_interval.count()),
          task_cfg.max_retries, enum_to_string(task_cfg.trigger_rule),
          task_cfg.is_branch, task_cfg.branch_xcom_key,
          task_cfg.depends_on_past,
          xcom::serialize_push_configs(task_cfg.xcom_push),
          xcom::serialize_pull_configs(task_cfg.xcom_pull)),
      res));
  co_return ok(static_cast<int64_t>(res.last_insert_id()));
}

auto MySQLDatabase::save_tasks_on_connection(boost::mysql::any_connection &conn,
                                             int64_t dag_rowid,
                                             std::vector<TaskConfig> &tasks)
    -> task<Result<void>> {
  if (tasks.empty()) {
    co_return ok();
  }

  auto format_task = [dag_rowid](const TaskConfig &task,
                                 boost::mysql::format_context_base &ctx) {
    boost::mysql::format_sql_to(
        ctx, "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
        dag_rowid, task.task_id.str(), task.name, task.command, task.working_dir,
        task.executor,
        ExecutorRegistry::instance().serialize_config(task.executor_config),
        static_cast<int>(task.execution_timeout.count()),
        static_cast<int>(task.retry_interval.count()), task.max_retries,
        enum_to_string(task.trigger_rule), task.is_branch, task.branch_xcom_key,
        task.depends_on_past, xcom::serialize_push_configs(task.xcom_push),
        xcom::serialize_pull_configs(task.xcom_pull));
  };

  boost::mysql::results upsert_res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO dag_tasks(dag_rowid, task_id, name, command, "
          "working_dir, executor, executor_config, timeout, retry_interval, "
          "max_retries, trigger_rule, is_branch, branch_xcom_key, "
          "depends_on_past, xcom_push, xcom_pull) VALUES {} "
          "ON DUPLICATE KEY UPDATE "
          "name=VALUES(name), command=VALUES(command), "
          "working_dir=VALUES(working_dir), executor=VALUES(executor), "
          "executor_config=VALUES(executor_config), timeout=VALUES(timeout), "
          "retry_interval=VALUES(retry_interval), "
          "max_retries=VALUES(max_retries), trigger_rule=VALUES(trigger_rule), "
          "is_branch=VALUES(is_branch), "
          "branch_xcom_key=VALUES(branch_xcom_key), "
          "depends_on_past=VALUES(depends_on_past), "
          "xcom_push=VALUES(xcom_push), xcom_pull=VALUES(xcom_pull)",
          boost::mysql::sequence(std::cref(tasks), format_task)),
      upsert_res));

  std::vector<std::string> task_ids;
  task_ids.reserve(tasks.size());
  for (const auto &task : tasks) {
    task_ids.emplace_back(task.task_id.str());
  }

  auto format_task_id = [](const std::string &task_id,
                           boost::mysql::format_context_base &ctx) {
    boost::mysql::format_sql_to(ctx, "{}", task_id);
  };

  boost::mysql::results rowids_res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "SELECT task_id, task_rowid FROM dag_tasks WHERE dag_rowid = {} "
          "AND task_id IN ({})",
          dag_rowid,
          boost::mysql::sequence(std::cref(task_ids), format_task_id)),
      rowids_res));

  std::unordered_map<std::string, int64_t> rowids_by_task_id;
  rowids_by_task_id.reserve(rowids_res.rows().size());
  for (auto row : rowids_res.rows()) {
    rowids_by_task_id.emplace(as_str(row.at(0)), as_i64(row.at(1)));
  }

  for (auto &task : tasks) {
    auto it = rowids_by_task_id.find(task.task_id.str());
    if (it == rowids_by_task_id.end()) {
      co_return fail(Error::DatabaseQueryFailed);
    }
    task.task_rowid = it->second;
  }

  co_return ok();
}

auto MySQLDatabase::replace_task_dependencies_on_connection(
    boost::mysql::any_connection &conn, int64_t dag_rowid,
    const std::vector<TaskConfig> &tasks) -> task<Result<void>> {
  boost::mysql::results clear_res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "DELETE FROM task_dependencies WHERE dag_rowid = {}", dag_rowid),
      clear_res));

  struct DependencyRow {
    int64_t task_rowid;
    int64_t depends_on_task_rowid;
  };

  std::unordered_map<std::string, std::int64_t> task_rowid_by_id;
  task_rowid_by_id.reserve(tasks.size());
  for (const auto &task : tasks) {
    if (task.task_rowid <= 0) {
      co_return fail(Error::DatabaseQueryFailed);
    }
    task_rowid_by_id.emplace(task.task_id.str(), task.task_rowid);
  }

  std::vector<DependencyRow> rows;
  for (const auto &task : tasks) {
    std::unordered_set<std::int64_t> seen;
    seen.reserve(task.dependencies.size());
    for (const auto &dep : task.dependencies) {
      auto dep_it = task_rowid_by_id.find(dep.task_id.str());
      if (dep_it == task_rowid_by_id.end()) {
        log::error("replace_task_dependencies_on_connection missing dependency "
                   "lookup for task_id='{}' depends_on='{}'",
                   task.task_id, dep.task_id);
        co_return fail(Error::NotFound);
      }
      if (seen.insert(dep_it->second).second) {
        rows.push_back(DependencyRow{
            .task_rowid = task.task_rowid,
            .depends_on_task_rowid = dep_it->second,
        });
      }
    }
  }

  if (rows.empty()) {
    co_return ok();
  }

  auto format_dep = [dag_rowid](const DependencyRow &row,
                                boost::mysql::format_context_base &ctx) {
    boost::mysql::format_sql_to(ctx, "({}, {}, {}, 'success')", dag_rowid,
                                row.task_rowid, row.depends_on_task_rowid);
  };

  boost::mysql::results insert_res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "INSERT INTO task_dependencies(dag_rowid, task_rowid, "
          "depends_on_task_rowid, dependency_type) VALUES {} "
          "ON DUPLICATE KEY UPDATE dependency_type=VALUES(dependency_type)",
          boost::mysql::sequence(std::cref(rows), format_dep)),
      insert_res));
  co_return ok();
}

auto MySQLDatabase::delete_task_on_connection(
    boost::mysql::any_connection &conn, int64_t dag_rowid,
    const TaskId &task_id) -> task<Result<void>> {
  boost::mysql::results res;
  DAGFORGE_TRY_MYSQL(execute_mysql(
      conn,
      boost::mysql::with_params(
          "DELETE FROM dag_tasks WHERE dag_rowid = {} AND task_id = {}",
          dag_rowid, task_id.str()),
      res));
  if (res.affected_rows() == 0) {
    co_return fail(Error::NotFound);
  }
  co_return ok();
}

auto MySQLDatabase::delete_task(const DAGId &dag_id, const TaskId &task_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    auto delete_res = co_await delete_task_on_connection(
        conn_res->get(), *dag_rowid_res, task_id);
    if (!delete_res) {
      co_return fail(delete_res.error());
    }
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::save_task_dependencies(
    const DAGId &dag_id, const TaskId &task_id,
    const std::vector<TaskId> &dep_task_ids, std::string_view dependency_type)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    std::vector<std::string> lookup_task_ids;
    lookup_task_ids.reserve(dep_task_ids.size() + 1);
    lookup_task_ids.emplace_back(task_id.str());
    for (const auto &dep_id : dep_task_ids) {
      lookup_task_ids.emplace_back(dep_id.str());
    }

    auto format_task_id = [](const std::string &id,
                             boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "{}", id);
    };

    boost::mysql::results rows_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT task_id, task_rowid FROM dag_tasks "
            "WHERE dag_rowid = {} AND task_id IN ({})",
            *dag_rowid_res,
            boost::mysql::sequence(std::cref(lookup_task_ids), format_task_id)),
        rows_res));

    std::unordered_map<std::string, std::int64_t> task_rowid_by_id;
    task_rowid_by_id.reserve(rows_res.rows().size());
    for (auto row : rows_res.rows()) {
      task_rowid_by_id.emplace(as_str(row.at(0)), as_i64(row.at(1)));
    }

    auto it_task_rowid = task_rowid_by_id.find(task_id.str());
    if (it_task_rowid == task_rowid_by_id.end()) {
      co_return fail(Error::NotFound);
    }
    const auto task_rowid = it_task_rowid->second;

    boost::mysql::results del_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params("DELETE FROM task_dependencies WHERE "
                                  "dag_rowid = {} AND task_rowid = {}",
                                  *dag_rowid_res, task_rowid),
        del_res));

    if (dep_task_ids.empty()) {
      conn_res->return_without_reset();
      co_return ok();
    }

    std::vector<std::int64_t> dep_rowids;
    dep_rowids.reserve(dep_task_ids.size());
    std::unordered_set<std::int64_t> unique_dep_rowids;
    unique_dep_rowids.reserve(dep_task_ids.size());
    for (const auto &dep_id : dep_task_ids) {
      if (auto it = task_rowid_by_id.find(dep_id.str());
          it != task_rowid_by_id.end() &&
          unique_dep_rowids.insert(it->second).second) {
        dep_rowids.emplace_back(it->second);
      }
    }

    if (dep_rowids.empty()) {
      conn_res->return_without_reset();
      co_return ok();
    }

    const std::string dep_type(dependency_type);
    auto format_dep = [dag_rowid = *dag_rowid_res, task_rowid,
                       &dep_type](const std::int64_t &dep_rowid,
                                  boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "({}, {}, {}, {})", dag_rowid,
                                  task_rowid, dep_rowid, dep_type);
    };

    boost::mysql::results ins_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "INSERT INTO task_dependencies(dag_rowid, task_rowid, "
            "depends_on_task_rowid, "
            "dependency_type) VALUES {} "
            "ON DUPLICATE KEY UPDATE dependency_type=VALUES(dependency_type)",
            boost::mysql::sequence(std::cref(dep_rowids), format_dep)),
        ins_res));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::clear_task_dependencies(const DAGId &dag_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "DELETE FROM task_dependencies WHERE dag_rowid = {}",
            *dag_rowid_res),
        res));
    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::save_dag_run(const DAGRun &run) -> task<Result<int64_t>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<int64_t>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }
        auto rowid_res =
            co_await save_dag_run_on_connection(conn_res->get(), run);
        if (!rowid_res) {
          co_return fail(rowid_res.error());
        }
        conn_res->return_without_reset();
        co_return rowid_res;
      },
      "save_dag_run",
      std::format("dag_run_id={} dag_rowid={} state={} trigger={}", run.id(),
                  run.dag_rowid(), enum_to_string(run.state()),
                  enum_to_string(run.trigger_type())));
}

auto MySQLDatabase::create_run_with_task_instances_transaction(
    const DAGRun &run, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<int64_t>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<int64_t>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        auto &conn = conn_res->get();
        boost::mysql::results tx_res;
        DAGFORGE_TRY_MYSQL(
            execute_mysql(conn, "START TRANSACTION", tx_res));

        auto rowid_res = co_await save_dag_run_on_connection(conn, run);
        if (!rowid_res) {
          boost::mysql::results rollback_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(conn, "ROLLBACK", rollback_res));
          co_return fail(rowid_res.error());
        }

        std::vector<TaskInstanceInfo> instances_with_rowid = instances;
        for (auto &ti : instances_with_rowid) {
          ti.run_rowid = *rowid_res;
        }

        auto batch_res = co_await save_task_instances_batch_on_connection(
            conn, run.id(), instances_with_rowid, *rowid_res,
            to_millis(run.execution_date()));
        if (!batch_res) {
          boost::mysql::results rollback_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(conn, "ROLLBACK", rollback_res));
          co_return fail(batch_res.error());
        }

        boost::mysql::results commit_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(conn, "COMMIT", commit_res));
        conn_res->return_without_reset();
        co_return rowid_res;
      },
      "create_run_with_task_instances_transaction",
      std::format("dag_run_id={} dag_rowid={} {}", run.id(), run.dag_rowid(),
                  summarize_task_instance_batch(instances)));
}

auto MySQLDatabase::acquire_batch_writer_connection()
    -> task<Result<boost::mysql::pooled_connection>> {
  return get_connection();
}

auto MySQLDatabase::create_runs_with_task_instances_transaction(
    boost::mysql::any_connection &conn,
    const std::vector<RunInsertBundle> &bundles)
    -> task<Result<std::vector<int64_t>>> {
  if (bundles.empty()) {
    co_return ok(std::vector<int64_t>{});
  }

  auto rowids_res = co_await mysql_try(
      [&]() -> task<Result<std::vector<int64_t>>> {
        auto format_run = [](const RunInsertBundle &bundle,
                             boost::mysql::format_context_base &ctx) {
          const auto &run = bundle.run;
          boost::mysql::format_sql_to(
              ctx, "({}, {}, {}, {}, {}, {}, {}, {}, {})", run.id().str(),
              run.dag_rowid(), run.dag_version(),
              util::enum_to_code(run.state()),
              util::enum_to_code(run.trigger_type()),
              to_millis(run.scheduled_at()), to_millis(run.started_at()),
              to_millis(run.finished_at()), to_millis(run.execution_date()));
        };
        auto format_run_id = [](const RunInsertBundle &bundle,
                                boost::mysql::format_context_base &ctx) {
          boost::mysql::format_sql_to(ctx, "{}", bundle.run.id().str());
        };

        boost::mysql::results tx_res;
        DAGFORGE_TRY_MYSQL(
            execute_mysql(conn, "START TRANSACTION", tx_res));

        boost::mysql::results run_insert_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn,
            boost::mysql::with_params(
                "INSERT INTO dag_runs(dag_run_id, dag_rowid, dag_version, "
                "state, "
                "trigger_type, scheduled_at, started_at, finished_at, "
                "execution_date) "
                "VALUES {} "
                "ON DUPLICATE KEY UPDATE run_rowid=LAST_INSERT_ID(run_rowid), "
                "state=VALUES(state), trigger_type=VALUES(trigger_type), "
                "scheduled_at=VALUES(scheduled_at), "
                "started_at=VALUES(started_at), "
                "finished_at=VALUES(finished_at), "
                "execution_date=VALUES(execution_date)",
                boost::mysql::sequence(std::cref(bundles), format_run)),
            run_insert_res));

        boost::mysql::results rowid_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn,
            boost::mysql::with_params(
                "SELECT dag_run_id, run_rowid FROM dag_runs "
                "WHERE dag_run_id IN ({})",
                boost::mysql::sequence(std::cref(bundles), format_run_id)),
            rowid_res));

        std::unordered_map<std::string, int64_t> rowids_by_run_id;
        rowids_by_run_id.reserve(rowid_res.rows().size());
        for (auto row : rowid_res.rows()) {
          rowids_by_run_id.emplace(as_str(row.at(0)), as_i64(row.at(1)));
        }

        std::size_t total_instances = 0;
        for (const auto &bundle : bundles) {
          total_instances += bundle.instances.size();
        }

        std::vector<int64_t> rowids;
        rowids.reserve(bundles.size());
        std::unordered_map<int64_t, std::int64_t> execution_date_by_run_rowid;
        execution_date_by_run_rowid.reserve(bundles.size());
        std::vector<TaskInstanceInfo> flattened_instances;
        flattened_instances.reserve(total_instances);
        for (const auto &bundle : bundles) {
          const auto it = rowids_by_run_id.find(bundle.run.id().str());
          if (it == rowids_by_run_id.end()) {
            boost::mysql::results rollback_res;
            DAGFORGE_TRY_MYSQL(execute_mysql(conn, "ROLLBACK", rollback_res));
            co_return fail(Error::DatabaseQueryFailed);
          }

          const auto run_rowid = it->second;
          rowids.push_back(run_rowid);
          execution_date_by_run_rowid.try_emplace(
              run_rowid, to_millis(bundle.run.execution_date()));
          for (const auto &instance : bundle.instances) {
            auto stamped = instance;
            stamped.run_rowid = run_rowid;
            flattened_instances.push_back(std::move(stamped));
          }
        }

        if (!flattened_instances.empty()) {
          const auto now_ms = to_millis(std::chrono::system_clock::now());
          auto format_ti = [&execution_date_by_run_rowid,
                            now_ms](const TaskInstanceInfo &ti,
                                    boost::mysql::format_context_base &ctx) {
            const auto attempt = normalized_task_attempt(ti);
            const auto execution_date_ms =
                execution_date_by_run_rowid.at(ti.run_rowid);
            const auto heartbeat_ms =
                ti.state == TaskState::Running
                    ? std::max<std::int64_t>(to_millis(ti.started_at), now_ms)
                    : 0;
            boost::mysql::format_sql_to(
                ctx, "({}, {}, {}, {}, '', {}, {}, {}, {}, {}, {}, {}, {})",
                ti.run_rowid, ti.task_rowid, attempt,
                util::enum_to_code(ti.state), heartbeat_ms, execution_date_ms,
                to_millis(ti.started_at), to_millis(ti.finished_at), now_ms,
                ti.exit_code, ti.error_message, ti.error_type);
          };

          boost::mysql::results task_upsert_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(
              conn,
              boost::mysql::with_params(
                  "INSERT INTO task_instances(run_rowid, task_rowid, attempt, "
                  "state, worker_id, last_heartbeat, execution_date, "
                  "started_at, "
                  "finished_at, updated_at, exit_code, error_message, "
                  "error_type) VALUES {} "
                  "ON DUPLICATE KEY UPDATE state=VALUES(state), "
                  "last_heartbeat=IF(VALUES(state)={}, VALUES(last_heartbeat), "
                  "last_heartbeat), execution_date=VALUES(execution_date), "
                  "started_at=VALUES(started_at), "
                  "finished_at=VALUES(finished_at), "
                  "updated_at=VALUES(updated_at), "
                  "exit_code=VALUES(exit_code), "
                  "error_message=VALUES(error_message), "
                  "error_type=VALUES(error_type)",
                  boost::mysql::sequence(std::cref(flattened_instances),
                                         format_ti),
                  util::enum_to_code(TaskState::Running)),
              task_upsert_res));
        }

        boost::mysql::results commit_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(conn, "COMMIT", commit_res));
        co_return ok(std::move(rowids));
      },
      "create_runs_with_task_instances_transaction",
      std::format("batch_size={}", bundles.size()));
  if (!rowids_res) {
    co_return fail(rowids_res.error());
  }
  co_return ok(std::move(*rowids_res));
}

auto MySQLDatabase::update_dag_run_state(const DAGRunId &id, DAGRunState state)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE dag_runs SET state = {} WHERE dag_run_id = {}",
            util::enum_to_code(state), id.str()),
        res));

    conn_res->return_without_reset();
    if (res.affected_rows() == 0) {
      co_return fail(Error::NotFound);
    }
    co_return ok();
  });
}

auto MySQLDatabase::get_dag_run_state(const DAGRunId &id)
    -> task<Result<DAGRunState>> {
  co_return co_await mysql_try([&]() -> task<Result<DAGRunState>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT state FROM dag_runs WHERE dag_run_id = {}", id.str()),
        res));
    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }
    auto state = decode_dag_run_state(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(state);
  });
}

auto MySQLDatabase::get_incomplete_dag_runs()
    -> task<Result<std::vector<DAGRunId>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<DAGRunId>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT dag_run_id FROM dag_runs WHERE state IN ({}, {})",
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running)),
        res));

    std::vector<DAGRunId> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(as_str(row.at(0)));
    }
    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::save_task_instance(const DAGRunId &run_id,
                                       const TaskInstanceInfo &info)
    -> task<Result<void>> {
  return update_task_instance(run_id, info);
}

auto MySQLDatabase::update_task_instance(const DAGRunId &run_id,
                                         const TaskInstanceInfo &info)
    -> task<Result<void>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<void>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        auto run_rowid = info.run_rowid;
        std::int64_t execution_date_ms = -1;
        if (run_rowid <= 0) {
          boost::mysql::results run_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(
              conn_res->get(),
              boost::mysql::with_params(
                  "SELECT run_rowid, execution_date FROM dag_runs "
                  "WHERE dag_run_id = {}",
                  run_id.str()),
              run_res));
          if (run_res.rows().empty()) {
            co_return fail(Error::NotFound);
          }
          run_rowid = as_i64(run_res.rows().at(0).at(0));
          execution_date_ms = as_i64(run_res.rows().at(0).at(1));
        }
        if (execution_date_ms < 0) {
          boost::mysql::results run_res;
          DAGFORGE_TRY_MYSQL(execute_mysql(
              conn_res->get(),
              boost::mysql::with_params(
                  "SELECT execution_date FROM dag_runs WHERE run_rowid = {}",
                  run_rowid),
              run_res));
          if (run_res.rows().empty()) {
            co_return fail(Error::NotFound);
          }
          execution_date_ms = as_i64(run_res.rows().at(0).at(0));
        }
        const auto now_ms = to_millis(std::chrono::system_clock::now());
        const auto heartbeat_ms =
            info.state == TaskState::Running
                ? std::max<std::int64_t>(to_millis(info.started_at), now_ms)
                : 0;

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "INSERT INTO task_instances(run_rowid, task_rowid, attempt, "
                "state, "
                "worker_id, last_heartbeat, execution_date, "
                "started_at, "
                "finished_at, updated_at, exit_code, error_message, "
                "error_type) "
                "VALUES({}, {}, {}, {}, '', {}, {}, {}, {}, {}, {}, {}, {}) "
                "ON DUPLICATE KEY UPDATE state=VALUES(state), "
                "last_heartbeat=IF(VALUES(state)={}, VALUES(last_heartbeat), "
                "last_heartbeat), "
                "execution_date=VALUES(execution_date), "
                "started_at=VALUES(started_at), "
                "finished_at=VALUES(finished_at), "
                "updated_at=VALUES(updated_at), "
                "exit_code=VALUES(exit_code), "
                "error_message=VALUES(error_message), "
                "error_type=VALUES(error_type)",
                run_rowid, info.task_rowid, normalized_task_attempt(info),
                util::enum_to_code(info.state), heartbeat_ms, execution_date_ms,
                to_millis(info.started_at), to_millis(info.finished_at), now_ms,
                info.exit_code, info.error_message, info.error_type,
                util::enum_to_code(TaskState::Running)),
            res));

        conn_res->return_without_reset();
        co_return ok();
      },
      "update_task_instance",
      std::format(
          "dag_run_id={} run_rowid={} task_rowid={} attempt={} state={}",
          run_id, info.run_rowid, info.task_rowid, info.attempt,
          enum_to_string(info.state)));
}

auto MySQLDatabase::get_task_instances(const DAGRunId &run_id)
    -> task<Result<std::vector<TaskInstanceInfo>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<TaskInstanceInfo>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results run_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
                run_id.str()),
            run_res));
        if (run_res.rows().empty()) {
          co_return fail(Error::NotFound);
        }
        auto run_rowid = as_i64(run_res.rows().at(0).at(0));

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT ti.task_rowid, t.task_id, ti.attempt, ti.state, "
                "ti.started_at, ti.finished_at, ti.exit_code, "
                "ti.error_message, ti.error_type "
                "FROM task_instances ti "
                "JOIN dag_tasks t ON t.task_rowid = ti.task_rowid "
                "WHERE ti.run_rowid = {}",
                run_rowid),
            res));

        std::vector<TaskInstanceInfo> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          TaskInstanceInfo info;
          info.task_rowid = as_i64(row.at(0));
          info.task_id = TaskId{as_str(row.at(1))};
          info.attempt = static_cast<int>(as_i64(row.at(2)));
          info.state = decode_task_state(row.at(3));
          info.started_at = from_millis(as_i64(row.at(4)));
          info.finished_at = from_millis(as_i64(row.at(5)));
          info.exit_code = static_cast<int>(as_i64(row.at(6)));
          info.error_message = as_str(row.at(7));
          info.error_type = as_str(row.at(8));
          info.run_rowid = run_rowid;
          out.emplace_back(std::move(info));
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::save_task_instances_batch(
    const DAGRunId &run_id, const std::vector<TaskInstanceInfo> &instances)
    -> task<Result<void>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<void>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }
        auto run_rowid = instances.empty() ? -1 : instances.front().run_rowid;
        auto batch_res = co_await save_task_instances_batch_on_connection(
            conn_res->get(), run_id, instances, run_rowid, -1);
        if (!batch_res) {
          co_return fail(batch_res.error());
        }
        conn_res->return_without_reset();
        co_return ok();
      },
      "save_task_instances_batch",
      std::format("dag_run_id={} {}", run_id,
                  summarize_task_instance_batch(instances)));
}

auto MySQLDatabase::list_run_history(std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<RunHistoryEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
                "r.dag_version, "
                "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
                "r.finished_at, "
                "r.execution_date "
                "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
                "ORDER BY r.execution_date DESC LIMIT {}",
                limit),
            res));

        std::vector<RunHistoryEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(to_run_history_entry(row));
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::list_dag_run_history(const DAGId &dag_id, std::size_t limit)
    -> task<Result<std::vector<RunHistoryEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<RunHistoryEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
                "r.dag_version, "
                "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
                "r.finished_at, "
                "r.execution_date "
                "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
                "WHERE d.dag_id = {} "
                "ORDER BY r.execution_date DESC LIMIT {}",
                dag_id.str(), limit),
            res));

        std::vector<RunHistoryEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(to_run_history_entry(row));
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::get_run_history(const DAGRunId &run_id)
    -> task<Result<RunHistoryEntry>> {
  co_return co_await mysql_try([&]() -> task<Result<RunHistoryEntry>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT r.dag_run_id, d.dag_id, r.dag_rowid, r.run_rowid, "
            "r.dag_version, "
            "r.state, r.trigger_type, r.scheduled_at, r.started_at, "
            "r.finished_at, "
            "r.execution_date "
            "FROM dag_runs r JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "WHERE r.dag_run_id = {} LIMIT 1",
            run_id.str()),
        res));

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    auto entry = to_run_history_entry(res.rows().at(0));
    conn_res->return_without_reset();
    co_return ok(std::move(entry));
  });
}

auto MySQLDatabase::save_xcom(const DAGRunId &run_id, const TaskId &task_id,
                              std::string key, std::string value_json)
    -> task<Result<void>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<void>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results key_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT r.run_rowid, t.task_rowid "
                "FROM dag_runs r "
                "JOIN dags d ON d.dag_rowid = r.dag_rowid "
                "JOIN dag_tasks t ON t.dag_rowid = d.dag_rowid "
                "WHERE r.dag_run_id = {} AND t.task_id = {}",
                run_id.str(), task_id.str()),
            key_res));
        if (key_res.rows().empty()) {
          co_return fail(Error::NotFound);
        }

        const auto run_rowid = as_i64(key_res.rows().at(0).at(0));
        const auto task_rowid = as_i64(key_res.rows().at(0).at(1));

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "INSERT INTO xcom_values(run_rowid, task_rowid, `key`, value, "
                "value_type, byte_size, "
                "created_at, expires_at) VALUES({}, {}, {}, {}, "
                "'json', {}, {}, 0) "
                "ON DUPLICATE KEY UPDATE value=VALUES(value), "
                "byte_size=VALUES(byte_size), "
                "created_at=VALUES(created_at)",
                run_rowid, task_rowid, key, value_json,
                static_cast<int>(value_json.size()),
                to_millis(std::chrono::system_clock::now())),
            res));

        conn_res->return_without_reset();
        co_return ok();
      },
      "save_xcom",
      std::format("dag_run_id={} task_id={} key={} payload_bytes={}", run_id,
                  task_id, key, value_json.size()));
}

auto MySQLDatabase::get_xcom(const DAGRunId &run_id, const TaskId &task_id,
                             std::string_view key) -> task<Result<XComEntry>> {
  co_return co_await mysql_try([&]() -> task<Result<XComEntry>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT x.`key`, CAST(x.value AS CHAR), x.byte_size, x.created_at "
            "FROM xcom_values x "
            "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {} AND x.`key` = {}",
            run_id.str(), task_id.str(), std::string(key)),
        res));

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    const auto &row = res.rows().at(0);
    XComEntry entry;
    entry.key = as_str(row.at(0));
    entry.value = as_str(row.at(1));
    entry.byte_size = static_cast<std::size_t>(as_i64(row.at(2)));
    entry.created_at = from_millis(as_i64(row.at(3)));

    conn_res->return_without_reset();
    co_return ok(std::move(entry));
  });
}

auto MySQLDatabase::get_task_xcoms(const DAGRunId &run_id,
                                   const TaskId &task_id)
    -> task<Result<std::vector<XComEntry>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<XComEntry>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT x.`key`, CAST(x.value AS CHAR), x.byte_size, x.created_at "
            "FROM xcom_values x "
            "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {}",
            run_id.str(), task_id.str()),
        res));

    std::vector<XComEntry> out;
    out.reserve(res.rows().size());

    for (auto row : res.rows()) {
      out.emplace_back(XComEntry{
          .key = as_str(row.at(0)),
          .value = as_str(row.at(1)),
          .byte_size = static_cast<std::size_t>(as_i64(row.at(2))),
          .created_at = from_millis(as_i64(row.at(3))),
      });
    }

    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::get_run_xcoms(const DAGRunId &run_id)
    -> task<Result<std::vector<XComTaskEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<XComTaskEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res) {
          co_return fail(conn_res.error());
        }

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT t.task_id, x.`key`, CAST(x.value AS CHAR), "
                "x.byte_size, x.created_at "
                "FROM xcom_values x "
                "JOIN dag_runs r ON r.run_rowid = x.run_rowid "
                "JOIN dag_tasks t ON t.task_rowid = x.task_rowid "
                "WHERE r.dag_run_id = {}",
                run_id.str()),
            res));

        std::vector<XComTaskEntry> out;
        out.reserve(res.rows().size());

        for (auto row : res.rows()) {
          out.emplace_back(XComTaskEntry{
              .task_id = TaskId{as_str(row.at(0))},
              .key = as_str(row.at(1)),
              .value = as_str(row.at(2)),
              .byte_size = static_cast<std::size_t>(as_i64(row.at(3))),
              .created_at = from_millis(as_i64(row.at(4))),
          });
        }

        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

auto MySQLDatabase::delete_run_xcoms(const DAGRunId &run_id)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results run_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT run_rowid FROM dag_runs WHERE dag_run_id = {}",
            run_id.str()),
        run_res));
    if (run_res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    boost::mysql::results del_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "DELETE FROM xcom_values WHERE run_rowid = {}",
            as_i64(run_res.rows().at(0).at(0))),
        del_res));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_last_execution_date(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await mysql_try([&]() -> task<Result<TimePoint>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT MAX(r.execution_date) FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid WHERE d.dag_id = {}",
            dag_id.str()),
        res));

    if (res.rows().empty() || res.rows().at(0).at(0).is_null()) {
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(from_millis(as_i64(res.rows().at(0).at(0))));
  });
}

auto MySQLDatabase::run_exists(const DAGId &dag_id, TimePoint execution_time)
    -> task<Result<bool>> {
  return has_dag_run(dag_id, execution_time);
}

auto MySQLDatabase::has_dag_run(const DAGId &dag_id, TimePoint execution_date)
    -> task<Result<bool>> {
  co_return co_await mysql_try([&]() -> task<Result<bool>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT EXISTS("
            "SELECT 1 FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "WHERE d.dag_id = {} AND r.execution_date = {} LIMIT 1"
            ")",
            dag_id.str(), to_millis(execution_date)),
        res));

    auto exists = !res.rows().empty() && as_bool(res.rows().at(0).at(0));
    conn_res->return_without_reset();
    co_return ok(exists);
  });
}

auto MySQLDatabase::list_dag_run_execution_dates(const DAGId &dag_id,
                                                 TimePoint start, TimePoint end)
    -> task<Result<std::vector<TimePoint>>> {
  co_return co_await mysql_try([&]() -> task<Result<std::vector<TimePoint>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT r.execution_date "
            "FROM dag_runs r "
            "JOIN dags d ON d.dag_rowid = r.dag_rowid "
            "WHERE d.dag_id = {} AND r.execution_date BETWEEN {} AND {} "
            "ORDER BY r.execution_date",
            dag_id.str(), to_millis(start), to_millis(end)),
        res));

    std::vector<TimePoint> execution_dates;
    execution_dates.reserve(res.rows().size());
    for (auto row : res.rows()) {
      execution_dates.push_back(from_millis(as_i64(row.at(0))));
    }

    conn_res->return_without_reset();
    co_return ok(std::move(execution_dates));
  });
}

auto MySQLDatabase::save_watermark(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params("INSERT INTO dag_watermarks(dag_rowid, "
                                  "last_scheduled_at, last_success_at, "
                                  "last_failure_at) VALUES({}, {}, 0, 0) "
                                  "ON DUPLICATE KEY UPDATE "
                                  "last_scheduled_at=VALUES(last_scheduled_at)",
                                  *dag_rowid_res, to_millis(ts)),
        res));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_watermark(const DAGId &dag_id)
    -> task<Result<TimePoint>> {
  co_return co_await mysql_try([&]() -> task<Result<TimePoint>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT last_scheduled_at FROM dag_watermarks WHERE dag_rowid = {}",
            *dag_rowid_res),
        res));

    if (res.rows().empty()) {
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(from_millis(as_i64(res.rows().at(0).at(0))));
  });
}

auto MySQLDatabase::update_watermark_success(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "INSERT INTO dag_watermarks(dag_rowid, last_scheduled_at, "
            "last_success_at, "
            "last_failure_at) VALUES({}, 0, {}, 0) "
            "ON DUPLICATE KEY UPDATE last_success_at=VALUES(last_success_at)",
            *dag_rowid_res, to_millis(ts)),
        res));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::update_watermark_failure(const DAGId &dag_id, TimePoint ts)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }
    auto dag_rowid_res = co_await get_dag_rowid(conn_res->get(), dag_id);
    if (!dag_rowid_res) {
      co_return fail(dag_rowid_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "INSERT INTO dag_watermarks(dag_rowid, last_scheduled_at, "
            "last_success_at, "
            "last_failure_at) VALUES({}, 0, 0, {}) "
            "ON DUPLICATE KEY UPDATE last_failure_at=VALUES(last_failure_at)",
            *dag_rowid_res, to_millis(ts)),
        res));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::get_previous_task_state(std::int64_t task_rowid,
                                            TimePoint current_execution_date,
                                            const DAGRunId &current_run_id)
    -> task<Result<TaskState>> {
  (void)current_run_id;
  co_return co_await mysql_try([&]() -> task<Result<TaskState>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT state FROM task_instances "
            "WHERE task_rowid = {} AND execution_date < {} "
            "ORDER BY execution_date DESC, attempt DESC LIMIT 1",
            task_rowid, to_millis(current_execution_date)),
        res));

    if (res.rows().empty()) {
      conn_res->return_without_reset();
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok(decode_task_state(res.rows().at(0).at(0)));
  });
}

auto MySQLDatabase::clear_all_dag_data() -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::pipeline_request req;
    for (const auto *sql :
         {"DELETE FROM xcom_values", "DELETE FROM task_instances",
          "DELETE FROM dag_runs", "DELETE FROM task_dependencies",
          "DELETE FROM dag_tasks", "DELETE FROM dag_watermarks",
          "DELETE FROM dags"}) {
      req.add_execute(sql);
    }

    std::vector<boost::mysql::stage_response> stage_responses;
    DAGFORGE_TRY_MYSQL(
        run_pipeline_mysql(conn_res->get(), req, stage_responses));

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::mark_incomplete_runs_failed() -> task<Result<std::size_t>> {
  co_return co_await mysql_try([&]() -> task<Result<std::size_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE task_instances ti "
            "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
            "SET ti.state = {}, "
            "ti.finished_at = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * "
            "1000 AS UNSIGNED), "
            "ti.exit_code = IF(ti.exit_code = 0, 1, ti.exit_code), "
            "ti.error_type = IF(ti.error_type = '', 'orphan_recovery', "
            "ti.error_type), "
            "ti.error_message = IF(ti.error_message = '', "
            "'Marked failed during crash recovery', ti.error_message) "
            "WHERE r.state IN ({}, {}) AND ti.state IN ({})",
            util::enum_to_code(TaskState::Failed),
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running),
            util::enum_to_code(TaskState::Running)),
        res));

    boost::mysql::results run_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE dag_runs SET state = {} WHERE state IN ({}, {})",
            util::enum_to_code(DAGRunState::Failed),
            util::enum_to_code(DAGRunState::Queued),
            util::enum_to_code(DAGRunState::Running)),
        run_res));

    conn_res->return_without_reset();
    co_return ok(static_cast<std::size_t>(run_res.affected_rows()));
  });
}

auto MySQLDatabase::claim_task_instances(std::size_t limit,
                                         std::string_view worker_id)
    -> task<Result<std::vector<ClaimedTaskInstance>>> {
  co_return co_await mysql_try([&]() -> task<Result<
                                         std::vector<ClaimedTaskInstance>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    auto &conn = conn_res->get();
    boost::mysql::results tx_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(conn, "START TRANSACTION", tx_res));

    struct QueuedTaskCandidate {
      int64_t run_rowid{0};
      int64_t task_rowid{0};
      int attempt{0};
      std::int64_t execution_date{0};
    };

    auto fetch_candidates =
        [&](TaskState state) -> task<Result<std::vector<QueuedTaskCandidate>>> {
      boost::mysql::results res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn,
          boost::mysql::with_params(
              "SELECT run_rowid, task_rowid, attempt, execution_date "
              "FROM task_instances "
              "WHERE state = {} "
              "ORDER BY execution_date ASC "
              "LIMIT {} FOR UPDATE SKIP LOCKED",
              util::enum_to_code(state), limit),
          res));
      std::vector<QueuedTaskCandidate> out;
      out.reserve(res.rows().size());
      for (auto row : res.rows()) {
        out.push_back(QueuedTaskCandidate{
            .run_rowid = as_i64(row.at(0)),
            .task_rowid = as_i64(row.at(1)),
            .attempt = static_cast<int>(as_i64(row.at(2))),
            .execution_date = as_i64(row.at(3)),
        });
      }
      co_return ok(std::move(out));
    };

    auto retrying_res = co_await fetch_candidates(TaskState::Retrying);
    if (!retrying_res) {
      boost::mysql::results rollback_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(conn, "ROLLBACK", rollback_res));
      co_return fail(retrying_res.error());
    }
    auto ready_res = co_await fetch_candidates(TaskState::Ready);
    if (!ready_res) {
      boost::mysql::results rollback_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(conn, "ROLLBACK", rollback_res));
      co_return fail(ready_res.error());
    }

    auto retrying = std::move(*retrying_res);
    auto ready = std::move(*ready_res);
    std::vector<QueuedTaskCandidate> selected;
    selected.reserve(limit);

    std::size_t retry_idx = 0;
    std::size_t ready_idx = 0;
    auto take_retrying = [&]() {
      const auto &lhs = retrying[retry_idx];
      const auto &rhs = ready[ready_idx];
      if (lhs.execution_date != rhs.execution_date) {
        return lhs.execution_date < rhs.execution_date;
      }
      if (lhs.run_rowid != rhs.run_rowid) {
        return lhs.run_rowid < rhs.run_rowid;
      }
      if (lhs.task_rowid != rhs.task_rowid) {
        return lhs.task_rowid < rhs.task_rowid;
      }
      return lhs.attempt < rhs.attempt;
    };

    while (selected.size() < limit &&
           (retry_idx < retrying.size() || ready_idx < ready.size())) {
      if (retry_idx >= retrying.size()) {
        selected.push_back(ready[ready_idx++]);
        continue;
      }
      if (ready_idx >= ready.size() || take_retrying()) {
        selected.push_back(retrying[retry_idx++]);
      } else {
        selected.push_back(ready[ready_idx++]);
      }
    }

    if (selected.empty()) {
      boost::mysql::results commit_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(conn, "COMMIT", commit_res));
      conn_res->return_without_reset();
      co_return ok(std::vector<ClaimedTaskInstance>{});
    }

    std::vector<ClaimedTaskInstance> claimed;
    claimed.reserve(selected.size());
    std::vector<std::tuple<int64_t, int64_t, int>> claim_keys;
    claim_keys.reserve(selected.size());
    const auto now_ms = to_millis(std::chrono::system_clock::now());

    for (const auto &row : selected) {
      ClaimedTaskInstance c{
          .run_rowid = row.run_rowid,
          .task_rowid = row.task_rowid,
          .attempt = row.attempt,
      };
      claim_keys.emplace_back(c.run_rowid, c.task_rowid, c.attempt);
      claimed.emplace_back(std::move(c));
    }

    auto format_claim_key = [](const std::tuple<int64_t, int64_t, int> &k,
                               boost::mysql::format_context_base &ctx) {
      boost::mysql::format_sql_to(ctx, "({}, {}, {})", std::get<0>(k),
                                  std::get<1>(k), std::get<2>(k));
    };

    boost::mysql::results upd_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        boost::mysql::with_params(
            "UPDATE task_instances "
            "SET state={}, worker_id={}, last_heartbeat={}, "
            "updated_at={}, started_at=IF(started_at=0, {}, started_at) "
            "WHERE (run_rowid, task_rowid, attempt) IN ({}) "
            "AND state IN ({}, {})",
            util::enum_to_code(TaskState::Running), worker_id, now_ms, now_ms,
            now_ms,
            boost::mysql::sequence(std::cref(claim_keys), format_claim_key),
            util::enum_to_code(TaskState::Retrying),
            util::enum_to_code(TaskState::Ready)),
        upd_res));

    boost::mysql::results claimed_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn,
        boost::mysql::with_params(
            "SELECT r.dag_run_id, ti.run_rowid, ti.task_rowid, ti.attempt "
            "FROM task_instances ti "
            "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
            "WHERE (ti.run_rowid, ti.task_rowid, ti.attempt) IN ({})",
            boost::mysql::sequence(std::cref(claim_keys), format_claim_key)),
        claimed_res));
    claimed.clear();
    claimed.reserve(claimed_res.rows().size());
    for (auto row : claimed_res.rows()) {
      claimed.emplace_back(ClaimedTaskInstance{
          .dag_run_id = DAGRunId{as_str(row.at(0))},
          .run_rowid = as_i64(row.at(1)),
          .task_rowid = as_i64(row.at(2)),
          .attempt = static_cast<int>(as_i64(row.at(3))),
      });
    }

    if (upd_res.affected_rows() != claim_keys.size()) {
      boost::mysql::results verify_res;
      DAGFORGE_TRY_MYSQL(execute_mysql(
          conn,
          boost::mysql::with_params(
              "SELECT r.dag_run_id, ti.run_rowid, ti.task_rowid, ti.attempt "
              "FROM task_instances ti "
              "JOIN dag_runs r ON r.run_rowid = ti.run_rowid "
              "WHERE (ti.run_rowid, ti.task_rowid, ti.attempt) IN ({}) "
              "AND ti.state = {}",
              boost::mysql::sequence(std::cref(claim_keys), format_claim_key),
              util::enum_to_code(TaskState::Running)),
          verify_res));
      claimed.clear();
      claimed.reserve(verify_res.rows().size());
      for (auto row : verify_res.rows()) {
        claimed.emplace_back(ClaimedTaskInstance{
            .dag_run_id = DAGRunId{as_str(row.at(0))},
            .run_rowid = as_i64(row.at(1)),
            .task_rowid = as_i64(row.at(2)),
            .attempt = static_cast<int>(as_i64(row.at(3))),
        });
      }
    }

    boost::mysql::results commit_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(conn, "COMMIT", commit_res));
    conn_res->return_without_reset();
    co_return ok(std::move(claimed));
  });
}

auto MySQLDatabase::touch_task_heartbeat(const TaskInstanceKey &key)
    -> task<Result<void>> {
  co_return co_await mysql_try([&]() -> task<Result<void>> {
    if (!key.valid()) {
      co_return fail(Error::InvalidArgument);
    }
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    const auto now_ms = to_millis(std::chrono::system_clock::now());

    boost::mysql::results upd_res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE task_instances "
            "SET last_heartbeat = GREATEST({}, last_heartbeat + 1), "
            "updated_at = GREATEST({}, updated_at + 1) "
            "WHERE run_rowid={} AND task_rowid={} AND attempt={} "
            "AND state={}",
            now_ms, now_ms, key.run_rowid, key.task_rowid, key.attempt,
            util::enum_to_code(TaskState::Running)),
        upd_res));

    if (upd_res.affected_rows() == 0) {
      conn_res->return_without_reset();
      co_return fail(Error::NotFound);
    }

    conn_res->return_without_reset();
    co_return ok();
  });
}

auto MySQLDatabase::reap_zombie_task_instances(
    std::int64_t heartbeat_timeout_ms) -> task<Result<std::size_t>> {
  co_return co_await mysql_try([&]() -> task<Result<std::size_t>> {
    auto conn_res = co_await get_connection();
    if (!conn_res) {
      co_return fail(conn_res.error());
    }

    const auto now_ms = to_millis(std::chrono::system_clock::now());
    const auto cutoff_ms = now_ms - heartbeat_timeout_ms;

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "UPDATE task_instances "
            "SET state={}, finished_at={}, updated_at={}, "
            "exit_code=IF(exit_code=0,1,exit_code), "
            "error_type=IF(error_type='', 'zombie_reaper', error_type), "
            "error_message=IF(error_message='', "
            "'Zombie task reaped due to heartbeat timeout', error_message) "
            "WHERE state={} "
            "AND ((last_heartbeat > 0 AND last_heartbeat < {}) "
            "OR (last_heartbeat = 0 AND started_at > 0 AND started_at < {}))",
            util::enum_to_code(TaskState::Failed), now_ms, now_ms,
            util::enum_to_code(TaskState::Running), cutoff_ms, cutoff_ms),
        res));

    conn_res->return_without_reset();
    co_return ok(static_cast<std::size_t>(res.affected_rows()));
  });
}

auto MySQLDatabase::append_task_log(const DAGRunId &run_id,
                                    const TaskId &task_id, int attempt,
                                    std::string stream, std::string content)
    -> task<Result<void>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<void>> {
        auto conn_res = co_await get_connection();
        if (!conn_res)
          co_return fail(conn_res.error());

        boost::mysql::results run_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params("SELECT r.run_rowid, t.task_rowid "
                                      "FROM dag_runs r "
                                      "JOIN dag_tasks t ON t.dag_rowid = "
                                      "r.dag_rowid AND t.task_id = {} "
                                      "WHERE r.dag_run_id = {} LIMIT 1",
                                      task_id.str(), run_id.str()),
            run_res));

        if (run_res.rows().empty())
          co_return fail(Error::NotFound);

        const auto run_rowid = as_i64(run_res.rows().at(0).at(0));
        const auto task_rowid = as_i64(run_res.rows().at(0).at(1));
        const auto now_ms = to_millis(std::chrono::system_clock::now());

        boost::mysql::results ins_res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "INSERT INTO task_logs "
                "(run_rowid, task_rowid, attempt, stream, logged_at, content) "
                "VALUES ({}, {}, {}, {}, {}, {})",
                run_rowid, task_rowid, attempt, stream, now_ms, content),
            ins_res));

        conn_res->return_without_reset();
        co_return ok();
      },
      "append_task_log",
      std::format("dag_run_id={} task_id={} attempt={} stream={} bytes={}",
                  run_id, task_id, attempt, stream, content.size()));
}

auto MySQLDatabase::get_task_logs(const DAGRunId &run_id, const TaskId &task_id,
                                  int attempt, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await mysql_try([&]()
                                   -> task<
                                       Result<std::vector<orm::TaskLogEntry>>> {
    auto conn_res = co_await get_connection();
    if (!conn_res)
      co_return fail(conn_res.error());

    boost::mysql::results res;
    DAGFORGE_TRY_MYSQL(execute_mysql(
        conn_res->get(),
        boost::mysql::with_params(
            "SELECT tl.log_rowid, tl.attempt, tl.stream, tl.logged_at, "
            "tl.content "
            "FROM task_logs tl "
            "JOIN dag_runs r ON r.run_rowid = tl.run_rowid "
            "JOIN dag_tasks t ON t.task_rowid = tl.task_rowid "
            "WHERE r.dag_run_id = {} AND t.task_id = {} AND tl.attempt = {} "
            "ORDER BY tl.logged_at ASC, tl.log_rowid ASC "
            "LIMIT {}",
            run_id.str(), task_id.str(), attempt,
            static_cast<std::int64_t>(limit)),
        res));

    std::vector<orm::TaskLogEntry> out;
    out.reserve(res.rows().size());
    for (auto row : res.rows()) {
      out.emplace_back(orm::TaskLogEntry{
          .log_rowid = as_i64(row.at(0)),
          .dag_run_id = run_id.clone(),
          .task_id = task_id.clone(),
          .attempt = static_cast<int>(as_i64(row.at(1))),
          .stream = as_str(row.at(2)),
          .logged_at = from_millis(as_i64(row.at(3))),
          .content = as_str(row.at(4)),
      });
    }
    conn_res->return_without_reset();
    co_return ok(std::move(out));
  });
}

auto MySQLDatabase::get_run_logs(const DAGRunId &run_id, std::size_t limit)
    -> task<Result<std::vector<orm::TaskLogEntry>>> {
  co_return co_await mysql_try(
      [&]() -> task<Result<std::vector<orm::TaskLogEntry>>> {
        auto conn_res = co_await get_connection();
        if (!conn_res)
          co_return fail(conn_res.error());

        boost::mysql::results res;
        DAGFORGE_TRY_MYSQL(execute_mysql(
            conn_res->get(),
            boost::mysql::with_params(
                "SELECT tl.log_rowid, t.task_id, tl.attempt, tl.stream, "
                "tl.logged_at, tl.content "
                "FROM task_logs tl "
                "JOIN dag_runs r ON r.run_rowid = tl.run_rowid "
                "JOIN dag_tasks t ON t.task_rowid = tl.task_rowid "
                "WHERE r.dag_run_id = {} "
                "ORDER BY tl.logged_at ASC, tl.log_rowid ASC "
                "LIMIT {}",
                run_id.str(), static_cast<std::int64_t>(limit)),
            res));

        std::vector<orm::TaskLogEntry> out;
        out.reserve(res.rows().size());
        for (auto row : res.rows()) {
          out.emplace_back(orm::TaskLogEntry{
              .log_rowid = as_i64(row.at(0)),
              .dag_run_id = run_id.clone(),
              .task_id = TaskId{as_str(row.at(1))},
              .attempt = static_cast<int>(as_i64(row.at(2))),
              .stream = as_str(row.at(3)),
              .logged_at = from_millis(as_i64(row.at(4))),
              .content = as_str(row.at(5)),
          });
        }
        conn_res->return_without_reset();
        co_return ok(std::move(out));
      });
}

} // namespace dagforge::storage
