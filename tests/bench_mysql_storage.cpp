// bench_mysql_storage.cpp — Benchmark MySQL storage operations.
//
// Measures:
//   - Schema DDL: sequential vs pipeline execution
//   - Task dependency insertion: loop vs batch VALUES
//   - Task instance batch upsert throughput
//   - Heartbeat query: prepared statement vs with_params
//
// Uses a single ConnContext per benchmark function (not thread_local) to
// avoid shared-connection issues when google-benchmark runs multiple threads.
// PauseTiming/ResumeTiming are paired correctly to exclude setup/teardown.

#include "bench_utils.hpp"

#include "dagforge/config/system_config.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/scheduler/task.hpp"
#include "dagforge/storage/mysql_schema.hpp"
#include "dagforge/util/enum.hpp"
#include "dagforge/util/enum_mysql_formatter.hpp"

#include "benchmark_compat.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/mysql/any_connection.hpp>
#include <boost/mysql/connect_params.hpp>
#include <boost/mysql/pipeline.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/sequence.hpp>
#include <boost/mysql/statement.hpp>
#include <boost/mysql/with_params.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge {
namespace {

using boost::asio::use_awaitable;

[[nodiscard]] auto load_db_config() -> DatabaseConfig {
  DatabaseConfig cfg;
  cfg.host = bench::env_or_default("DAGFORGE_BENCH_MYSQL_HOST", "127.0.0.1");
  cfg.username = bench::env_or_default("DAGFORGE_BENCH_MYSQL_USER", "dagforge");
  cfg.password =
      bench::env_or_default("DAGFORGE_BENCH_MYSQL_PASSWORD", "dagforge");
  cfg.database = bench::env_or_default("DAGFORGE_BENCH_MYSQL_DB", "dagforge");
  cfg.pool_size = 8;
  return cfg;
}

[[nodiscard]] auto benchmark_run_nonce() -> const std::string & {
  static const std::string nonce = [] {
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    return std::to_string(now_ns);
  }();
  return nonce;
}

[[nodiscard]] auto split_sql_statements(std::string_view schema_sql)
    -> std::vector<std::string> {
  std::vector<std::string> out;
  std::stringstream ss{std::string(schema_sql)};
  std::string stmt;
  while (std::getline(ss, stmt, ';')) {
    auto first = stmt.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
      continue;
    }
    auto last = stmt.find_last_not_of(" \t\r\n");
    out.push_back(stmt.substr(first, last - first + 1));
  }
  return out;
}

/// Per-benchmark connection context.  Each benchmark function creates its own
/// instance so there is no sharing across google-benchmark threads.
struct ConnContext {
  io::IoContext io;
  std::optional<boost::mysql::any_connection> conn;
  DatabaseConfig cfg{load_db_config()};

  auto ensure_open(benchmark::State &state) -> bool {
    if (conn.has_value()) {
      return true;
    }
    try {
      conn.emplace(io.get_executor());
      boost::mysql::connect_params params;
      params.server_address.emplace_host_and_port(cfg.host, cfg.port);
      params.username = cfg.username;
      params.password = cfg.password;
      params.database = cfg.database;
      params.ssl = boost::mysql::ssl_mode::disable;
      params.multi_queries = false;

      bench::run_on_io(io, [&]() -> task<void> {
        co_await conn->async_connect(params, use_awaitable);
      }());
      return true;
    } catch (const std::exception &e) {
      state.SkipWithError(e.what());
      conn.reset();
      return false;
    }
  }

  ~ConnContext() {
    if (!conn) {
      return;
    }
    try {
      bench::run_on_io(io, [&]() -> task<void> {
        co_await conn->async_close(use_awaitable);
      }());
    } catch (...) {
    }
  }
};

void BM_MySQL_EnsureSchema_Sequential(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const auto stmts = split_sql_statements(schema::V1_SCHEMA);
  for (auto _ : state) {
    bench::run_on_io(ctx.io, [&]() -> task<void> {
      for (const auto &sql : stmts) {
        boost::mysql::results res;
        co_await ctx.conn->async_execute(sql, res, use_awaitable);
      }
      boost::mysql::results version_res;
      co_await ctx.conn->async_execute(
          "INSERT IGNORE INTO schema_version(version) VALUES (1)", version_res,
          use_awaitable);
    }());
  }
  state.SetItemsProcessed(static_cast<int64_t>(stmts.size()) *
                          state.iterations());
}

void BM_MySQL_EnsureSchema_Pipeline(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const auto stmts = split_sql_statements(schema::V1_SCHEMA);
  for (auto _ : state) {
    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::pipeline_request req;
      for (const auto &sql : stmts) {
        req.add_execute(sql);
      }
      req.add_execute("INSERT IGNORE INTO schema_version(version) VALUES (1)");
      std::vector<boost::mysql::stage_response> responses;
      co_await ctx.conn->async_run_pipeline(req, responses, use_awaitable);
    }());
  }
  state.SetItemsProcessed(static_cast<int64_t>(stmts.size()) *
                          state.iterations());
}

[[nodiscard]] auto setup_dependency_case(ConnContext &ctx, int dep_count,
                                         int iter_seed)
    -> std::tuple<int64_t, int64_t, std::vector<int64_t>> {
  const std::string dag_id = "bench_dep_dag_" + benchmark_run_nonce() + "_" +
                             std::to_string(iter_seed);
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

  return bench::run_on_io(
      ctx.io,
      [&]() -> task<std::tuple<int64_t, int64_t, std::vector<int64_t>>> {
        boost::mysql::results res;
        co_await ctx.conn->async_execute(
            boost::mysql::with_params(
                "INSERT INTO dags(dag_id, name, created_at, updated_at) "
                "VALUES({}, {}, {}, {})",
                dag_id, dag_id, now, now),
            res, use_awaitable);
        const auto dag_rowid = static_cast<int64_t>(res.last_insert_id());

        co_await ctx.conn->async_execute(
            boost::mysql::with_params("INSERT INTO dag_tasks(dag_rowid, "
                                      "task_id, command) VALUES({}, {}, {})",
                                      dag_rowid, "target", "echo target"),
            res, use_awaitable);
        const auto task_rowid = static_cast<int64_t>(res.last_insert_id());

        std::vector<int64_t> dep_rowids;
        dep_rowids.reserve(static_cast<std::size_t>(dep_count));
        for (int i = 0; i < dep_count; ++i) {
          const auto dep_id = std::string("dep_") + std::to_string(i);
          co_await ctx.conn->async_execute(
              boost::mysql::with_params("INSERT INTO dag_tasks(dag_rowid, "
                                        "task_id, command) VALUES({}, {}, {})",
                                        dag_rowid, dep_id, "echo dep"),
              res, use_awaitable);
          dep_rowids.push_back(static_cast<int64_t>(res.last_insert_id()));
        }

        co_return std::make_tuple(dag_rowid, task_rowid, dep_rowids);
      }());
}

void BM_MySQL_TaskDependency_Insert_Batch(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const int dep_count = static_cast<int>(state.range(0));
  int iter_seed = 0;
  for (auto _ : state) {
    state.PauseTiming();
    auto [dag_rowid, task_rowid, dep_rowids] =
        setup_dependency_case(ctx, dep_count, ++iter_seed);

    auto format_dep = [dag_rowid,
                       task_rowid](const int64_t &dep_rowid,
                                   boost::mysql::format_context_base &fmt_ctx) {
      boost::mysql::format_sql_to(fmt_ctx, "({}, {}, {}, 'success')", dag_rowid,
                                  task_rowid, dep_rowid);
    };
    state.ResumeTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params("DELETE FROM task_dependencies WHERE "
                                    "dag_rowid = {} AND task_rowid = {}",
                                    dag_rowid, task_rowid),
          res, use_awaitable);
      co_await ctx.conn->async_execute(
          boost::mysql::with_params(
              "INSERT INTO task_dependencies(dag_rowid, task_rowid, "
              "depends_on_task_rowid, dependency_type) "
              "VALUES {}",
              boost::mysql::sequence(std::cref(dep_rowids), format_dep)),
          res, use_awaitable);
    }());
    state.PauseTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params("DELETE FROM dags WHERE dag_rowid = {}",
                                    dag_rowid),
          res, use_awaitable);
    }());
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(dep_count) * state.iterations());
}

void BM_MySQL_TaskDependency_BatchInsert(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const int dep_count = static_cast<int>(state.range(0));
  int iter_seed = 100000;
  for (auto _ : state) {
    state.PauseTiming();
    auto [dag_rowid, task_rowid, dep_rowids] =
        setup_dependency_case(ctx, dep_count, ++iter_seed);

    auto format_dep = [dag_rowid,
                       task_rowid](const int64_t &dep_rowid,
                                   boost::mysql::format_context_base &fmt_ctx) {
      boost::mysql::format_sql_to(fmt_ctx, "({}, {}, {}, 'success')", dag_rowid,
                                  task_rowid, dep_rowid);
    };
    state.ResumeTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params("DELETE FROM task_dependencies WHERE "
                                    "dag_rowid = {} AND task_rowid = {}",
                                    dag_rowid, task_rowid),
          res, use_awaitable);
      co_await ctx.conn->async_execute(
          boost::mysql::with_params(
              "INSERT INTO task_dependencies(dag_rowid, task_rowid, "
              "depends_on_task_rowid, dependency_type) "
              "VALUES {}",
              boost::mysql::sequence(std::cref(dep_rowids), format_dep)),
          res, use_awaitable);
    }());
    state.PauseTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params("DELETE FROM dags WHERE dag_rowid = {}",
                                    dag_rowid),
          res, use_awaitable);
    }());
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(dep_count) * state.iterations());
}

void BM_MySQL_SaveTaskInstancesBatch(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const int batch = static_cast<int>(state.range(0));
  int iter_seed = 500000;
  for (auto _ : state) {
    state.PauseTiming();
    auto [dag_rowid, task_rowid, dep_rowids] =
        setup_dependency_case(ctx, std::max(0, batch - 1), ++iter_seed);

    const auto run_id = std::string("bench_batch_run_") +
                        benchmark_run_nonce() + "_" + std::to_string(iter_seed);
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();

    auto run_rowid = bench::run_on_io(ctx.io, [&]() -> task<int64_t> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params(
              "INSERT INTO dag_runs(dag_run_id, dag_rowid, execution_date) "
              "VALUES({}, {}, {})",
              run_id, dag_rowid, now_ms),
          res, use_awaitable);
      co_return static_cast<int64_t>(res.last_insert_id());
    }());

    std::vector<int64_t> task_rowids;
    task_rowids.reserve(static_cast<std::size_t>(batch));
    task_rowids.push_back(task_rowid);
    for (auto dep_rowid : dep_rowids) {
      task_rowids.push_back(dep_rowid);
    }

    auto format_ti = [run_rowid,
                      now_ms](const int64_t &tr,
                              boost::mysql::format_context_base &ctx_fmt) {
      const auto running_state_code =
          dagforge::util::enum_to_code(TaskState::Running);
      boost::mysql::format_sql_to(ctx_fmt, "({}, {}, 1, {}, {}, 0, 0, '', '')",
                                  run_rowid, tr, running_state_code, now_ms);
    };
    state.ResumeTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params(
              "INSERT INTO task_instances(run_rowid, task_rowid, attempt, "
              "state, started_at, "
              "finished_at, exit_code, error_message, error_type) VALUES {} "
              "ON DUPLICATE KEY UPDATE state=VALUES(state), "
              "started_at=VALUES(started_at)",
              boost::mysql::sequence(std::cref(task_rowids), format_ti)),
          res, use_awaitable);
    }());
    state.PauseTiming();

    bench::run_on_io(ctx.io, [&]() -> task<void> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params("DELETE FROM dags WHERE dag_rowid = {}",
                                    dag_rowid),
          res, use_awaitable);
    }());
    state.ResumeTiming();
  }

  state.SetItemsProcessed(static_cast<int64_t>(batch) * state.iterations());
}

void BM_MySQL_Heartbeat_Prepared(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  constexpr auto dag_id = "bench_hb_dag";
  const auto exec_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

  // One-time fixture: insert a dag + run so the SELECT has a real row.
  bench::run_on_io(ctx.io, [&]() -> task<void> {
    boost::mysql::results res;
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    co_await ctx.conn->async_execute(
        boost::mysql::with_params(
            "INSERT IGNORE INTO dags(dag_id, name, created_at, updated_at) "
            "VALUES({}, {}, {}, {})",
            dag_id, dag_id, now_ms, now_ms),
        res, use_awaitable);
    const auto dag_rowid = static_cast<int64_t>(
        res.last_insert_id() > 0 ? res.last_insert_id() : 1);
    co_await ctx.conn->async_execute(
        boost::mysql::with_params(
            "INSERT IGNORE INTO dag_runs(dag_run_id, dag_rowid, "
            "execution_date) VALUES({}, {}, {})",
            "bench_hb_run", dag_rowid, exec_ms),
        res, use_awaitable);
  }());

  auto stmt = bench::run_on_io(ctx.io, [&]() -> task<boost::mysql::statement> {
    auto prepared = co_await ctx.conn->async_prepare_statement(
        "SELECT EXISTS(SELECT 1 FROM dag_runs r JOIN dags d ON "
        "d.dag_rowid = r.dag_rowid "
        "WHERE d.dag_id = ? AND r.execution_date = ? LIMIT 1)",
        use_awaitable);
    co_return prepared;
  }());

  for (auto _ : state) {
    auto r = bench::run_on_io(ctx.io, [&]() -> task<bool> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(stmt.bind(dag_id, exec_ms), res,
                                       use_awaitable);
      co_return !res.rows().empty() && res.rows().at(0).at(0).as_int64() != 0;
    }());
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

void BM_MySQL_Heartbeat_StringStream(benchmark::State &state) {
  ConnContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const std::string dag_id = "bench_hb_dag";
  const auto exec_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

  for (auto _ : state) {
    auto exists = bench::run_on_io(ctx.io, [&]() -> task<bool> {
      boost::mysql::results res;
      co_await ctx.conn->async_execute(
          boost::mysql::with_params(
              "SELECT EXISTS(SELECT 1 FROM dag_runs r JOIN dags d ON "
              "d.dag_rowid = r.dag_rowid "
              "WHERE d.dag_id = {} AND r.execution_date = {} LIMIT 1)",
              dag_id, exec_ms),
          res, use_awaitable);
      co_return !res.rows().empty() && res.rows().at(0).at(0).as_int64() != 0;
    }());
    benchmark::DoNotOptimize(exists);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_MySQL_EnsureSchema_Sequential)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(20);
BENCHMARK(BM_MySQL_EnsureSchema_Pipeline)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(20);

BENCHMARK(BM_MySQL_TaskDependency_Insert_Batch)
    ->RangeMultiplier(4)
    ->Range(8, 1024)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(50);

BENCHMARK(BM_MySQL_TaskDependency_BatchInsert)
    ->RangeMultiplier(4)
    ->Range(8, 1024)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(100);

BENCHMARK(BM_MySQL_SaveTaskInstancesBatch)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(100);

BENCHMARK(BM_MySQL_Heartbeat_Prepared)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(200);
BENCHMARK(BM_MySQL_Heartbeat_StringStream)
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(200);

} // namespace
} // namespace dagforge
