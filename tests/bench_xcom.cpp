// bench_xcom.cpp — Benchmark XCom template resolution, cache, MySQL
// persistence.
//
// Uses bench::run_on_io / bench::env_or_default from bench_utils.hpp.
// MySQL benchmark is single-threaded to avoid schema-creation races.

#include "bench_utils.hpp"

#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/config/system_config.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/xcom/template_resolver.hpp"
#include "dagforge/xcom/xcom_types.hpp"

#include "benchmark_compat.hpp"

#include <chrono>
#include <cstdint>
#include <format>
#include <string>
#include <vector>

namespace dagforge {
namespace {

/// Pre-generated template strings to avoid allocation noise during benchmark.
class PreallocatedTemplates {
public:
  explicit PreallocatedTemplates(std::size_t count) {
    simple_.reserve(count);
    nested_.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      simple_.emplace_back(
          std::format("SELECT * FROM t WHERE ds='{{{{ds}}}}' AND id={}", i));
      nested_.emplace_back(
          std::format("INSERT INTO log(ts, run, seq) VALUES ('{{{{ts}}}}', "
                      "'{{{{dag_run_id}}}}', {})",
                      i));
    }
  }

  [[nodiscard]] auto simple(std::size_t idx) const -> const std::string & {
    return simple_[idx];
  }

  [[nodiscard]] auto nested(std::size_t idx) const -> const std::string & {
    return nested_[idx];
  }

private:
  std::vector<std::string> simple_;
  std::vector<std::string> nested_;
};

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

[[nodiscard]] auto bench_nonce() -> const std::string & {
  static const std::string v = [] {
    const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    return std::to_string(now_ns);
  }();
  return v;
}

class XComMySQLBenchContext {
public:
  XComMySQLBenchContext()
      : runtime_(1), service_(runtime_.runtime, load_db_config()),
        dag_id_(std::format("bench_xcom_dag_{}", bench_nonce())),
        run_id_(std::format("bench_xcom_run_{}", bench_nonce())),
        task_id_("bench_xcom_task") {}

  ~XComMySQLBenchContext() {
    if (!open_) {
      return;
    }
    try {
      auto close_res = service_.sync_wait(service_.close());
      if (!close_res) {
        last_error_ = close_res.error().message();
      }
    } catch (const std::exception &e) {
      last_error_ = e.what();
    } catch (...) {
      last_error_ = "XComMySQLBenchContext close failed";
    }
  }

  auto ensure_open(benchmark::State &state) -> bool {
    if (open_) {
      return true;
    }
    if (open_failed_) {
      state.SkipWithError(last_error_.c_str());
      return false;
    }
    try {
      auto open_res = service_.sync_wait(service_.open());
      if (!open_res) {
        last_error_ = open_res.error().message();
        open_failed_ = true;
        state.SkipWithError(last_error_.c_str());
        return false;
      }

      if (!ensure_fixture_rows()) {
        open_failed_ = true;
        state.SkipWithError(last_error_.c_str());
        return false;
      }
      open_ = true;
      return true;
    } catch (const std::exception &e) {
      open_failed_ = true;
      last_error_ = e.what();
      state.SkipWithError(last_error_.c_str());
      return false;
    }
  }

  auto insert_xcom_blocking(std::string key, const std::string &value_json)
      -> Result<void> {
    if (!open_) {
      return fail(Error::InvalidState);
    }

    auto parsed = parse_json(value_json);
    if (!parsed) {
      last_error_ = parsed.error().message();
      return fail(parsed.error());
    }

    try {
      auto save_res = service_.sync_wait(
          service_.save_xcom(run_id_, task_id_, key, value_json));
      if (!save_res) {
        last_error_ = save_res.error().message();
      }
      return save_res;
    } catch (const std::exception &e) {
      last_error_ = e.what();
      return fail(Error::DatabaseQueryFailed);
    } catch (...) {
      last_error_ = "insert_xcom_blocking unknown exception";
      return fail(Error::DatabaseQueryFailed);
    }
  }

  [[nodiscard]] auto last_error_message() const -> const std::string & {
    return last_error_;
  }

private:
  auto ensure_fixture_rows() -> bool {
    const auto now = std::chrono::system_clock::now();

    auto task_cfg_res = TaskConfig::builder()
                            .id(task_id_.str())
                            .name(task_id_.str())
                            .command("echo xcom")
                            .executor(ExecutorType::Shell)
                            .build();
    if (!task_cfg_res) {
      last_error_ = task_cfg_res.error().message();
      return false;
    }

    DAGInfo dag{};
    dag.dag_id = dag_id_;
    dag.name = dag_id_.str();
    dag.created_at = now;
    dag.updated_at = now;
    dag.tasks.push_back(*task_cfg_res);
    dag.rebuild_task_index();

    auto dag_rowid = service_.sync_wait(service_.save_dag(dag));
    if (!dag_rowid) {
      last_error_ = dag_rowid.error().message();
      return false;
    }

    auto save_task_res =
        service_.sync_wait(service_.save_task(dag_id_, *task_cfg_res));
    if (!save_task_res) {
      last_error_ = save_task_res.error().message();
      return false;
    }

    DAG dag_graph;
    if (!dag_graph.add_node(task_id_.clone())) {
      last_error_ = "failed to add task node";
      return false;
    }

    auto run_res = DAGRun::create(run_id_.clone(),
                                  std::make_shared<DAG>(std::move(dag_graph)));
    if (!run_res) {
      last_error_ = run_res.error().message();
      return false;
    }
    run_res->set_dag_rowid(*dag_rowid);
    run_res->set_dag_version(1);
    run_res->set_execution_date(now);

    auto save_run_res = service_.sync_wait(service_.save_dag_run(*run_res));
    if (!save_run_res) {
      last_error_ = save_run_res.error().message();
      return false;
    }
    return true;
  }

  bench::RuntimeGuard runtime_;
  PersistenceService service_;
  DAGId dag_id_;
  DAGRunId run_id_;
  TaskId task_id_;
  bool open_{false};
  bool open_failed_{false};
  std::string last_error_;
};

void BM_XComTemplateResolve(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  PreallocatedTemplates templates(count);

  TemplateResolver resolver([](const DAGRunId &, const TaskId &,
                               std::string_view) -> Result<XComEntry> {
    return fail(Error::NotFound);
  });

  TemplateContext ctx;
  ctx.dag_run_id = DAGRunId{"bench_run"};
  ctx.dag_id = DAGId{"bench_dag"};
  ctx.task_id = TaskId{"bench_task"};
  ctx.execution_date = std::chrono::system_clock::now();
  ctx.data_interval_start = ctx.execution_date - std::chrono::hours(24);
  ctx.data_interval_end = ctx.execution_date;
  std::vector<XComPullConfig> empty_pulls;

  for (auto _ : state) {
    for (std::size_t i = 0; i < count; ++i) {
      auto r1 =
          resolver.resolve_template(templates.simple(i), ctx, empty_pulls);
      auto r2 =
          resolver.resolve_template(templates.nested(i), ctx, empty_pulls);
      benchmark::DoNotOptimize(r1);
      benchmark::DoNotOptimize(r2);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(count * 2) * state.iterations());
}

void BM_XComCacheHit(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  XComCache cache;
  cache.reserve(count);
  const DAGRunId run_id{"bench_run"};
  const TaskId task_id{"producer"};
  std::vector<std::string> keys;
  keys.reserve(count);

  for (std::size_t i = 0; i < count; ++i) {
    keys.emplace_back(std::format("key_{}", i));
    cache.set(run_id, task_id, keys.back(),
              dump_json(JsonValue{std::format("value_{}", i)}));
  }

  for (auto _ : state) {
    for (std::size_t i = 0; i < count; ++i) {
      auto result = cache.get(run_id, task_id, keys[i]);
      benchmark::DoNotOptimize(result);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

void BM_XCom_AsyncPersistence_MySQL(benchmark::State &state) {
  XComMySQLBenchContext ctx;
  if (!ctx.ensure_open(state)) {
    return;
  }

  const auto payload_kb = static_cast<std::size_t>(state.range(0));
  const std::string payload =
      std::format("{{\"data\":\"{}\"}}", std::string(payload_kb * 1024, 'x'));
  std::uint64_t key_seq = 0;

  for (auto _ : state) {
    const auto key = std::format("bench_key_{}", key_seq++);
    auto save_res = ctx.insert_xcom_blocking(key, payload);
    if (!save_res) {
      const auto detail = ctx.last_error_message();
      state.SkipWithError(detail.empty() ? save_res.error().message().c_str()
                                         : detail.c_str());
      return;
    }
  }

  state.SetBytesProcessed(static_cast<int64_t>(payload.size()) *
                          static_cast<int64_t>(state.iterations()));
}

void BM_XComCacheMiss(benchmark::State &state) {
  const auto count = static_cast<std::size_t>(state.range(0));
  XComCache cache;
  cache.reserve(count);
  const DAGRunId run_id{"bench_run"};
  const TaskId task_id{"producer"};
  std::vector<std::string> existing_keys;
  std::vector<std::string> keys;
  existing_keys.reserve(count);
  keys.reserve(count);

  for (std::size_t i = 0; i < count; ++i) {
    existing_keys.emplace_back(std::format("key_{}", i));
    cache.set(run_id, task_id, existing_keys.back(),
              dump_json(JsonValue{std::format("value_{}", i)}));
    keys.emplace_back(std::format("missing_key_{}", i));
  }

  for (auto _ : state) {
    for (std::size_t i = 0; i < count; ++i) {
      auto result = cache.get(run_id, task_id, keys[i]);
      benchmark::DoNotOptimize(result);
    }
  }
  state.SetItemsProcessed(static_cast<int64_t>(count) * state.iterations());
}

BENCHMARK(BM_XComTemplateResolve)
    ->Arg(100)
    ->Arg(500)
    ->Arg(1000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_XComCacheHit)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_XComCacheMiss)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_XCom_AsyncPersistence_MySQL)
    ->Arg(1)
    ->Arg(4)
    ->Arg(8)
    ->Unit(benchmark::kMillisecond)
    ->Iterations(30);

} // namespace
} // namespace dagforge
