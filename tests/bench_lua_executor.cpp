// bench_lua_executor.cpp
// Bench the Lua executor hot path with and without XCom/JSON context.

#include "bench_utils.hpp"

#include "dagforge/executor/executor.hpp"

#include "benchmark_compat.hpp"

#include <atomic>
#include <chrono>
#include <latch>
#include <memory>
#include <string>
#include <string_view>

namespace dagforge {
namespace {

using LuaRuntimeContext = ExecutorRequest::LuaRuntimeContext;

class LuaExecutorBench {
public:
  LuaExecutorBench()
      : runtime_(1), executor_(create_lua_executor(runtime_.runtime)) {
    if (executor_ == nullptr) {
      throw std::runtime_error("failed to create lua executor");
    }
  }

  [[nodiscard]] auto run(ExecutorRequest req) -> ExecutorResult {
    std::latch done{1};
    ExecutorResult result;

    ExecutionSink sink;
    sink.on_complete = [&](const InstanceId &, ExecutorResult r) {
      result = std::move(r);
      done.count_down();
    };

    const auto start_res = executor_->start(std::move(req), std::move(sink));
    if (!start_res) {
      throw std::runtime_error(start_res.error().message());
    }

    done.wait();
    return result;
  }

private:
  bench::RuntimeGuard runtime_;
  std::unique_ptr<IExecutor> executor_;
};

[[nodiscard]] auto make_inline_request() -> ExecutorRequest {
  ExecutorRequest req;
  req.instance_id = InstanceId{"bench_lua_inline"};
  req.execution_timeout = std::chrono::seconds(5);
  req.config = LuaExecutorConfig{.script = "return 1"};
  return req;
}

[[nodiscard]] auto make_xcom_request() -> ExecutorRequest {
  ExecutorRequest req;
  req.instance_id = InstanceId{"bench_lua_xcom"};
  req.execution_timeout = std::chrono::seconds(5);
  req.config = LuaExecutorConfig{
      .script = R"(
        local payload = dagforge.xcom_pull("upstream", "payload")
        if not payload then
          error("missing xcom payload")
        end
        payload.tax = payload.subtotal * payload.tax_rate
        payload.total = payload.subtotal + payload.tax
        dagforge.xcom_push("result", payload)
        return dagforge.json_encode(payload)
      )",
  };

  auto ctx = std::make_shared<LuaRuntimeContext>();
  ctx->dag_id = DAGId{"bench_lua_dag"};
  ctx->dag_run_id = DAGRunId{"bench_lua_run"};
  ctx->task_id = TaskId{"bench_lua_task"};
  ctx->execution_date = "2026-04-05T00:00:00Z";
  ctx->xcom_values["upstream"] =
      std::make_shared<LuaRuntimeContext::TaskXComMap>(
          LuaRuntimeContext::TaskXComMap{{"payload",
                                          R"({"subtotal":128.5,"tax_rate":0.13})"}});
  req.lua_context = std::move(ctx);
  return req;
}

void BM_LuaExecutorInlineReturn(benchmark::State &state) {
  LuaExecutorBench bench;
  const auto req = make_inline_request();

  for (auto _ : state) {
    auto result = bench.run(req);
    benchmark::DoNotOptimize(result.exit_code);
    benchmark::DoNotOptimize(result.stdout_output);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BM_LuaExecutorXComRoundtrip(benchmark::State &state) {
  LuaExecutorBench bench;
  const auto req = make_xcom_request();

  for (auto _ : state) {
    auto result = bench.run(req);
    benchmark::DoNotOptimize(result.exit_code);
    benchmark::DoNotOptimize(result.stdout_output);
    benchmark::DoNotOptimize(result.xcom_outputs);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

BENCHMARK(BM_LuaExecutorInlineReturn)->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_LuaExecutorXComRoundtrip)->Unit(benchmark::kMicrosecond);

} // namespace
} // namespace dagforge
