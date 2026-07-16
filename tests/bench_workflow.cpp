#include "dagforge/workflow/executor_registry.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "bench_utils.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <format>
#include <latch>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

class ImmediateExecutor final : public ITaskExecutor {
public:
  [[nodiscard]] auto type() const noexcept -> std::string_view override {
    return "bench";
  }

  [[nodiscard]] auto compile(JsonPayload config, ExecutorCompileContext) const
      -> Result<CompiledExecutorConfig> override {
    return ok(CompiledExecutorConfig::from_encoded(std::move(config)));
  }

  auto start(TaskExecutionRequest request, TaskExecutionSink sink)
      -> Result<void> override {
    if (sink.on_state) {
      sink.on_state(request.instance_id, "running");
    }
    ExecutorOutputs outputs;
    outputs.reserve(request.outputs.size());
    for (const auto &output : request.outputs) {
      outputs.emplace_back(output.clone(), std::string{"ok"});
    }
    if (sink.on_complete) {
      sink.on_complete(request.instance_id,
                       task_succeeded(std::move(outputs)));
    }
    return ok();
  }

  auto cancel(const InstanceId &) -> void override {}

  auto quiesce(std::chrono::milliseconds) -> Result<void> override {
    return ok();
  }
};

auto register_executor(ExecutorRegistry &registry) -> void {
  auto registered =
      registry.register_executor(std::make_shared<ImmediateExecutor>());
  if (!registered) {
    std::terminate();
  }
}

[[nodiscard]] auto make_plan(std::size_t node_count, bool fan_out,
                             bool checkpoint_each_node = false)
    -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{fan_out ? "bench-fan-out" : "bench-linear"};
  plan.nodes.reserve(node_count);
  for (std::size_t index = 0; index < node_count; ++index) {
    plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{std::format("node-{}", index)},
        .executor = "bench",
        .outputs = {WorkflowPortId{"result"}},
        .checkpoint = checkpoint_each_node,
    });
  }
  if (node_count < 2) {
    return plan;
  }
  plan.edges.reserve(node_count - 1);
  for (std::size_t index = 1; index < node_count; ++index) {
    const auto source = fan_out ? 0 : index - 1;
    plan.edges.push_back(ConditionalEdge{
        .source =
            OutputRef{
                .node_id = WorkflowNodeId{std::format("node-{}", source)},
                .port = WorkflowPortId{"result"},
            },
        .target = WorkflowNodeId{std::format("node-{}", index)},
    });
  }
  return plan;
}

[[nodiscard]] auto make_plan_json(std::size_t node_count, bool fan_out)
    -> std::string {
  std::string json =
      std::format(R"({{"workflow_id":"bench-{}","schema_version":1,"nodes":[)",
                  fan_out ? "fan-out" : "linear");
  for (std::size_t index = 0; index < node_count; ++index) {
    if (index != 0) {
      json.push_back(',');
    }
    json += std::format(
        R"({{"id":"node-{}","executor":"bench","outputs":["result"],"config":{{}}}})",
        index);
  }
  json += "],\"edges\":[";
  for (std::size_t index = 1; index < node_count; ++index) {
    if (index != 1) {
      json.push_back(',');
    }
    const auto source = fan_out ? 0 : index - 1;
    json += std::format(
        R"({{"source_node":"node-{}","source_port":"result","target":"node-{}","condition":{{"kind":"always"}}}})",
        source, index);
  }
  json += "]}";
  return json;
}

void BM_WorkflowPlanParseLinear(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  const auto payload = make_plan_json(node_count, false);
  for (auto _ : state) {
    auto parsed = WorkflowPlanLoader::from_json(payload);
    if (!parsed) {
      state.SkipWithError(parsed.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize(parsed->nodes.size());
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void BM_WorkflowPlanCompileLinear(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  ExecutorRegistry registry;
  register_executor(registry);
  PlanCompiler compiler(registry);
  const auto source = make_plan(node_count, false);
  for (auto _ : state) {
    state.PauseTiming();
    auto plan = source;
    state.ResumeTiming();
    auto compiled = compiler.compile(std::move(plan));
    if (!compiled) {
      state.SkipWithError(compiled.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize((*compiled)->digest.data());
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void BM_WorkflowPlanCompileFanOut(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  ExecutorRegistry registry;
  register_executor(registry);
  PlanCompiler compiler(registry);
  const auto source = make_plan(node_count, true);
  for (auto _ : state) {
    state.PauseTiming();
    auto plan = source;
    state.ResumeTiming();
    auto compiled = compiler.compile(std::move(plan));
    if (!compiled) {
      state.SkipWithError(compiled.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize((*compiled)->topological_order.size());
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void BM_WorkflowPlanParseAndCompile(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  ExecutorRegistry registry;
  register_executor(registry);
  PlanCompiler compiler(registry);
  const auto payload = make_plan_json(node_count, true);
  for (auto _ : state) {
    auto parsed = WorkflowPlanLoader::from_json(payload);
    if (!parsed) {
      state.SkipWithError(parsed.error().message().c_str());
      return;
    }
    auto compiled = compiler.compile(std::move(*parsed));
    if (!compiled) {
      state.SkipWithError(compiled.error().message().c_str());
      return;
    }
    benchmark::DoNotOptimize((*compiled)->digest.data());
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void run_workflow_runtime_benchmark(benchmark::State &state, bool fan_out,
                                    bool checkpoint_each_node = false) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  bench::RuntimeGuard guard(4);
  ExecutorRegistry registry;
  register_executor(registry);
  auto compiled = PlanCompiler{registry}.compile(
      make_plan(node_count, fan_out, checkpoint_each_node));
  if (!compiled) {
    state.SkipWithError(compiled.error().message().c_str());
    return;
  }
  WorkflowRuntime runtime(guard.runtime, registry,
                          std::make_shared<InMemoryArtifactStore>(),
                          std::make_shared<EvidenceLedger>(100'000),
                          std::make_shared<CheckpointStore>(), 1);

  for (auto _ : state) {
    std::latch completed{1};
    std::atomic<RunState> final_state{RunState::Running};
    WorkflowCallbacks callbacks;
    callbacks.on_complete = [&completed, &final_state](
                                const WorkflowRunId &,
                                std::shared_ptr<const RunSnapshot> snapshot) {
      final_state.store(snapshot->state, std::memory_order_release);
      completed.count_down();
    };
    auto started =
        runtime.start(*compiled,
                      TriggerEnvelope{
                          .workflow_id = (*compiled)->workflow_id.clone(),
                          .source = "benchmark",
                          .event_type = checkpoint_each_node
                                            ? "checkpoint-each-node"
                                            : (fan_out ? "fan-out" : "linear"),
                      },
                      std::move(callbacks));
    if (!started) {
      state.SkipWithError(started.error().message().c_str());
      return;
    }
    completed.wait();
    if (final_state.load(std::memory_order_acquire) != RunState::Succeeded) {
      state.SkipWithError("workflow did not reach succeeded state");
      return;
    }
    benchmark::DoNotOptimize(started->str().data());
  }

  auto quiesced = runtime.quiesce(std::chrono::seconds(5));
  if (!quiesced) {
    state.SkipWithError(quiesced.error().message().c_str());
    return;
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void BM_WorkflowRuntimeLinear(benchmark::State &state) {
  run_workflow_runtime_benchmark(state, false);
}

void BM_WorkflowRuntimeFanOut(benchmark::State &state) {
  run_workflow_runtime_benchmark(state, true);
}

void BM_WorkflowRuntimeCheckpointEachNode(benchmark::State &state) {
  run_workflow_runtime_benchmark(state, false, true);
}

BENCHMARK(BM_WorkflowPlanParseLinear)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_WorkflowPlanCompileLinear)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_WorkflowPlanCompileFanOut)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_WorkflowPlanParseAndCompile)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_WorkflowRuntimeLinear)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_WorkflowRuntimeFanOut)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

BENCHMARK(BM_WorkflowRuntimeCheckpointEachNode)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

} // namespace
} // namespace dagforge::workflow
