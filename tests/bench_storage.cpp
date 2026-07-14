#include "dagforge/workflow/checkpoint_store.hpp"

#include "benchmark_compat.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <format>
#include <string>
#include <system_error>
#include <unistd.h>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto make_checkpoint(std::size_t node_count)
    -> WorkflowCheckpoint {
  WorkflowCheckpoint checkpoint;
  checkpoint.plan.workflow_id = WorkflowId{"bench-checkpoint"};
  checkpoint.plan.nodes.reserve(node_count);
  checkpoint.snapshot.run_id = WorkflowRunId{"bench-checkpoint__run"};
  checkpoint.snapshot.workflow_id = checkpoint.plan.workflow_id.clone();
  checkpoint.snapshot.plan_id = WorkflowPlanId{"bench-plan"};
  checkpoint.snapshot.state = RunState::Running;
  checkpoint.snapshot.tasks.reserve(node_count);
  checkpoint.values.reserve(node_count);

  for (std::size_t index = 0; index < node_count; ++index) {
    const auto node_name = std::format("node-{}", index);
    checkpoint.plan.nodes.push_back(NodePlan{
        .node_id = WorkflowNodeId{node_name},
        .executor = "bench",
        .outputs = {WorkflowPortId{"result"}},
    });
    checkpoint.snapshot.tasks.push_back(TaskSnapshot{
        .node_id = WorkflowNodeId{node_name},
        .state = TaskState::Succeeded,
        .attempt_count = 1,
        .attempts = {AttemptSnapshot{
            .attempt_id = AttemptId{std::format("attempt-{}", index)},
            .number = 1,
            .state = AttemptState::Succeeded,
        }},
    });
    checkpoint.values.emplace_back(
        OutputRef{.node_id = WorkflowNodeId{node_name},
                  .port = WorkflowPortId{"result"}},
        std::string(64, 'x'));
  }
  return checkpoint;
}

void BM_CheckpointMemorySave(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  const auto checkpoint = make_checkpoint(node_count);
  CheckpointStore store;
  for (auto _ : state) {
    auto saved = store.save(checkpoint);
    if (!saved) {
      state.SkipWithError(saved.error().message().c_str());
      return;
    }
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
}

void BM_CheckpointFileSave(benchmark::State &state) {
  const auto node_count = static_cast<std::size_t>(state.range(0));
  const auto checkpoint = make_checkpoint(node_count);
  const auto directory =
      std::filesystem::temp_directory_path() /
      std::format("dagforge-bench-checkpoint-{}-{}", ::getpid(), node_count);
  std::error_code error;
  std::filesystem::remove_all(directory, error);
  CheckpointStore store(directory);
  for (auto _ : state) {
    auto saved = store.save(checkpoint);
    if (!saved) {
      state.SkipWithError(saved.error().message().c_str());
      std::filesystem::remove_all(directory, error);
      return;
    }
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(node_count) *
                          state.iterations());
  std::filesystem::remove_all(directory, error);
}

BENCHMARK(BM_CheckpointMemorySave)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_CheckpointFileSave)
    ->Arg(8)
    ->Arg(32)
    ->Arg(128)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime();

} // namespace
} // namespace dagforge::workflow
