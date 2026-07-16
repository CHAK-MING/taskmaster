#include "checkpoint_validator.hpp"

#include "../../detail/state_machine.hpp"
#include "../../detail/value_size.hpp"

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <unordered_set>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto failure_is_valid(const ExecutionFailure &failure) -> bool {
  return failure.kind != Error::Success && !failure.code.empty() &&
         !failure.message.empty() && failure.details.is_object() &&
         std::ranges::all_of(failure.artifacts, [](const auto &artifact) {
           return !artifact.name.empty() &&
                  !artifact.artifact.artifact_id.empty() &&
                  !artifact.artifact.media_type.empty() &&
                  !artifact.artifact.digest.empty();
         });
}

[[nodiscard]] auto snapshot_failures_are_valid(const RunSnapshot &snapshot)
    -> bool {
  if (snapshot.failure && !failure_is_valid(*snapshot.failure)) {
    return false;
  }
  for (const auto &task : snapshot.tasks) {
    if (task.failure && !failure_is_valid(*task.failure)) {
      return false;
    }
    for (const auto &attempt : task.attempts) {
      if (attempt.failure && !failure_is_valid(*attempt.failure)) {
        return false;
      }
    }
  }
  return true;
}

[[nodiscard]] auto output_declared(const WorkflowPlan &plan,
                                   const OutputRef &output) -> bool {
  const auto node = std::ranges::find_if(
      plan.nodes, [&](const NodePlan &candidate) {
        return candidate.node_id == output.node_id;
      });
  return node != plan.nodes.end() &&
         std::ranges::find(node->outputs, output.port) != node->outputs.end();
}

} // namespace

auto validate_checkpoint_model(const WorkflowCheckpoint &checkpoint)
    -> Result<void> {
  if (checkpoint.plan.workflow_id.empty() ||
      checkpoint.trigger.trigger_id.empty() ||
      checkpoint.plan.workflow_id != checkpoint.trigger.workflow_id ||
      checkpoint.plan.workflow_id != checkpoint.snapshot.workflow_id ||
      checkpoint.snapshot.tasks.size() != checkpoint.plan.nodes.size() ||
      !run_snapshot_is_valid(checkpoint.snapshot) ||
      !snapshot_failures_are_valid(checkpoint.snapshot)) {
    return fail(Error::InvalidArgument);
  }
  for (std::size_t index = 0; index < checkpoint.plan.nodes.size(); ++index) {
    if (checkpoint.snapshot.tasks[index].node_id !=
        checkpoint.plan.nodes[index].node_id) {
      return fail(Error::InvalidArgument);
    }
  }

  std::unordered_set<OutputRef, OutputRefHash> outputs;
  std::uint64_t total_output_bytes = 0;
  for (const auto &entry : checkpoint.values) {
    const auto &output = entry.output;
    const auto &value = entry.value;
    const auto node = std::ranges::find_if(
        checkpoint.plan.nodes, [&](const NodePlan &candidate) {
          return candidate.node_id == output.node_id;
        });
    if (node == checkpoint.plan.nodes.end()) {
      return fail(Error::InvalidArgument);
    }
    const auto node_index = static_cast<std::size_t>(
        std::distance(checkpoint.plan.nodes.begin(), node));
    if (!output_declared(checkpoint.plan, output) ||
        checkpoint.snapshot.tasks[node_index].state != TaskState::Succeeded ||
        !outputs.emplace(output).second) {
      return fail(Error::InvalidArgument);
    }
    const auto value_bytes = value_size_bytes(value);
    if (value_bytes >
        checkpoint.plan.policy.budget.max_total_output_bytes -
            total_output_bytes) {
      return fail(Error::ResourceExhausted);
    }
    total_output_bytes += value_bytes;
  }

  if (checkpoint.snapshot.state == RunState::Succeeded) {
    if (!std::ranges::all_of(checkpoint.snapshot.tasks, [](const auto &task) {
          return task.state == TaskState::Succeeded ||
                 task.state == TaskState::Skipped;
        })) {
      return fail(Error::InvalidArgument);
    }
    for (const auto &published : checkpoint.plan.outputs) {
      if (!outputs.contains(published)) {
        return fail(Error::InvalidArgument);
      }
    }
  }
  return ok();
}

} // namespace dagforge::workflow::detail
