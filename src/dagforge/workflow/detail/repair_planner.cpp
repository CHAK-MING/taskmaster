#include "repair_planner.hpp"

#include "dagforge/util/json.hpp"

#include <algorithm>
#include <cstddef>
#include <format>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto output_key(const OutputRef &output) -> std::string {
  return std::format("{}\x1f{}", output.node_id, output.port);
}

[[nodiscard]] auto same_inputs(std::span<const InputBinding> left,
                               std::span<const InputBinding> right) -> bool {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (left[index].input != right[index].input ||
        left[index].source != right[index].source) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] auto same_outputs(std::span<const WorkflowPortId> left,
                                std::span<const WorkflowPortId> right) -> bool {
  return std::ranges::equal(left, right);
}

[[nodiscard]] auto same_execution_contract(const NodePlan &left,
                                            const NodePlan &right) -> bool {
  return left.executor == right.executor &&
         dump_json(left.config) == dump_json(right.config) &&
         same_inputs(left.inputs, right.inputs) &&
         same_outputs(left.outputs, right.outputs) &&
         left.timeout == right.timeout;
}

[[nodiscard]] auto edge_key(const ConditionalEdge &edge) -> std::string {
  return std::format("{}\x1f{}\x1f{}\x1f{}\x1f{}", edge.source.node_id,
                     edge.source.port, std::to_underlying(edge.condition.kind),
                     edge.condition.expected_bool,
                     edge.condition.expected_string);
}

[[nodiscard]] auto incoming_edges(const WorkflowPlan &plan,
                                  const WorkflowNodeId &node_id)
    -> std::vector<std::string> {
  std::vector<std::string> edges;
  for (const auto &edge : plan.edges) {
    if (edge.target == node_id) {
      edges.push_back(edge_key(edge));
    }
  }
  std::ranges::sort(edges);
  return edges;
}

[[nodiscard]] auto revised_source_plan(const ExecutionPlan &execution)
    -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = execution.workflow_id.clone();
  plan.nodes.reserve(execution.nodes.size());
  for (const auto &node : execution.nodes) {
    plan.nodes.push_back(node.plan);
  }
  plan.edges = execution.edges;
  plan.outputs = execution.outputs;
  plan.policy = execution.policy;
  return plan;
}

} // namespace

auto plan_repair(const ExecutionPlan &revised,
                 const WorkflowCheckpoint &parent,
                 const IArtifactStore &artifacts) -> Result<RepairPlan> {
  if (revised.workflow_id != parent.snapshot.workflow_id ||
      revised.workflow_id != parent.plan.workflow_id) {
    return fail(Error::InvalidArgument);
  }

  std::unordered_map<std::string, const NodePlan *> parent_nodes;
  for (const auto &node : parent.plan.nodes) {
    parent_nodes.emplace(node.node_id.str(), std::addressof(node));
  }
  std::unordered_map<std::string, const TaskSnapshot *> parent_tasks;
  for (const auto &task : parent.snapshot.tasks) {
    parent_tasks.emplace(task.node_id.str(), std::addressof(task));
  }
  std::unordered_map<std::string,
                     const std::pair<OutputRef, WorkflowValue> *>
      parent_values;
  for (const auto &value : parent.values) {
    parent_values.emplace(output_key(value.first), std::addressof(value));
  }

  const auto target_plan = revised_source_plan(revised);
  RepairPlan result;
  result.decisions.reserve(revised.nodes.size());

  std::vector<bool> reusable(revised.nodes.size(), false);
  for (const auto node_index : revised.topological_order) {
    const auto &compiled = revised.nodes[node_index];
    const auto node_key = compiled.plan.node_id.str();
    RepairNodeDecision decision{.node_id = compiled.plan.node_id.clone()};

    const auto parent_node = parent_nodes.find(node_key);
    const auto parent_task = parent_tasks.find(node_key);
    if (parent_node == parent_nodes.end() || parent_task == parent_tasks.end()) {
      decision.reason = "node_added";
    } else if (parent_task->second->state != TaskState::Succeeded) {
      decision.reason = "source_not_succeeded";
    } else if (!same_execution_contract(*parent_node->second,
                                        compiled.plan)) {
      decision.reason = "execution_contract_changed";
    } else if (incoming_edges(parent.plan, compiled.plan.node_id) !=
               incoming_edges(target_plan, compiled.plan.node_id)) {
      decision.reason = "incoming_condition_changed";
    } else if (std::ranges::any_of(
                   compiled.dependencies,
                   [&](std::size_t dependency) { return !reusable[dependency]; })) {
      decision.reason = "dependency_invalidated";
    } else {
      bool missing_required_output = false;
      for (const auto &port : compiled.plan.outputs) {
        OutputRef output{.node_id = compiled.plan.node_id.clone(),
                         .port = port.clone()};
        const auto key = output_key(output);
        const auto retained = parent_values.find(key);
        if (retained == parent_values.end()) {
          missing_required_output = true;
          break;
        }
        if (const auto *artifact =
                std::get_if<ArtifactRef>(&retained->second->second);
            artifact != nullptr && !artifacts.get(artifact->artifact_id)) {
          missing_required_output = true;
          break;
        }
      }
      if (missing_required_output) {
        decision.reason = "required_output_missing";
      } else {
        decision.reused = true;
        decision.reason = "reused";
        reusable[node_index] = true;
        result.reused_nodes.emplace(node_key);
        for (const auto &[output, value] : parent.values) {
          if (output.node_id == compiled.plan.node_id) {
            result.values.emplace_back(output, value);
          }
        }
      }
    }
    result.decisions.push_back(std::move(decision));
  }
  return ok(std::move(result));
}

} // namespace dagforge::workflow::detail
