#include "dagforge/workflow/plan_compiler.hpp"

#include "dagforge/util/json.hpp"

#include "../detail/sha256.hpp"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <format>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto contains_port(const NodePlan &node,
                                 const WorkflowPortId &port) -> bool {
  return std::ranges::any_of(
      node.outputs, [&](const auto &candidate) { return candidate == port; });
}

[[nodiscard]] auto node_path(std::size_t index, std::string_view suffix)
    -> std::string {
  return std::format("/nodes/{}{}", index, suffix);
}

[[nodiscard]] auto reject_plan(
    Error kind, std::string code, std::string description, std::string path,
    std::optional<WorkflowNodeId> node_id = std::nullopt,
    std::optional<std::string> executor = std::nullopt) -> PlanDiagnostic {
  return make_plan_diagnostic(kind, std::move(code), std::move(description),
                              std::move(path), std::move(node_id),
                              std::move(executor));
}

[[nodiscard]] auto reject_plan(
    std::error_code cause, std::string code, std::string description,
    std::string path, std::optional<WorkflowNodeId> node_id = std::nullopt,
    std::optional<std::string> executor = std::nullopt) -> PlanDiagnostic {
  return make_plan_diagnostic(cause, std::move(code), std::move(description),
                              std::move(path), std::move(node_id),
                              std::move(executor));
}

[[nodiscard]] auto executor_diagnostic(const ExecutorCompileFailure &failure,
                                       std::size_t node_index,
                                       const NodePlan &node) -> PlanDiagnostic {
  auto path = failure.code == "executor_not_registered"
                  ? node_path(node_index, "/executor")
                  : node_path(node_index, "/config");
  if (failure.code != "executor_not_registered" && !failure.path.empty()) {
    path += failure.path.starts_with('/') ? failure.path : "/" + failure.path;
  }
  return make_plan_diagnostic(failure.kind, failure.code, failure.description,
                              std::move(path), node.node_id, node.executor,
                              failure.details);
}

[[nodiscard]] auto canonical_config(const JsonPayload &config)
    -> Result<JsonPayload> {
  auto parsed = parse_json_as<glz::generic_sorted_i64>(config.encoded());
  if (!parsed || !parsed->is_object()) {
    return fail(parsed ? Error::InvalidArgument : parsed.error());
  }
  return JsonPayload::from(*parsed);
}

[[nodiscard]] auto canonical_plan(WorkflowPlan plan) -> Result<std::string> {
  for (auto &node : plan.nodes) {
    auto config = canonical_config(node.config);
    if (!config) {
      return fail(config.error());
    }
    node.config = std::move(*config);
    std::ranges::sort(node.outputs);
    std::ranges::sort(
        node.inputs, [](const InputBinding &lhs, const InputBinding &rhs) {
          return std::tie(lhs.input, lhs.source.node_id, lhs.source.port) <
                 std::tie(rhs.input, rhs.source.node_id, rhs.source.port);
        });
  }
  std::ranges::sort(plan.nodes, {}, &NodePlan::node_id);
  std::ranges::sort(
      plan.edges, [](const ConditionalEdge &lhs, const ConditionalEdge &rhs) {
        return std::tie(lhs.source.node_id, lhs.source.port, lhs.target,
                        lhs.condition.kind, lhs.condition.expected_bool,
                        lhs.condition.expected_string) <
               std::tie(rhs.source.node_id, rhs.source.port, rhs.target,
                        rhs.condition.kind, rhs.condition.expected_bool,
                        rhs.condition.expected_string);
      });
  std::ranges::sort(plan.outputs, [](const OutputRef &lhs,
                                     const OutputRef &rhs) {
    return std::tie(lhs.node_id, lhs.port) < std::tie(rhs.node_id, rhs.port);
  });
  return serialize_json(plan);
}

} // namespace

PlanCompiler::PlanCompiler(const ExecutorRegistry &executors,
                           PlanValidator validator)
    : executors_(&executors), validator_(std::move(validator)) {}

auto PlanCompiler::compile(WorkflowPlan plan) const
    -> PlanResult<std::shared_ptr<const ExecutionPlan>> {
  auto validated = validator_.validate(plan);
  if (!validated) {
    return plan_fail(std::move(validated.error()));
  }

  std::unordered_map<std::string, std::size_t> node_index;
  node_index.reserve(plan.nodes.size());
  for (std::size_t index = 0; index < plan.nodes.size(); ++index) {
    auto &node = plan.nodes[index];
    if (node.outputs.empty()) {
      node.outputs.emplace_back("result");
    }

    std::unordered_set<std::string> output_names;
    for (std::size_t output_index = 0; output_index < node.outputs.size();
         ++output_index) {
      const auto &output = node.outputs[output_index];
      if (!output_names.emplace(output.str()).second) {
        return plan_fail(reject_plan(
            Error::AlreadyExists, "plan_node_output_duplicate",
            "Workflow Plan node output names must be unique",
            node_path(index, std::format("/outputs/{}", output_index)),
            node.node_id, node.executor));
      }
    }

    if (!node_index.emplace(node.node_id.str(), index).second) {
      return plan_fail(
          reject_plan(Error::AlreadyExists, "plan_node_id_duplicate",
                      "Workflow Plan node ids must be unique",
                      node_path(index, "/id"), node.node_id, node.executor));
    }
  }

  std::unordered_set<OutputRef, OutputRefHash> published_outputs;
  for (std::size_t output_index = 0; output_index < plan.outputs.size();
       ++output_index) {
    const auto &output = plan.outputs[output_index];
    const auto source = node_index.find(output.node_id.str());
    if (source == node_index.end() ||
        !contains_port(plan.nodes[source->second], output.port)) {
      return plan_fail(reject_plan(
          Error::NotFound, "plan_published_output_not_found",
          "Published output does not reference a declared node output",
          std::format("/outputs/{}", output_index)));
    }
    if (!published_outputs.emplace(output).second) {
      return plan_fail(reject_plan(Error::AlreadyExists,
                                   "plan_published_output_duplicate",
                                   "Published outputs must be unique",
                                   std::format("/outputs/{}", output_index)));
    }
  }

  std::vector<CompiledNode> compiled;
  compiled.reserve(plan.nodes.size());
  for (std::size_t index = 0; index < plan.nodes.size(); ++index) {
    compiled.push_back(CompiledNode{
        .index = index,
        .plan = plan.nodes[index],
        .source_config = plan.nodes[index].config,
    });
  }

  auto add_dependency = [&](std::size_t source, std::size_t target) {
    auto &dependencies = compiled[target].dependencies;
    if (std::ranges::find(dependencies, source) == dependencies.end()) {
      dependencies.push_back(source);
      compiled[source].dependents.push_back(target);
    }
  };

  for (std::size_t target = 0; target < compiled.size(); ++target) {
    std::unordered_set<std::string> input_names;
    for (std::size_t input_index = 0;
         input_index < compiled[target].plan.inputs.size(); ++input_index) {
      const auto &binding = compiled[target].plan.inputs[input_index];
      if (!input_names.emplace(binding.input.str()).second) {
        return plan_fail(reject_plan(
            Error::AlreadyExists, "plan_node_input_duplicate",
            "Workflow Plan node input names must be unique",
            node_path(target, std::format("/inputs/{}/input", input_index)),
            compiled[target].plan.node_id, compiled[target].plan.executor));
      }
      const auto source_it = node_index.find(binding.source.node_id.str());
      if (source_it == node_index.end() || source_it->second == target ||
          !contains_port(compiled[source_it->second].plan,
                         binding.source.port)) {
        return plan_fail(reject_plan(
            Error::NotFound, "plan_node_input_source_not_found",
            "Workflow Plan input does not reference a declared upstream output",
            node_path(target, std::format("/inputs/{}", input_index)),
            compiled[target].plan.node_id, compiled[target].plan.executor));
      }
      add_dependency(source_it->second, target);
    }
  }

  for (std::size_t edge_index = 0; edge_index < plan.edges.size();
       ++edge_index) {
    const auto &edge = plan.edges[edge_index];
    const auto source_it = node_index.find(edge.source.node_id.str());
    const auto target_it = node_index.find(edge.target.str());
    if (source_it == node_index.end() || target_it == node_index.end() ||
        source_it->second == target_it->second ||
        !contains_port(compiled[source_it->second].plan, edge.source.port)) {
      return plan_fail(reject_plan(Error::NotFound,
                                   "plan_edge_reference_not_found",
                                   "Workflow Plan edge does not reference "
                                   "valid distinct nodes and output",
                                   std::format("/edges/{}", edge_index)));
    }
    add_dependency(source_it->second, target_it->second);
  }

  std::vector<std::size_t> indegree(compiled.size());
  std::deque<std::size_t> ready;
  for (std::size_t index = 0; index < compiled.size(); ++index) {
    indegree[index] = compiled[index].dependencies.size();
    if (indegree[index] == 0) {
      ready.push_back(index);
    }
  }

  std::vector<std::size_t> topological_order;
  topological_order.reserve(compiled.size());
  while (!ready.empty()) {
    const auto current = ready.front();
    ready.pop_front();
    topological_order.push_back(current);
    for (const auto dependent : compiled[current].dependents) {
      if (--indegree[dependent] == 0) {
        ready.push_back(dependent);
      }
    }
  }
  if (topological_order.size() != compiled.size()) {
    return plan_fail(reject_plan(Error::CycleDetected, "plan_cycle_detected",
                                 "Workflow Plan contains a dependency cycle",
                                 "/nodes"));
  }

  for (std::size_t target = 0; target < compiled.size(); ++target) {
    auto compiled_config = executors_->compile(
        compiled[target].plan.executor, compiled[target].plan.config,
        ExecutorCompileContext{.inputs = compiled[target].plan.inputs,
                               .outputs = compiled[target].plan.outputs});
    if (!compiled_config) {
      return plan_fail(executor_diagnostic(compiled_config.error(), target,
                                           compiled[target].plan));
    }
    auto canonical = canonical_config(compiled_config->encoded());
    if (!canonical) {
      return plan_fail(reject_plan(
          canonical.error(), "executor_compiled_config_invalid",
          "Executor produced an invalid compiled configuration",
          node_path(target, "/config"), compiled[target].plan.node_id,
          compiled[target].plan.executor));
    }
    compiled[target].executor_config = std::move(*compiled_config);
    compiled[target].plan.config = std::move(*canonical);
    plan.nodes[target].config = compiled[target].plan.config;
  }

  auto plan_digest = digest(plan);
  if (!plan_digest) {
    return plan_fail(reject_plan(plan_digest.error(), "plan_digest_failed",
                                 "Workflow Plan digest could not be computed",
                                 ""));
  }

  auto execution_plan = std::make_shared<ExecutionPlan>();
  execution_plan->plan_id = generate_workflow_plan_id();
  execution_plan->workflow_id = plan.workflow_id;
  execution_plan->schema_version = plan.schema_version;
  execution_plan->digest = std::move(*plan_digest);
  execution_plan->nodes = std::move(compiled);
  execution_plan->edges = std::move(plan.edges);
  execution_plan->topological_order = std::move(topological_order);
  execution_plan->outputs = std::move(plan.outputs);
  execution_plan->policy = std::move(plan.policy);
  return plan_ok(
      std::shared_ptr<const ExecutionPlan>{std::move(execution_plan)});
}

auto PlanCompiler::compile(WorkflowPlan plan,
                           const WorkflowPlanId &plan_id) const
    -> PlanResult<std::shared_ptr<const ExecutionPlan>> {
  if (plan_id.empty()) {
    return plan_fail(reject_plan(Error::InvalidArgument, "plan_id_required",
                                 "Restored Workflow Plan requires a plan_id",
                                 "/plan_id"));
  }
  auto compiled = compile(std::move(plan));
  if (!compiled) {
    return plan_fail(std::move(compiled.error()));
  }
  auto restored = std::make_shared<ExecutionPlan>(**compiled);
  restored->plan_id = plan_id.clone();
  return plan_ok(std::shared_ptr<const ExecutionPlan>{std::move(restored)});
}

auto PlanCompiler::digest(const WorkflowPlan &plan) -> Result<std::string> {
  auto canonical = canonical_plan(plan);
  if (!canonical) {
    return fail(canonical.error());
  }
  return detail::sha256_hex(*canonical);
}

} // namespace dagforge::workflow
