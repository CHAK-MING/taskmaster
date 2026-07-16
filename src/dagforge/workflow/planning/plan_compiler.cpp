#include "dagforge/workflow/plan_compiler.hpp"

#include "dagforge/util/json.hpp"

#include "../detail/sha256.hpp"

#include <algorithm>
#include <cstdint>
#include <deque>
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
  return std::ranges::any_of(node.outputs, [&](const auto &candidate) {
    return candidate == port;
  });
}

[[nodiscard]] auto canonical_config(const JsonPayload &config)
    -> Result<JsonPayload> {
  auto parsed = parse_json_as<glz::generic_sorted_i64>(config.encoded());
  if (!parsed || !parsed->is_object()) {
    return fail(parsed ? Error::InvalidArgument : parsed.error());
  }
  return JsonPayload::from(*parsed);
}

[[nodiscard]] auto canonical_plan(WorkflowPlan plan)
    -> Result<std::string> {
  for (auto &node : plan.nodes) {
    auto config = canonical_config(node.config);
    if (!config) {
      return fail(config.error());
    }
    node.config = std::move(*config);
    std::ranges::sort(node.outputs);
    std::ranges::sort(node.inputs, [](const InputBinding &lhs,
                                     const InputBinding &rhs) {
      return std::tie(lhs.input, lhs.source.node_id, lhs.source.port) <
             std::tie(rhs.input, rhs.source.node_id, rhs.source.port);
    });
  }
  std::ranges::sort(plan.nodes, {}, &NodePlan::node_id);
  std::ranges::sort(plan.edges, [](const ConditionalEdge &lhs,
                                   const ConditionalEdge &rhs) {
    return std::tie(lhs.source.node_id, lhs.source.port, lhs.target,
                    lhs.condition.kind, lhs.condition.expected_bool,
                    lhs.condition.expected_string) <
           std::tie(rhs.source.node_id, rhs.source.port, rhs.target,
                    rhs.condition.kind, rhs.condition.expected_bool,
                    rhs.condition.expected_string);
  });
  std::ranges::sort(plan.outputs, [](const OutputRef &lhs,
                                     const OutputRef &rhs) {
    return std::tie(lhs.node_id, lhs.port) <
           std::tie(rhs.node_id, rhs.port);
  });
  return serialize_json(plan);
}

} // namespace

PlanCompiler::PlanCompiler(const ExecutorRegistry &executors,
                           PlanValidator validator)
    : executors_(&executors), validator_(std::move(validator)) {}

auto PlanCompiler::compile(WorkflowPlan plan) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto validated = validator_.validate(plan);
  if (!validated) {
    return fail(validated.error());
  }

  std::unordered_map<std::string, std::size_t> node_index;
  node_index.reserve(plan.nodes.size());
  for (std::size_t index = 0; index < plan.nodes.size(); ++index) {
    auto &node = plan.nodes[index];
    if (node.outputs.empty()) {
      node.outputs.emplace_back("result");
    }

    std::unordered_set<std::string> output_names;
    for (const auto &output : node.outputs) {
      if (output.empty() || !output_names.emplace(output.str()).second) {
        return fail(Error::InvalidArgument);
      }
    }

    if (!node_index.emplace(node.node_id.str(), index).second) {
      return fail(Error::AlreadyExists);
    }
  }

  std::unordered_set<OutputRef, OutputRefHash> published_outputs;
  for (const auto &output : plan.outputs) {
    const auto source = node_index.find(output.node_id.str());
    if (output.node_id.empty() || output.port.empty() ||
        source == node_index.end() ||
        !contains_port(plan.nodes[source->second], output.port)) {
      return fail(Error::NotFound);
    }
    if (!published_outputs.emplace(output).second) {
      return fail(Error::AlreadyExists);
    }
  }

  std::vector<CompiledNode> compiled;
  compiled.reserve(plan.nodes.size());
  for (std::size_t index = 0; index < plan.nodes.size(); ++index) {
    compiled.push_back(CompiledNode{.index = index, .plan = plan.nodes[index]});
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
    for (const auto &binding : compiled[target].plan.inputs) {
      if (binding.input.empty() ||
          !input_names.emplace(binding.input.str()).second) {
        return fail(Error::InvalidArgument);
      }
      const auto source_it = node_index.find(binding.source.node_id.str());
      if (source_it == node_index.end() || source_it->second == target ||
          !contains_port(compiled[source_it->second].plan,
                         binding.source.port)) {
        return fail(Error::NotFound);
      }
      add_dependency(source_it->second, target);
    }

    auto compiled_config = executors_->compile(
        compiled[target].plan.executor, compiled[target].plan.config,
        ExecutorCompileContext{.inputs = compiled[target].plan.inputs,
                               .outputs = compiled[target].plan.outputs});
    if (!compiled_config) {
      return fail(compiled_config.error());
    }
    auto canonical = canonical_config(compiled_config->encoded());
    if (!canonical) {
      return fail(canonical.error());
    }
    compiled[target].executor_config = std::move(*compiled_config);
    compiled[target].plan.config = std::move(*canonical);
    plan.nodes[target].config = compiled[target].plan.config;
  }

  for (const auto &edge : plan.edges) {
    const auto source_it = node_index.find(edge.source.node_id.str());
    const auto target_it = node_index.find(edge.target.str());
    if (source_it == node_index.end() || target_it == node_index.end() ||
        source_it->second == target_it->second ||
        !contains_port(compiled[source_it->second].plan, edge.source.port)) {
      return fail(Error::NotFound);
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
    return fail(Error::CycleDetected);
  }

  auto plan_digest = digest(plan);
  if (!plan_digest) {
    return fail(plan_digest.error());
  }

  auto execution_plan = std::make_shared<ExecutionPlan>();
  execution_plan->plan_id = generate_workflow_plan_id();
  execution_plan->workflow_id = plan.workflow_id;
  execution_plan->digest = std::move(*plan_digest);
  execution_plan->nodes = std::move(compiled);
  execution_plan->edges = std::move(plan.edges);
  execution_plan->topological_order = std::move(topological_order);
  execution_plan->outputs = std::move(plan.outputs);
  execution_plan->policy = std::move(plan.policy);
  return ok(std::shared_ptr<const ExecutionPlan>{std::move(execution_plan)});
}

auto PlanCompiler::compile(WorkflowPlan plan,
                           const WorkflowPlanId &plan_id) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  if (plan_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  auto compiled = compile(std::move(plan));
  if (!compiled) {
    return fail(compiled.error());
  }
  auto restored = std::make_shared<ExecutionPlan>(**compiled);
  restored->plan_id = plan_id.clone();
  return ok(std::shared_ptr<const ExecutionPlan>{std::move(restored)});
}

auto PlanCompiler::digest(const WorkflowPlan &plan) -> Result<std::string> {
  auto canonical = canonical_plan(plan);
  if (!canonical) {
    return fail(canonical.error());
  }
  return detail::sha256_hex(*canonical);
}

} // namespace dagforge::workflow
