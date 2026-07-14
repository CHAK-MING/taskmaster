#include "dagforge/workflow/plan_compiler.hpp"

#include "dagforge/util/json.hpp"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <deque>
#include <memory>
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

[[nodiscard]] auto canonical_json(const JsonValue &value) -> JsonValue {
  if (value.is_object()) {
    std::vector<std::pair<std::string_view, const JsonValue *>> fields;
    fields.reserve(value.get_object().size());
    for (const auto &[key, field] : value.get_object()) {
      fields.emplace_back(key, &field);
    }
    std::ranges::sort(fields, {}, &decltype(fields)::value_type::first);

    JsonValue sorted = JsonValue::object_t{};
    for (const auto &[key, field] : fields) {
      sorted[std::string{key}] = canonical_json(*field);
    }
    return sorted;
  }
  if (value.is_array()) {
    JsonValue sorted = JsonValue::array_t{};
    sorted.get_array().reserve(value.get_array().size());
    for (const auto &element : value.get_array()) {
      sorted.get_array().push_back(canonical_json(element));
    }
    return sorted;
  }
  return value;
}

[[nodiscard]] auto sha256(std::string_view input) -> Result<std::string> {
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>{
      EVP_MD_CTX_new(), &EVP_MD_CTX_free};
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), input.data(), input.size()) != 1) {
    return fail(Error::Unknown);
  }

  std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(context.get(), bytes.data(), &size) != 1) {
    return fail(Error::Unknown);
  }

  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.resize(static_cast<std::size_t>(size) * 2);
  for (unsigned int i = 0; i < size; ++i) {
    out[static_cast<std::size_t>(i) * 2] = kHex[bytes[i] >> 4U];
    out[static_cast<std::size_t>(i) * 2 + 1] = kHex[bytes[i] & 0x0fU];
  }
  return ok(std::move(out));
}

[[nodiscard]] auto canonical_plan(const WorkflowPlan &plan)
    -> Result<std::string> {
  std::vector<const NodePlan *> nodes;
  nodes.reserve(plan.nodes.size());
  for (const auto &node : plan.nodes) {
    nodes.push_back(&node);
  }
  std::ranges::sort(nodes, {}, [](const NodePlan *node) {
    return node->node_id.value();
  });

  std::vector<const ConditionalEdge *> edges;
  edges.reserve(plan.edges.size());
  for (const auto &edge : plan.edges) {
    edges.push_back(&edge);
  }
  std::ranges::sort(edges, [](const ConditionalEdge *lhs,
                              const ConditionalEdge *rhs) {
    return std::tie(lhs->source.node_id, lhs->source.port, lhs->target,
                    lhs->condition.kind, lhs->condition.expected_bool,
                    lhs->condition.expected_string) <
           std::tie(rhs->source.node_id, rhs->source.port, rhs->target,
                    rhs->condition.kind, rhs->condition.expected_bool,
                    rhs->condition.expected_string);
  });

  JsonValue canonical = JsonValue::object_t{};
  canonical["workflow_id"] = plan.workflow_id.str();
  canonical["schema_version"] =
      static_cast<std::int64_t>(plan.schema_version);

  JsonValue budget = JsonValue::object_t{};
  budget["max_nodes"] =
      static_cast<std::int64_t>(plan.policy.budget.max_nodes);
  budget["max_parallel_nodes"] =
      static_cast<std::int64_t>(plan.policy.budget.max_parallel_nodes);
  budget["max_total_output_bytes"] = static_cast<std::int64_t>(
      plan.policy.budget.max_total_output_bytes);
  budget["max_run_duration"] = static_cast<std::int64_t>(
      plan.policy.budget.max_run_duration.count());

  JsonValue policy = JsonValue::object_t{};
  policy["failure_policy"] = static_cast<std::int64_t>(
      static_cast<unsigned>(plan.policy.failure_policy));
  policy["budget"] = std::move(budget);
  canonical["policy"] = std::move(policy);

  JsonValue canonical_nodes = JsonValue::array_t{};
  canonical_nodes.get_array().reserve(nodes.size());

  for (const auto *node : nodes) {
    JsonValue serialized = JsonValue::object_t{};
    serialized["id"] = node->node_id.str();
    serialized["name"] = node->name;
    serialized["executor"] = node->executor;
    serialized["config"] = canonical_json(node->config);
    serialized["max_retries"] =
        static_cast<std::int64_t>(node->max_retries);
    serialized["retry_initial_delay"] = static_cast<std::int64_t>(
        node->retry_initial_delay.count());
    serialized["retry_max_delay"] =
        static_cast<std::int64_t>(node->retry_max_delay.count());
    serialized["timeout"] =
        static_cast<std::int64_t>(node->timeout.count());
    serialized["checkpoint"] = node->checkpoint;

    auto outputs = node->outputs;
    std::ranges::sort(outputs);
    JsonValue serialized_outputs = JsonValue::array_t{};
    serialized_outputs.get_array().reserve(outputs.size());
    for (const auto &output : outputs) {
      serialized_outputs.get_array().push_back(output.str());
    }
    serialized["outputs"] = std::move(serialized_outputs);

    auto inputs = node->inputs;
    std::ranges::sort(inputs, [](const InputBinding &lhs,
                                 const InputBinding &rhs) {
      return std::tie(lhs.input, lhs.source.node_id, lhs.source.port) <
             std::tie(rhs.input, rhs.source.node_id, rhs.source.port);
    });
    JsonValue serialized_inputs = JsonValue::array_t{};
    serialized_inputs.get_array().reserve(inputs.size());
    for (const auto &input : inputs) {
      JsonValue binding = JsonValue::object_t{};
      binding["input"] = input.input.str();
      binding["node"] = input.source.node_id.str();
      binding["port"] = input.source.port.str();
      serialized_inputs.get_array().push_back(std::move(binding));
    }
    serialized["inputs"] = std::move(serialized_inputs);
    canonical_nodes.get_array().push_back(std::move(serialized));
  }
  canonical["nodes"] = std::move(canonical_nodes);

  JsonValue canonical_edges = JsonValue::array_t{};
  canonical_edges.get_array().reserve(edges.size());
  for (const auto *edge : edges) {
    JsonValue condition = JsonValue::object_t{};
    condition["kind"] = static_cast<std::int64_t>(
        static_cast<unsigned>(edge->condition.kind));
    condition["expected_bool"] = edge->condition.expected_bool;
    condition["expected_string"] = edge->condition.expected_string;

    JsonValue serialized = JsonValue::object_t{};
    serialized["source_node"] = edge->source.node_id.str();
    serialized["source_port"] = edge->source.port.str();
    serialized["target"] = edge->target.str();
    serialized["condition"] = std::move(condition);
    canonical_edges.get_array().push_back(std::move(serialized));
  }
  canonical["edges"] = std::move(canonical_edges);

  auto published_outputs = plan.outputs;
  std::ranges::sort(published_outputs, [](const OutputRef &lhs,
                                          const OutputRef &rhs) {
    return std::tie(lhs.node_id, lhs.port) <
           std::tie(rhs.node_id, rhs.port);
  });
  JsonValue canonical_outputs = JsonValue::array_t{};
  canonical_outputs.get_array().reserve(published_outputs.size());
  for (const auto &output : published_outputs) {
    JsonValue serialized = JsonValue::object_t{};
    serialized["node"] = output.node_id.str();
    serialized["port"] = output.port.str();
    canonical_outputs.get_array().push_back(std::move(serialized));
  }
  canonical["outputs"] = std::move(canonical_outputs);

  return serialize_json(canonical);
}

} // namespace

auto PolicyEngine::validate(const WorkflowPlan &plan) const -> Result<void> {
  if (plan.workflow_id.empty() || plan.schema_version != 1 ||
      plan.nodes.empty()) {
    return fail(Error::InvalidArgument);
  }

  switch (plan.policy.failure_policy) {
  case FailurePolicy::ContinueIndependent:
  case FailurePolicy::FailFast:
    break;
  default:
    return fail(Error::InvalidArgument);
  }

  const auto &budget = plan.policy.budget;
  if (budget.max_nodes == 0 || budget.max_parallel_nodes == 0 ||
      budget.max_total_output_bytes == 0 || budget.max_run_duration <=
                                                std::chrono::seconds::zero() ||
      plan.nodes.size() > budget.max_nodes) {
    return fail(Error::ResourceExhausted);
  }

  for (const auto &node : plan.nodes) {
    if (node.node_id.empty() || node.timeout <= std::chrono::seconds::zero() ||
        node.max_retries < 0 ||
        node.retry_initial_delay < std::chrono::milliseconds::zero() ||
        node.retry_max_delay < node.retry_initial_delay) {
      return fail(Error::InvalidArgument);
    }

    if (node.executor.empty()) {
      return fail(Error::InvalidArgument);
    }
  }
  return ok();
}

PlanCompiler::PlanCompiler(const ExecutorRegistry &executors,
                           PolicyEngine policy_engine)
    : executors_(&executors), policy_engine_(std::move(policy_engine)) {}

auto PlanCompiler::compile(WorkflowPlan plan) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto policy_result = policy_engine_.validate(plan);
  if (!policy_result) {
    return fail(policy_result.error());
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

  std::unordered_set<std::string> published_outputs;
  for (const auto &output : plan.outputs) {
    const auto source = node_index.find(output.node_id.str());
    std::string key;
    key.reserve(output.node_id.size() + output.port.size() + 1);
    key.append(output.node_id.value());
    key.push_back('\x1f');
    key.append(output.port.value());
    if (output.node_id.empty() || output.port.empty() ||
        source == node_index.end() ||
        !contains_port(plan.nodes[source->second], output.port)) {
      return fail(Error::NotFound);
    }
    if (!published_outputs.emplace(std::move(key)).second) {
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
    compiled[target].plan.config = std::move(*compiled_config);
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
  return sha256(*canonical);
}

} // namespace dagforge::workflow
