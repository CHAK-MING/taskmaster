#include "dagforge/workflow/plan_compiler.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/workflow/node_configs.hpp"

#include <openssl/evp.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <deque>
#include <format>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto valid_env_key(std::string_view key) -> bool {
  if (key.empty() ||
      !(std::isalpha(static_cast<unsigned char>(key.front())) != 0 ||
        key.front() == '_')) {
    return false;
  }
  return std::ranges::all_of(key.substr(1), [](unsigned char ch) {
    return std::isalnum(ch) != 0 || ch == '_';
  });
}

[[nodiscard]] auto contains_port(const NodePlan &node,
                                 const WorkflowPortId &port) -> bool {
  return std::ranges::any_of(node.outputs, [&](const auto &candidate) {
    return candidate == port;
  });
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
    return std::tie(lhs->source.node_id, lhs->source.port, lhs->target) <
           std::tie(rhs->source.node_id, rhs->source.port, rhs->target);
  });

  std::string canonical;
  canonical.reserve(1024 + plan.nodes.size() * 256);
  std::format_to(std::back_inserter(canonical), "workflow={};schema={};",
                 plan.workflow_id, plan.schema_version);
  std::format_to(
      std::back_inserter(canonical),
      "policy={},{},{},{},{};",
      static_cast<unsigned>(plan.policy.failure_policy),
      plan.policy.budget.max_nodes, plan.policy.budget.max_parallel_nodes,
      plan.policy.budget.max_total_output_bytes,
      plan.policy.budget.max_run_duration.count());

  for (const auto *node : nodes) {
    auto config = serialize_json(node->command);
    if (!config) {
      return fail(config.error());
    }
    std::format_to(std::back_inserter(canonical),
                   "node={};name={};retry={};retry_initial={};retry_max={};timeout={};checkpoint={};command={};",
                   node->node_id, node->name,
                   node->max_retries, node->retry_initial_delay.count(),
                   node->retry_max_delay.count(), node->timeout.count(),
                   node->checkpoint, *config);

    auto outputs = node->outputs;
    std::ranges::sort(outputs);
    for (const auto &output : outputs) {
      std::format_to(std::back_inserter(canonical), "out={};", output);
    }

    auto inputs = node->inputs;
    std::ranges::sort(inputs, [](const InputBinding &lhs,
                                 const InputBinding &rhs) {
      return std::tie(lhs.input, lhs.source.node_id, lhs.source.port) <
             std::tie(rhs.input, rhs.source.node_id, rhs.source.port);
    });
    for (const auto &input : inputs) {
      std::format_to(std::back_inserter(canonical), "in={}:{}:{};",
                     input.input, input.source.node_id, input.source.port);
    }
  }

  for (const auto *edge : edges) {
    std::format_to(std::back_inserter(canonical),
                   "edge={}:{}:{}:{}:{}:{};", edge->source.node_id,
                   edge->source.port, edge->target,
                   static_cast<unsigned>(edge->condition.kind),
                   edge->condition.expected_bool,
                   edge->condition.expected_string);
  }

  return ok(std::move(canonical));
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

    if (node.command.program.empty() ||
        !std::string_view(node.command.program).starts_with('/')) {
      return fail(Error::InvalidArgument);
    }
    std::unordered_set<std::string> env_names;
    for (const auto &entry : node.command.env) {
      if (!valid_env_key(entry.key) || !env_names.emplace(entry.key).second) {
        return fail(Error::InvalidArgument);
      }
    }
    std::unordered_set<std::string> input_names;
    for (const auto &input : node.inputs) {
      input_names.emplace(input.input.str());
    }
    for (const auto &binding : node.command.input_env) {
      if (binding.input.empty() || !input_names.contains(binding.input) ||
          !valid_env_key(binding.environment) ||
          !env_names.emplace(binding.environment).second) {
        return fail(Error::InvalidArgument);
      }
    }
  }
  return ok();
}

PlanCompiler::PlanCompiler(PolicyEngine policy_engine)
    : policy_engine_(std::move(policy_engine)) {}

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
