#include "dagforge/workflow/workflow_control_plane.hpp"

#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/json.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::workflow::detail {

struct BudgetDto {
  std::size_t max_nodes{256};
  std::size_t max_parallel_nodes{32};
  std::uint64_t max_total_output_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t max_model_tokens{1'000'000};
  int max_run_duration_sec{3600};
};

struct PolicyDto {
  bool allow_shell{false};
  bool allow_docker{true};
  bool allow_lua{true};
  bool allow_network{true};
  bool allow_model_calls{true};
  bool allow_tools{true};
  bool require_approval_for_shell{true};
  std::vector<std::string> allowed_http_hosts;
  std::vector<std::string> allowed_model_providers;
  std::vector<std::string> allowed_tools;
  BudgetDto budget;
};

struct InputDto {
  std::string input;
  std::string node;
  std::string port{"result"};
};

struct NodeDto {
  std::string id;
  std::string name;
  std::string type{"noop"};
  JsonValue config{JsonValue::object_t{}};
  std::vector<InputDto> inputs;
  std::vector<std::string> outputs;
  int max_retries{0};
  int timeout_sec{300};
  bool checkpoint{false};
};

struct ConditionDto {
  std::string kind{"always"};
  bool expected_bool{true};
  std::string expected_string;
};

struct EdgeDto {
  std::string source_node;
  std::string source_port{"result"};
  std::string target;
  ConditionDto condition;
};

struct OutputDto {
  std::string node;
  std::string port{"result"};
};

struct WorkflowPlanDto {
  std::string workflow_id;
  std::uint32_t schema_version{1};
  std::vector<NodeDto> nodes;
  std::vector<EdgeDto> edges;
  std::vector<OutputDto> outputs;
  PolicyDto policy;
};

} // namespace dagforge::workflow::detail

namespace glz {
template <> struct meta<dagforge::workflow::detail::BudgetDto> {
  using T = dagforge::workflow::detail::BudgetDto;
  static constexpr auto value = object(
      "max_nodes", &T::max_nodes, "max_parallel_nodes",
      &T::max_parallel_nodes, "max_total_output_bytes",
      &T::max_total_output_bytes, "max_model_tokens", &T::max_model_tokens,
      "max_run_duration_sec", &T::max_run_duration_sec);
};

template <> struct meta<dagforge::workflow::detail::PolicyDto> {
  using T = dagforge::workflow::detail::PolicyDto;
  static constexpr auto value = object(
      "allow_shell", &T::allow_shell, "allow_docker", &T::allow_docker,
      "allow_lua", &T::allow_lua, "allow_network", &T::allow_network,
      "allow_model_calls", &T::allow_model_calls, "allow_tools",
      &T::allow_tools, "require_approval_for_shell",
      &T::require_approval_for_shell, "allowed_http_hosts",
      &T::allowed_http_hosts, "allowed_model_providers",
      &T::allowed_model_providers, "allowed_tools", &T::allowed_tools,
      "budget", &T::budget);
};

template <> struct meta<dagforge::workflow::detail::InputDto> {
  using T = dagforge::workflow::detail::InputDto;
  static constexpr auto value = object("input", &T::input, "node", &T::node,
                                       "port", &T::port);
};

template <> struct meta<dagforge::workflow::detail::NodeDto> {
  using T = dagforge::workflow::detail::NodeDto;
  static constexpr auto value = object(
      "id", &T::id, "name", &T::name, "type", &T::type, "config",
      &T::config, "inputs", &T::inputs, "outputs", &T::outputs,
      "max_retries", &T::max_retries, "timeout_sec", &T::timeout_sec,
      "checkpoint", &T::checkpoint);
};

template <> struct meta<dagforge::workflow::detail::ConditionDto> {
  using T = dagforge::workflow::detail::ConditionDto;
  static constexpr auto value = object(
      "kind", &T::kind, "expected_bool", &T::expected_bool,
      "expected_string", &T::expected_string);
};

template <> struct meta<dagforge::workflow::detail::EdgeDto> {
  using T = dagforge::workflow::detail::EdgeDto;
  static constexpr auto value = object(
      "source_node", &T::source_node, "source_port", &T::source_port,
      "target", &T::target, "condition", &T::condition);
};

template <> struct meta<dagforge::workflow::detail::OutputDto> {
  using T = dagforge::workflow::detail::OutputDto;
  static constexpr auto value = object("node", &T::node, "port", &T::port);
};

template <> struct meta<dagforge::workflow::detail::WorkflowPlanDto> {
  using T = dagforge::workflow::detail::WorkflowPlanDto;
  static constexpr auto value = object(
      "workflow_id", &T::workflow_id, "schema_version", &T::schema_version,
      "nodes", &T::nodes, "edges", &T::edges, "outputs", &T::outputs,
      "policy", &T::policy);
};
} // namespace glz

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto parse_node_type(std::string_view value) -> Result<NodeType> {
  if (value == "shell")
    return ok(NodeType::Shell);
  if (value == "docker")
    return ok(NodeType::Docker);
  if (value == "lua")
    return ok(NodeType::Lua);
  if (value == "http")
    return ok(NodeType::Http);
  if (value == "model")
    return ok(NodeType::Model);
  if (value == "tool" || value == "mcp")
    return ok(NodeType::Tool);
  if (value == "compute")
    return ok(NodeType::Compute);
  if (value == "evaluator")
    return ok(NodeType::Evaluator);
  if (value == "approval")
    return ok(NodeType::Approval);
  if (value == "noop")
    return ok(NodeType::Noop);
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto parse_condition(std::string_view value)
    -> Result<ConditionKind> {
  if (value == "always")
    return ok(ConditionKind::Always);
  if (value == "bool_equals")
    return ok(ConditionKind::BoolEquals);
  if (value == "string_equals")
    return ok(ConditionKind::StringEquals);
  if (value == "evaluation_passed")
    return ok(ConditionKind::EvaluationPassed);
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto convert(detail::WorkflowPlanDto dto)
    -> Result<WorkflowPlan> {
  if (dto.workflow_id.empty() || dto.policy.budget.max_run_duration_sec <= 0) {
    return fail(Error::InvalidArgument);
  }

  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{std::move(dto.workflow_id)};
  plan.schema_version = dto.schema_version;
  plan.policy = WorkflowPolicy{
      .allow_shell = dto.policy.allow_shell,
      .allow_docker = dto.policy.allow_docker,
      .allow_lua = dto.policy.allow_lua,
      .allow_network = dto.policy.allow_network,
      .allow_model_calls = dto.policy.allow_model_calls,
      .allow_tools = dto.policy.allow_tools,
      .require_approval_for_shell = dto.policy.require_approval_for_shell,
      .allowed_http_hosts = std::move(dto.policy.allowed_http_hosts),
      .allowed_model_providers =
          std::move(dto.policy.allowed_model_providers),
      .allowed_tools = std::move(dto.policy.allowed_tools),
      .budget = ResourceBudget{
          .max_nodes = dto.policy.budget.max_nodes,
          .max_parallel_nodes = dto.policy.budget.max_parallel_nodes,
          .max_total_output_bytes =
              dto.policy.budget.max_total_output_bytes,
          .max_model_tokens = dto.policy.budget.max_model_tokens,
          .max_run_duration =
              std::chrono::seconds(dto.policy.budget.max_run_duration_sec),
      },
  };

  plan.nodes.reserve(dto.nodes.size());
  for (auto &source : dto.nodes) {
    auto type = parse_node_type(source.type);
    if (!type || source.id.empty() || source.timeout_sec <= 0) {
      return fail(type ? Error::InvalidArgument : type.error());
    }
    NodePlan node{
        .node_id = WorkflowNodeId{std::move(source.id)},
        .name = std::move(source.name),
        .type = *type,
        .config = std::move(source.config),
        .max_retries = source.max_retries,
        .timeout = std::chrono::seconds(source.timeout_sec),
        .checkpoint = source.checkpoint,
    };
    node.inputs.reserve(source.inputs.size());
    for (auto &input : source.inputs) {
      node.inputs.push_back(InputBinding{
          .input = WorkflowPortId{std::move(input.input)},
          .source = OutputRef{
              .node_id = WorkflowNodeId{std::move(input.node)},
              .port = WorkflowPortId{std::move(input.port)},
          },
      });
    }
    node.outputs.reserve(source.outputs.size());
    for (auto &output : source.outputs) {
      node.outputs.emplace_back(std::move(output));
    }
    plan.nodes.push_back(std::move(node));
  }

  plan.edges.reserve(dto.edges.size());
  for (auto &source : dto.edges) {
    auto condition = parse_condition(source.condition.kind);
    if (!condition) {
      return fail(condition.error());
    }
    plan.edges.push_back(ConditionalEdge{
        .source = OutputRef{
            .node_id = WorkflowNodeId{std::move(source.source_node)},
            .port = WorkflowPortId{std::move(source.source_port)},
        },
        .target = WorkflowNodeId{std::move(source.target)},
        .condition = ConditionExpr{
            .kind = *condition,
            .expected_bool = source.condition.expected_bool,
            .expected_string = std::move(source.condition.expected_string),
        },
    });
  }

  plan.outputs.reserve(dto.outputs.size());
  for (auto &output : dto.outputs) {
    plan.outputs.push_back(OutputRef{
        .node_id = WorkflowNodeId{std::move(output.node)},
        .port = WorkflowPortId{std::move(output.port)},
    });
  }
  return ok(std::move(plan));
}

} // namespace

auto WorkflowPlanLoader::from_json(std::string_view text)
    -> Result<WorkflowPlan> {
  auto dto = parse_json_as<detail::WorkflowPlanDto>(text);
  if (!dto) {
    return fail(dto.error());
  }
  return convert(std::move(*dto));
}

auto WorkflowPlanLoader::from_toml(std::string_view text)
    -> Result<WorkflowPlan> {
  auto dto = toml_util::parse_toml<detail::WorkflowPlanDto>(text);
  if (!dto) {
    return fail(dto.error());
  }
  return convert(std::move(*dto));
}

WorkflowControlPlane::WorkflowControlPlane() = default;

WorkflowControlPlane::WorkflowControlPlane(PlanCompiler compiler)
    : compiler_(std::move(compiler)) {}

auto WorkflowControlPlane::register_plan(WorkflowPlan plan)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto compiled = compiler_.compile(std::move(plan));
  if (!compiled) {
    return fail(compiled.error());
  }

  std::lock_guard lock(mutex_);
  if (const auto existing = plans_by_digest_.find((*compiled)->digest);
      existing != plans_by_digest_.end()) {
    latest_by_workflow_[existing->second->workflow_id.str()] = existing->second;
    return ok(existing->second);
  }
  plans_by_id_[(*compiled)->plan_id.str()] = *compiled;
  plans_by_digest_[(*compiled)->digest] = *compiled;
  latest_by_workflow_[(*compiled)->workflow_id.str()] = *compiled;
  return ok(std::move(*compiled));
}

auto WorkflowControlPlane::get_latest(const WorkflowId &workflow_id) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  std::lock_guard lock(mutex_);
  const auto it = latest_by_workflow_.find(workflow_id.str());
  if (it == latest_by_workflow_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto WorkflowControlPlane::get_plan(const WorkflowPlanId &plan_id) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  std::lock_guard lock(mutex_);
  const auto it = plans_by_id_.find(plan_id.str());
  if (it == plans_by_id_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto WorkflowControlPlane::list_plans() const
    -> std::vector<std::shared_ptr<const ExecutionPlan>> {
  std::vector<std::shared_ptr<const ExecutionPlan>> plans;
  std::lock_guard lock(mutex_);
  plans.reserve(plans_by_id_.size());
  for (const auto &[_, plan] : plans_by_id_) {
    plans.push_back(plan);
  }
  std::ranges::sort(plans, {}, [](const auto &plan) {
    return plan->workflow_id.value();
  });
  return plans;
}

} // namespace dagforge::workflow
