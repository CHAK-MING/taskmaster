#include "dagforge/workflow/workflow_plan_loader.hpp"

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
  int max_run_duration_sec{3600};
};

struct PolicyDto {
  std::string failure_policy{"continue_independent"};
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
  std::string executor;
  JsonValue config{JsonValue::object_t{}};
  std::vector<InputDto> inputs;
  std::vector<std::string> outputs;
  int max_retries{0};
  int retry_initial_delay_ms{1000};
  int retry_max_delay_ms{30000};
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
      &T::max_total_output_bytes, "max_run_duration_sec",
      &T::max_run_duration_sec);
};

template <> struct meta<dagforge::workflow::detail::PolicyDto> {
  using T = dagforge::workflow::detail::PolicyDto;
  static constexpr auto value = object(
      "failure_policy", &T::failure_policy, "budget", &T::budget);
};

template <> struct meta<dagforge::workflow::detail::InputDto> {
  using T = dagforge::workflow::detail::InputDto;
  static constexpr auto value = object("input", &T::input, "node", &T::node,
                                       "port", &T::port);
};

template <> struct meta<dagforge::workflow::detail::NodeDto> {
  using T = dagforge::workflow::detail::NodeDto;
  static constexpr auto value = object(
      "id", &T::id, "name", &T::name, "executor", &T::executor, "config",
      &T::config, "inputs", &T::inputs, "outputs", &T::outputs,
      "max_retries", &T::max_retries, "retry_initial_delay_ms",
      &T::retry_initial_delay_ms, "retry_max_delay_ms",
      &T::retry_max_delay_ms, "timeout_sec", &T::timeout_sec, "checkpoint",
      &T::checkpoint);
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

[[nodiscard]] auto parse_condition(std::string_view value)
    -> Result<ConditionKind> {
  if (value == "always")
    return ok(ConditionKind::Always);
  if (value == "bool_equals")
    return ok(ConditionKind::BoolEquals);
  if (value == "string_equals")
    return ok(ConditionKind::StringEquals);
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto parse_failure_policy(std::string_view value)
    -> Result<FailurePolicy> {
  if (value == "continue_independent") {
    return ok(FailurePolicy::ContinueIndependent);
  }
  if (value == "fail_fast") {
    return ok(FailurePolicy::FailFast);
  }
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto convert(detail::WorkflowPlanDto dto)
    -> Result<WorkflowPlan> {
  auto failure_policy = parse_failure_policy(dto.policy.failure_policy);
  if (dto.workflow_id.empty() || dto.policy.budget.max_run_duration_sec <= 0 ||
      !failure_policy) {
    return fail(Error::InvalidArgument);
  }

  WorkflowPlan plan;
  plan.workflow_id = WorkflowId{std::move(dto.workflow_id)};
  plan.schema_version = dto.schema_version;
  plan.policy = WorkflowPolicy{
      .failure_policy = *failure_policy,
      .budget = ResourceBudget{
          .max_nodes = dto.policy.budget.max_nodes,
          .max_parallel_nodes = dto.policy.budget.max_parallel_nodes,
          .max_total_output_bytes =
              dto.policy.budget.max_total_output_bytes,
          .max_run_duration =
              std::chrono::seconds(dto.policy.budget.max_run_duration_sec),
      },
  };

  plan.nodes.reserve(dto.nodes.size());
  for (auto &source : dto.nodes) {
    if (source.id.empty() || source.executor.empty() || source.timeout_sec <= 0 ||
        source.retry_initial_delay_ms < 0 || source.retry_max_delay_ms < 0 ||
        source.retry_max_delay_ms < source.retry_initial_delay_ms) {
      return fail(Error::InvalidArgument);
    }
    NodePlan node{
        .node_id = WorkflowNodeId{std::move(source.id)},
        .name = std::move(source.name),
        .executor = std::move(source.executor),
        .config = std::move(source.config),
        .max_retries = source.max_retries,
        .retry_initial_delay =
            std::chrono::milliseconds(source.retry_initial_delay_ms),
        .retry_max_delay =
            std::chrono::milliseconds(source.retry_max_delay_ms),
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

auto WorkflowPlanLoader::to_json(const WorkflowPlan &plan)
    -> Result<std::string> {
  JsonValue root = JsonValue::object_t{};
  root["workflow_id"] = plan.workflow_id.str();
  root["schema_version"] = static_cast<std::int64_t>(plan.schema_version);

  JsonValue nodes = JsonValue::array_t{};
  for (const auto &node : plan.nodes) {
    JsonValue inputs = JsonValue::array_t{};
    for (const auto &input : node.inputs) {
      JsonValue binding = JsonValue::object_t{};
      binding["input"] = input.input.str();
      binding["node"] = input.source.node_id.str();
      binding["port"] = input.source.port.str();
      inputs.get_array().push_back(std::move(binding));
    }
    JsonValue outputs = JsonValue::array_t{};
    for (const auto &output : node.outputs) {
      outputs.get_array().push_back(output.str());
    }
    JsonValue serialized = JsonValue::object_t{};
    serialized["id"] = node.node_id.str();
    serialized["name"] = node.name;
    serialized["executor"] = node.executor;
    serialized["config"] = node.config;
    serialized["inputs"] = std::move(inputs);
    serialized["outputs"] = std::move(outputs);
    serialized["max_retries"] =
        static_cast<std::int64_t>(node.max_retries);
    serialized["retry_initial_delay_ms"] =
        static_cast<std::int64_t>(node.retry_initial_delay.count());
    serialized["retry_max_delay_ms"] =
        static_cast<std::int64_t>(node.retry_max_delay.count());
    serialized["timeout_sec"] =
        static_cast<std::int64_t>(node.timeout.count());
    serialized["checkpoint"] = node.checkpoint;
    nodes.get_array().push_back(std::move(serialized));
  }
  root["nodes"] = std::move(nodes);

  JsonValue edges = JsonValue::array_t{};
  for (const auto &edge : plan.edges) {
    JsonValue condition = JsonValue::object_t{};
    condition["kind"] =
        std::string{to_string_view(edge.condition.kind)};
    condition["expected_bool"] = edge.condition.expected_bool;
    condition["expected_string"] = edge.condition.expected_string;

    JsonValue serialized = JsonValue::object_t{};
    serialized["source_node"] = edge.source.node_id.str();
    serialized["source_port"] = edge.source.port.str();
    serialized["target"] = edge.target.str();
    serialized["condition"] = std::move(condition);
    edges.get_array().push_back(std::move(serialized));
  }
  root["edges"] = std::move(edges);

  JsonValue outputs = JsonValue::array_t{};
  for (const auto &output : plan.outputs) {
    JsonValue serialized = JsonValue::object_t{};
    serialized["node"] = output.node_id.str();
    serialized["port"] = output.port.str();
    outputs.get_array().push_back(std::move(serialized));
  }
  root["outputs"] = std::move(outputs);
  JsonValue budget = JsonValue::object_t{};
  budget["max_nodes"] =
      static_cast<std::int64_t>(plan.policy.budget.max_nodes);
  budget["max_parallel_nodes"] =
      static_cast<std::int64_t>(plan.policy.budget.max_parallel_nodes);
  budget["max_total_output_bytes"] = static_cast<std::int64_t>(
      plan.policy.budget.max_total_output_bytes);
  budget["max_run_duration_sec"] = static_cast<std::int64_t>(
      std::chrono::duration_cast<std::chrono::seconds>(
          plan.policy.budget.max_run_duration)
          .count());

  JsonValue policy = JsonValue::object_t{};
  policy["failure_policy"] =
      std::string{to_string_view(plan.policy.failure_policy)};
  policy["budget"] = std::move(budget);
  root["policy"] = std::move(policy);
  return serialize_json(root);
}

} // namespace dagforge::workflow
