#include "dagforge/workflow/workflow_control_plane.hpp"

#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/node_configs.hpp"

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
      "id", &T::id, "name", &T::name, "config", &T::config, "inputs",
      &T::inputs, "outputs", &T::outputs,
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

struct ExtractedToml {
  std::string plan;
  std::vector<std::string> node_configs;
};

[[nodiscard]] auto trim(std::string_view value) -> std::string_view {
  const auto first = value.find_first_not_of(" \t\r");
  if (first == std::string_view::npos) {
    return {};
  }
  return value.substr(first, value.find_last_not_of(" \t\r") - first + 1);
}

[[nodiscard]] auto header_without_comment(std::string_view line)
    -> std::string_view {
  line = trim(line);
  if (const auto comment = line.find('#'); comment != std::string_view::npos) {
    line = trim(line.substr(0, comment));
  }
  return line;
}

[[nodiscard]] auto extract_node_configs(std::string_view text)
    -> Result<ExtractedToml> {
  ExtractedToml extracted;
  extracted.plan.reserve(text.size());

  std::vector<bool> config_seen;
  std::size_t current_node = std::string::npos;
  bool in_config = false;

  std::size_t offset = 0;
  while (offset < text.size()) {
    const auto newline = text.find('\n', offset);
    const auto length = newline == std::string_view::npos
                            ? text.size() - offset
                            : newline - offset;
    const auto line = text.substr(offset, length);
    const auto header = header_without_comment(line);

    if (in_config && header.starts_with('[')) {
      in_config = false;
    }

    if (!in_config && header == "[[nodes]]") {
      current_node = extracted.node_configs.size();
      extracted.node_configs.emplace_back();
      config_seen.push_back(false);
      extracted.plan.append(line);
      extracted.plan.push_back('\n');
    } else if (!in_config && header == "[nodes.config]") {
      if (current_node == std::string::npos || config_seen[current_node]) {
        return fail(Error::ParseError);
      }
      config_seen[current_node] = true;
      in_config = true;
    } else if (!in_config &&
               (header.starts_with("[nodes.config.") ||
                header.starts_with("[[nodes.config."))) {
      return fail(Error::ParseError);
    } else if (in_config) {
      extracted.node_configs[current_node].append(line);
      extracted.node_configs[current_node].push_back('\n');
    } else {
      extracted.plan.append(line);
      extracted.plan.push_back('\n');
    }

    if (newline == std::string_view::npos) {
      break;
    }
    offset = newline + 1;
  }
  return ok(std::move(extracted));
}

template <typename T>
[[nodiscard]] auto parse_typed_node_config(std::string_view text)
    -> Result<JsonValue> {
  auto config = toml_util::parse_toml<T>(text);
  if (!config) {
    return fail(config.error());
  }
  auto encoded = serialize_json(*config);
  if (!encoded) {
    return fail(encoded.error());
  }
  return parse_json(*encoded);
}

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
    auto command = parse_json_as<CommandNodeConfig>(dump_json(source.config));
    if (!command || source.id.empty() || source.timeout_sec <= 0 ||
        source.retry_initial_delay_ms < 0 || source.retry_max_delay_ms < 0 ||
        source.retry_max_delay_ms < source.retry_initial_delay_ms) {
      return fail(command ? Error::InvalidArgument : command.error());
    }
    NodePlan node{
        .node_id = WorkflowNodeId{std::move(source.id)},
        .name = std::move(source.name),
        .command = std::move(*command),
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

auto WorkflowPlanLoader::from_toml(std::string_view text)
    -> Result<WorkflowPlan> {
  auto extracted = extract_node_configs(text);
  if (!extracted) {
    return fail(extracted.error());
  }
  auto dto = toml_util::parse_toml<detail::WorkflowPlanDto>(extracted->plan);
  if (!dto) {
    return fail(dto.error());
  }
  if (dto->nodes.size() != extracted->node_configs.size()) {
    return fail(Error::ParseError);
  }
  for (std::size_t index = 0; index < dto->nodes.size(); ++index) {
    auto config = parse_typed_node_config<CommandNodeConfig>(
        extracted->node_configs[index]);
    if (!config) {
      return fail(config.error());
    }
    dto->nodes[index].config = std::move(*config);
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
    auto encoded = serialize_json(node.command);
    if (!encoded) {
      return fail(encoded.error());
    }
    auto config = parse_json(*encoded);
    if (!config) {
      return fail(config.error());
    }
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
    serialized["config"] = std::move(*config);
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

WorkflowControlPlane::WorkflowControlPlane() = default;

WorkflowControlPlane::WorkflowControlPlane(PlanCompiler compiler,
                                           AdmissionPolicy admission)
    : compiler_(std::move(compiler)), admission_(std::move(admission)) {}

auto WorkflowControlPlane::register_plan(WorkflowPlan plan)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto admitted = admission_.validate(plan);
  if (!admitted) {
    return fail(admitted.error());
  }
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

auto WorkflowControlPlane::restore_plan(WorkflowPlan plan,
                                        const WorkflowPlanId &plan_id)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto admitted = admission_.validate(plan);
  if (!admitted) {
    return fail(admitted.error());
  }
  auto compiled = compiler_.compile(std::move(plan), plan_id);
  if (!compiled) {
    return fail(compiled.error());
  }
  std::lock_guard lock(mutex_);
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
