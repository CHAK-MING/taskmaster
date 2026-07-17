#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <any>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

enum class FailurePolicy : std::uint8_t {
  ContinueIndependent,
  FailFast,
};

enum class ConditionKind : std::uint8_t {
  Always,
  BoolEquals,
  StringEquals,
};

} // namespace dagforge::workflow

namespace dagforge::util {

template <> struct EnumTraits<workflow::FailurePolicy> {
  using E = workflow::FailurePolicy;
  inline static constexpr std::array<EnumEntry<E>, 2> entries{{
      {"continue_independent", E::ContinueIndependent},
      {"fail_fast", E::FailFast},
  }};
  static_assert(enum_entries_are_valid(entries));
};

template <> struct EnumTraits<workflow::ConditionKind> {
  using E = workflow::ConditionKind;
  inline static constexpr std::array<EnumEntry<E>, 3> entries{{
      {"always", E::Always},
      {"bool_equals", E::BoolEquals},
      {"string_equals", E::StringEquals},
  }};
  static_assert(enum_entries_are_valid(entries));
};

} // namespace dagforge::util

namespace glz {

template <> struct meta<dagforge::workflow::FailurePolicy> {
  using E = dagforge::workflow::FailurePolicy;
  static constexpr auto keys = dagforge::util::enum_names<E>();
  static constexpr auto value = dagforge::util::enum_values<E>();
};

template <> struct meta<dagforge::workflow::ConditionKind> {
  using E = dagforge::workflow::ConditionKind;
  static constexpr auto keys = dagforge::util::enum_names<E>();
  static constexpr auto value = dagforge::util::enum_values<E>();
};

} // namespace glz

namespace dagforge::workflow {

struct ConditionExpr {
  ConditionKind kind{ConditionKind::Always};
  bool expected_bool{true};
  std::string expected_string;
};

struct ConditionalEdge {
  OutputRef source;
  WorkflowNodeId target;
  ConditionExpr condition;
};

struct ResourceBudget {
  std::size_t max_nodes{256};
  std::size_t max_parallel_nodes{32};
  std::uint64_t max_total_output_bytes{64ULL * 1024ULL * 1024ULL};
  std::chrono::milliseconds max_run_duration{std::chrono::hours(1)};
};

struct WorkflowPolicy {
  FailurePolicy failure_policy{FailurePolicy::ContinueIndependent};
  ResourceBudget budget;
};

struct NodePlan {
  WorkflowNodeId node_id;
  std::string name;
  std::string executor;
  JsonPayload config;
  std::vector<InputBinding> inputs;
  std::vector<WorkflowPortId> outputs;
  int max_retries{0};
  std::chrono::milliseconds retry_initial_delay{std::chrono::seconds(1)};
  std::chrono::milliseconds retry_max_delay{std::chrono::seconds(30)};
  std::chrono::seconds timeout{std::chrono::minutes(5)};
  bool checkpoint{false};
};

struct WorkflowPlan {
  WorkflowId workflow_id;
  std::uint32_t schema_version{1};
  std::vector<NodePlan> nodes;
  std::vector<ConditionalEdge> edges;
  std::vector<OutputRef> outputs;
  WorkflowPolicy policy;
};

class CompiledExecutorConfig {
public:
  CompiledExecutorConfig() = default;

  [[nodiscard]] static auto from_encoded(JsonPayload encoded)
      -> CompiledExecutorConfig {
    return CompiledExecutorConfig{std::move(encoded)};
  }

  template <typename T>
    requires std::copy_constructible<std::decay_t<T>>
  [[nodiscard]] static auto make(JsonPayload encoded, T &&value)
      -> CompiledExecutorConfig {
    CompiledExecutorConfig config{std::move(encoded)};
    config.value_ = std::make_shared<const std::any>(
        std::forward<T>(value));
    return config;
  }

  [[nodiscard]] auto encoded() const noexcept -> const JsonPayload & {
    return encoded_;
  }

  template <typename T> [[nodiscard]] auto get() const noexcept -> const T * {
    return value_ != nullptr ? std::any_cast<T>(value_.get()) : nullptr;
  }

private:
  explicit CompiledExecutorConfig(JsonPayload encoded)
      : encoded_(std::move(encoded)) {}

  JsonPayload encoded_;
  std::shared_ptr<const std::any> value_;
};

struct CompiledNode {
  std::size_t index{0};
  NodePlan plan;
  CompiledExecutorConfig executor_config;
  std::vector<std::size_t> dependencies;
  std::vector<std::size_t> dependents;
};

struct ExecutionPlan {
  WorkflowPlanId plan_id;
  WorkflowId workflow_id;
  std::string digest;
  std::vector<CompiledNode> nodes;
  std::vector<ConditionalEdge> edges;
  std::vector<std::size_t> topological_order;
  std::vector<OutputRef> outputs;
  WorkflowPolicy policy;
};

[[nodiscard]] inline auto source_plan(const ExecutionPlan &execution)
    -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = execution.workflow_id.clone();
  plan.nodes.reserve(execution.nodes.size());
  for (const auto &compiled : execution.nodes) {
    plan.nodes.push_back(compiled.plan);
  }
  plan.edges = execution.edges;
  plan.outputs = execution.outputs;
  plan.policy = execution.policy;
  return plan;
}

[[nodiscard]] constexpr auto to_string_view(FailurePolicy value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

[[nodiscard]] constexpr auto to_string_view(ConditionKind value) noexcept
    -> std::string_view {
  return util::enum_to_string_view(value);
}

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::OutputRef> {
  using T = dagforge::workflow::OutputRef;
  static constexpr auto modify = object("node", &T::node_id);
};

template <> struct meta<dagforge::workflow::InputBinding> {
  using T = dagforge::workflow::InputBinding;

  static constexpr auto read_node = [](T &binding,
                                       dagforge::WorkflowNodeId node_id) {
    binding.source.node_id = std::move(node_id);
  };
  static constexpr auto write_node =
      [](const T &binding) -> const dagforge::WorkflowNodeId & {
    return binding.source.node_id;
  };
  static constexpr auto read_port = [](T &binding,
                                       dagforge::WorkflowPortId port) {
    binding.source.port = std::move(port);
  };
  static constexpr auto write_port =
      [](const T &binding) -> const dagforge::WorkflowPortId & {
    return binding.source.port;
  };

  static constexpr auto value =
      object("input", &T::input, "node", custom<read_node, write_node>,
             "port", custom<read_port, write_port>);
};

template <> struct meta<dagforge::workflow::ConditionalEdge> {
  using T = dagforge::workflow::ConditionalEdge;

  static constexpr auto read_source_node =
      [](T &edge, dagforge::WorkflowNodeId node_id) {
    edge.source.node_id = std::move(node_id);
  };
  static constexpr auto write_source_node =
      [](const T &edge) -> const dagforge::WorkflowNodeId & {
    return edge.source.node_id;
  };
  static constexpr auto read_source_port =
      [](T &edge, dagforge::WorkflowPortId port) {
    edge.source.port = std::move(port);
  };
  static constexpr auto write_source_port =
      [](const T &edge) -> const dagforge::WorkflowPortId & {
    return edge.source.port;
  };

  static constexpr auto value = object(
      "source_node", custom<read_source_node, write_source_node>,
      "source_port", custom<read_source_port, write_source_port>, "target",
      &T::target, "condition", &T::condition);
};

template <> struct meta<dagforge::workflow::ResourceBudget> {
  using T = dagforge::workflow::ResourceBudget;

  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    return key == "max_run_duration" ? "max_run_duration_sec" : key;
  }

  static constexpr auto read_max_run_duration =
      [](T &budget, std::chrono::seconds duration) {
    budget.max_run_duration = duration;
  };
  static constexpr auto write_max_run_duration =
      [](const T &budget) -> std::chrono::seconds {
    return std::chrono::duration_cast<std::chrono::seconds>(
        budget.max_run_duration);
  };

  static constexpr auto modify =
      object("max_run_duration_sec",
             custom<read_max_run_duration, write_max_run_duration>);
};

template <> struct meta<dagforge::workflow::NodePlan> {
  using T = dagforge::workflow::NodePlan;
  static constexpr auto modify = object(
      "id", &T::node_id, "retry_initial_delay_ms",
      &T::retry_initial_delay, "retry_max_delay_ms", &T::retry_max_delay,
      "timeout_sec", &T::timeout);
};

} // namespace glz
