#pragma once

#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"

#include <array>
#include <compare>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::workflow {

struct Principal {
  std::string subject;
  std::vector<std::string> roles;
};

struct TraceContext {
  std::string trace_id;
  std::string parent_span_id;
};

struct ArtifactRef {
  ArtifactId artifact_id;
  std::string media_type{"application/octet-stream"};
  std::uint64_t size_bytes{0};
  std::string digest;
};

using WorkflowValue =
    std::variant<std::monostate, bool, std::int64_t, double, std::string,
                 JsonPayload, ArtifactRef>;

[[nodiscard]] auto workflow_value_text(const WorkflowValue &value)
    -> std::string;

namespace detail {

struct NullWorkflowValue {};
struct BoolWorkflowValue {
  bool value{false};
};
struct IntegerWorkflowValue {
  std::int64_t value{0};
};
struct NumberWorkflowValue {
  double value{0.0};
};
struct StringWorkflowValue {
  std::string value;
};
struct JsonWorkflowValue {
  JsonPayload value;
};
struct ArtifactWorkflowValue {
  ArtifactRef value;
};

using WorkflowValueWire =
    std::variant<NullWorkflowValue, BoolWorkflowValue, IntegerWorkflowValue,
                 NumberWorkflowValue, StringWorkflowValue, JsonWorkflowValue,
                 ArtifactWorkflowValue>;

[[nodiscard]] inline auto to_workflow_value_wire(const WorkflowValue &value)
    -> WorkflowValueWire {
  return std::visit(
      [](const auto &item) -> WorkflowValueWire {
        using T = std::remove_cvref_t<decltype(item)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return NullWorkflowValue{};
        } else if constexpr (std::is_same_v<T, bool>) {
          return BoolWorkflowValue{item};
        } else if constexpr (std::is_same_v<T, std::int64_t>) {
          return IntegerWorkflowValue{item};
        } else if constexpr (std::is_same_v<T, double>) {
          return NumberWorkflowValue{item};
        } else if constexpr (std::is_same_v<T, std::string>) {
          return StringWorkflowValue{item};
        } else if constexpr (std::is_same_v<T, dagforge::JsonPayload>) {
          return JsonWorkflowValue{item};
        } else {
          return ArtifactWorkflowValue{item};
        }
      },
      value);
}

[[nodiscard]] inline auto from_workflow_value_wire(WorkflowValueWire wire)
    -> WorkflowValue {
  return std::visit(
      [](auto &&item) -> WorkflowValue {
        using T = std::remove_cvref_t<decltype(item)>;
        if constexpr (std::is_same_v<T, NullWorkflowValue>) {
          return WorkflowValue{std::in_place_index<0>};
        } else if constexpr (std::is_same_v<T, BoolWorkflowValue>) {
          return WorkflowValue{std::in_place_index<1>, item.value};
        } else if constexpr (std::is_same_v<T, IntegerWorkflowValue>) {
          return WorkflowValue{std::in_place_index<2>, item.value};
        } else if constexpr (std::is_same_v<T, NumberWorkflowValue>) {
          return WorkflowValue{std::in_place_index<3>, item.value};
        } else if constexpr (std::is_same_v<T, StringWorkflowValue>) {
          return WorkflowValue{std::in_place_index<4>, std::move(item.value)};
        } else if constexpr (std::is_same_v<T, JsonWorkflowValue>) {
          return WorkflowValue{std::in_place_index<5>, std::move(item.value)};
        } else {
          return WorkflowValue{std::in_place_index<6>, std::move(item.value)};
        }
      },
      std::move(wire));
}

} // namespace detail

struct OutputRef {
  WorkflowNodeId node_id;
  WorkflowPortId port;

  auto operator<=>(const OutputRef &) const = default;
  auto operator==(const OutputRef &) const -> bool = default;
};

struct OutputValue {
  OutputRef output;
  WorkflowValue value;

  OutputValue() = default;
  OutputValue(OutputRef output, WorkflowValue value)
      : output(std::move(output)), value(std::move(value)) {}
};

struct OutputRefHash {
  [[nodiscard]] auto operator()(const OutputRef &output) const noexcept
      -> std::size_t {
    const auto node_hash = std::hash<WorkflowNodeId>{}(output.node_id);
    const auto port_hash = std::hash<WorkflowPortId>{}(output.port);
    return node_hash ^ (port_hash + 0x9e3779b97f4a7c15ULL +
                        (node_hash << 6U) + (node_hash >> 2U));
  }
};

struct InputBinding {
  WorkflowPortId input;
  OutputRef source;
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::detail::WorkflowValueWire> {
  static constexpr std::string_view tag = "type";
  static constexpr std::array<std::string_view, 7> ids{
      "null", "bool", "integer", "number", "string", "json", "artifact"};
};

template <> struct meta<dagforge::workflow::OutputValue> {
  using T = dagforge::workflow::OutputValue;
  static constexpr auto value =
      object("output", &T::output, "value", &T::value);
};

template <> struct to<JSON, dagforge::workflow::WorkflowValue> {
  template <auto Opts, class B>
  static void op(const dagforge::workflow::WorkflowValue &value,
                 is_context auto &&ctx, B &&buffer, auto &index) {
    auto wire = dagforge::workflow::detail::to_workflow_value_wire(value);
    to<JSON, dagforge::workflow::detail::WorkflowValueWire>::template op<Opts>(
        wire, ctx, std::forward<B>(buffer), index);
  }
};

template <> struct from<JSON, dagforge::workflow::WorkflowValue> {
  template <auto Opts>
  static void op(dagforge::workflow::WorkflowValue &value,
                 is_context auto &&ctx, auto &&it, auto end) {
    dagforge::workflow::detail::WorkflowValueWire wire;
    parse<JSON>::op<Opts>(wire, ctx, std::forward<decltype(it)>(it), end);
    value = dagforge::workflow::detail::from_workflow_value_wire(
        std::move(wire));
  }
};

} // namespace glz
