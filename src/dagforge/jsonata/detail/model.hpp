#pragma once

#include "dagforge/jsonata/program.hpp"

#include "unicode.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::jsonata::detail {

template <typename T> using Result = DiagnosticResult<T>;

struct Undefined {};
struct Value;
struct Array;
struct Object;
struct Sequence;
struct Function;
struct RegexValue;
struct RegexProgram;
struct Environment;
struct FunctionSignature;
struct ProgramData;

struct ValueFootprint {
  std::size_t nodes{1};
  std::size_t string_bytes{};
  std::size_t peak_sequence_items{};
};

struct Array {
  std::vector<Value> values;
  bool constructed{true};
  ValueFootprint footprint;
};

struct Object {
  std::vector<std::pair<std::string, Value>> members;
  ValueFootprint footprint;
};

struct Sequence {
  std::vector<Value> values;
  bool keep_singleton{false};
  ValueFootprint footprint;
};

struct Value {
  using Storage =
      std::variant<Undefined, std::nullptr_t, bool, double, std::string,
                   std::shared_ptr<Array>, std::shared_ptr<Object>,
                   std::shared_ptr<Sequence>, std::shared_ptr<Function>,
                   std::shared_ptr<RegexValue>>;

  Storage storage{Undefined{}};

  Value() = default;
  explicit Value(Undefined value) : storage(value) {}
  explicit Value(std::nullptr_t value) : storage(value) {}
  explicit Value(bool value) : storage(value) {}
  explicit Value(double value) : storage(value) {}
  explicit Value(std::string value) : storage(std::move(value)) {}
  explicit Value(std::shared_ptr<Array> value) : storage(std::move(value)) {}
  explicit Value(std::shared_ptr<Object> value) : storage(std::move(value)) {}
  explicit Value(std::shared_ptr<Sequence> value) : storage(std::move(value)) {}
  explicit Value(std::shared_ptr<Function> value) : storage(std::move(value)) {}
  explicit Value(std::shared_ptr<RegexValue> value)
      : storage(std::move(value)) {}
};

[[nodiscard]] inline auto undefined() -> Value { return Value{Undefined{}}; }

[[nodiscard]] inline auto is_undefined(const Value &value) -> bool {
  return std::holds_alternative<Undefined>(value.storage);
}

[[nodiscard]] inline auto is_sequence(const Value &value) -> bool {
  const auto *sequence = std::get_if<std::shared_ptr<Sequence>>(&value.storage);
  return sequence != nullptr && *sequence != nullptr;
}

[[nodiscard]] auto value_footprint(const Value &value) noexcept
    -> ValueFootprint;
auto recompute_footprint(Array &array) noexcept -> void;
auto recompute_footprint(Object &object) noexcept -> void;
auto recompute_footprint(Sequence &sequence) noexcept -> void;

[[nodiscard]] inline auto as_sequence(const Value &value)
    -> const std::shared_ptr<Sequence> & {
  return std::get<std::shared_ptr<Sequence>>(value.storage);
}

[[nodiscard]] inline auto make_array(std::vector<Value> values = {},
                                     bool constructed = false) -> Value {
  auto array = std::make_shared<Array>(
      Array{.values = std::move(values), .constructed = constructed});
  recompute_footprint(*array);
  return Value{std::move(array)};
}

[[nodiscard]] inline auto
make_object(std::vector<std::pair<std::string, Value>> members = {}) -> Value {
  auto object = std::make_shared<Object>(Object{.members = std::move(members)});
  recompute_footprint(*object);
  return Value{std::move(object)};
}

[[nodiscard]] inline auto make_sequence(std::vector<Value> values = {},
                                        bool keep_singleton = false) -> Value {
  auto sequence = std::make_shared<Sequence>(
      Sequence{.values = std::move(values), .keep_singleton = keep_singleton});
  recompute_footprint(*sequence);
  return Value{std::move(sequence)};
}

inline auto append_flattened(std::vector<Value> &out, Value value) -> void {
  std::vector<Value> pending;
  pending.push_back(std::move(value));
  while (!pending.empty()) {
    auto current = std::move(pending.back());
    pending.pop_back();
    if (is_undefined(current)) {
      continue;
    }
    if (is_sequence(current)) {
      const auto &items = as_sequence(current)->values;
      for (auto iterator = items.rbegin(); iterator != items.rend();
           ++iterator) {
        pending.push_back(*iterator);
      }
      continue;
    }
    out.push_back(std::move(current));
  }
}

[[nodiscard]] inline auto normalize_sequence(Value value) -> Value {
  if (!is_sequence(value)) {
    return value;
  }
  const auto &sequence = as_sequence(value);
  if (sequence->values.empty()) {
    return undefined();
  }
  if (sequence->values.size() == 1 && !sequence->keep_singleton) {
    return sequence->values.front();
  }
  return value;
}

struct ByteSpan {
  std::size_t begin{};
  std::size_t end{};
};

enum class SignatureTypeKind : std::uint8_t {
  Any,
  Json,
  String,
  Number,
  Boolean,
  Null,
  Object,
  Array,
  Function,
  Choice,
};

struct SignatureType {
  SignatureTypeKind kind{SignatureTypeKind::Any};
  std::shared_ptr<const SignatureType> element;
  std::vector<SignatureType> alternatives;
};

struct SignatureParameter {
  SignatureType type;
  bool optional{false};
  bool variadic{false};
  bool context_default{false};
};

struct FunctionSignature {
  std::vector<SignatureParameter> parameters;
  SignatureType result;
};

[[nodiscard]] inline auto make_failure(FailureKind kind, std::string code,
                                       std::string message,
                                       std::string_view source,
                                       std::size_t byte_offset,
                                       std::string token = {}) -> Failure {
  return Failure{.kind = kind,
                 .code = std::move(code),
                 .message = std::move(message),
                 .byte_offset = std::min(byte_offset, source.size()),
                 .position = utf16_units(source, byte_offset),
                 .token = std::move(token)};
}

[[nodiscard]] inline auto syntax_failure(std::string code, std::string message,
                                         std::string_view source,
                                         std::size_t byte_offset,
                                         std::string token = {}) -> Failure {
  return make_failure(FailureKind::Syntax, std::move(code), std::move(message),
                      source, byte_offset, std::move(token));
}

[[nodiscard]] inline auto type_failure(std::string code, std::string message,
                                       std::string_view source,
                                       std::size_t byte_offset,
                                       std::string token = {}) -> Failure {
  return make_failure(FailureKind::Type, std::move(code), std::move(message),
                      source, byte_offset, std::move(token));
}

[[nodiscard]] inline auto dynamic_failure(std::string code, std::string message,
                                          std::string_view source,
                                          std::size_t byte_offset,
                                          std::string token = {}) -> Failure {
  return make_failure(FailureKind::Dynamic, std::move(code), std::move(message),
                      source, byte_offset, std::move(token));
}

[[nodiscard]] inline auto host_failure(std::string code, std::string message,
                                       std::string_view source,
                                       std::size_t byte_offset = 0) -> Failure {
  return make_failure(FailureKind::Host, std::move(code), std::move(message),
                      source, byte_offset);
}

using NodeId = std::uint32_t;
inline constexpr NodeId kInvalidNode = std::numeric_limits<NodeId>::max();

enum class NodeKind : std::uint8_t {
  Undefined,
  Literal,
  Name,
  Variable,
  Regex,
  Wildcard,
  Descendant,
  Parent,
  Placeholder,
  Unary,
  Binary,
  Conditional,
  Array,
  Object,
  Block,
  Bind,
  Lambda,
  Call,
  Filter,
  Sort,
  IndexBind,
  FocusBind,
  Group,
  Path,
  Transform,
};

using Literal = std::variant<std::nullptr_t, bool, double, std::string>;

enum class PathStageKind : std::uint8_t {
  Filter,
  Index,
};

struct PathStage {
  PathStageKind kind{PathStageKind::Filter};
  NodeId expression{kInvalidNode};
  std::string name;
  ByteSpan span;
};

struct PathStep {
  NodeId expression{kInvalidNode};
  std::vector<PathStage> stages;
  std::optional<std::string> focus_binding;
  std::optional<std::string> index_binding;
  bool keep_array{false};
};

struct Node {
  NodeKind kind{NodeKind::Literal};
  ByteSpan span;
  std::string text;
  Literal literal{nullptr};
  std::vector<NodeId> children;
  std::vector<std::pair<NodeId, NodeId>> pairs;
  std::vector<bool> flags;
  std::vector<PathStep> path_steps;
  bool keep_singleton_array{false};
  bool tuple_path{false};
  bool tail_call{false};
  std::shared_ptr<const FunctionSignature> signature;
};

struct ProgramData {
  std::string source;
  std::vector<Node> nodes;
  NodeId root{kInvalidNode};
  CompileLimits compile_limits;
};

struct Environment {
  std::unordered_map<std::string, Value> bindings;
  std::shared_ptr<const Environment> parent;
  std::shared_ptr<const std::vector<Value>> ancestors;

  [[nodiscard]] auto lookup(std::string_view name) const
      -> std::optional<Value> {
    if (const auto found = bindings.find(std::string{name});
        found != bindings.end()) {
      return found->second;
    }
    return parent ? parent->lookup(name) : std::nullopt;
  }

  [[nodiscard]] auto lookup_ancestor(std::size_t depth) const
      -> std::optional<Value> {
    if (ancestors && depth > 0 && depth <= ancestors->size()) {
      return (*ancestors)[ancestors->size() - depth];
    }
    return parent ? parent->lookup_ancestor(depth) : std::nullopt;
  }
};

enum class FunctionKind : std::uint8_t {
  Builtin,
  Lambda,
  Partial,
  Composition,
  Transform,
};

struct Function {
  FunctionKind kind{FunctionKind::Builtin};
  std::string name;
  std::size_t arity{};
  NodeId body{kInvalidNode};
  std::vector<std::string> parameters;
  std::shared_ptr<const Environment> closure;
  Value captured_input;
  std::shared_ptr<Function> target;
  std::vector<std::optional<Value>> partial_arguments;
  std::vector<Value> composition;
  std::vector<NodeId> transform_nodes;
  std::shared_ptr<const FunctionSignature> signature;
  const ProgramData *program{};
  std::shared_ptr<const ProgramData> program_owner;
};

struct RegexValue {
  std::string pattern;
  std::string flags;
  std::shared_ptr<const RegexProgram> program;
};

[[nodiscard]] inline auto object_lookup(const Object &object,
                                        std::string_view key)
    -> std::optional<Value> {
  for (auto it = object.members.rbegin(); it != object.members.rend(); ++it) {
    if (it->first == key) {
      return it->second;
    }
  }
  return std::nullopt;
}

inline auto object_set(Object &object, std::string key, Value value) -> void {
  for (auto &member : object.members) {
    if (member.first == key) {
      member.second = std::move(value);
      recompute_footprint(object);
      return;
    }
  }
  object.members.emplace_back(std::move(key), std::move(value));
  recompute_footprint(object);
}

inline auto object_erase(Object &object, std::string_view key) -> void {
  std::erase_if(object.members,
                [key](const auto &member) { return member.first == key; });
  recompute_footprint(object);
}

[[nodiscard]] auto to_json(const Value &value, std::string_view source)
    -> Result<std::optional<JsonValue>>;
[[nodiscard]] auto effective_boolean(const Value &value) -> bool;
[[nodiscard]] auto value_equal(const Value &left, const Value &right) -> bool;
[[nodiscard]] auto value_to_string(const Value &value, bool prettify = false)
    -> std::string;
[[nodiscard]] auto runtime_type(const Value &value) -> std::string_view;
[[nodiscard]] auto parse_function_signature(std::string_view signature,
                                            std::string_view source,
                                            std::size_t byte_offset)
    -> Result<std::shared_ptr<const FunctionSignature>>;
[[nodiscard]] auto validate_function_arguments(
    const FunctionSignature &signature, std::span<const Value> arguments,
    const Value &context, std::string_view source, std::size_t byte_offset)
    -> Result<std::vector<Value>>;

} // namespace dagforge::jsonata::detail
