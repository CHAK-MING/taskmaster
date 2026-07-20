#include "evaluator.hpp"

#include "dagforge/core/scope_exit.hpp"
#include "json_adapter.hpp"
#include "regex_adapter.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <format>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

inline constexpr std::size_t kEnvironmentPruneInterval = 1024;

[[nodiscard]] auto is_callable(const Value &raw) noexcept -> bool {
  const auto value = normalize_sequence(raw);
  if (const auto *function =
          std::get_if<std::shared_ptr<Function>>(&value.storage)) {
    return *function != nullptr;
  }
  if (const auto *regex =
          std::get_if<std::shared_ptr<RegexValue>>(&value.storage)) {
    return *regex != nullptr;
  }
  return false;
}

[[nodiscard]] auto regex_callable_result(const RegexMatch &match,
                                         std::string_view input) -> Value {
  std::vector<Value> groups;
  groups.reserve(match.groups.size());
  for (const auto &group : match.groups) {
    groups.push_back(group.matched ? Value{group.text} : undefined());
  }
  return make_object({
      {"match", Value{match.text}},
      {"start", Value{static_cast<double>(utf16_units(input, match.start))}},
      {"end", Value{static_cast<double>(utf16_units(input, match.end))}},
      {"groups", make_array(std::move(groups))},
  });
}

auto set_tuple_binding(PathTuple &tuple, std::string name, Value value)
    -> void {
  for (auto &binding : tuple.bindings) {
    if (binding.first == name) {
      binding.second = std::move(value);
      return;
    }
  }
  tuple.bindings.emplace_back(std::move(name), std::move(value));
}

[[nodiscard]] auto
inherited_ancestors(const std::shared_ptr<Environment> &environment)
    -> std::vector<Value> {
  for (std::shared_ptr<const Environment> current = environment; current;
       current = current->parent) {
    if (current->ancestors) {
      return *current->ancestors;
    }
  }
  return {};
}

[[nodiscard]] auto creates_ancestor(const Node &node) noexcept -> bool {
  return node.kind == NodeKind::Name || node.kind == NodeKind::Wildcard ||
         node.kind == NodeKind::Descendant;
}

} // namespace

Evaluator::Evaluator(const ProgramData &program,
                     const EvaluationRequest &request)
    : program_(program), state_(std::make_shared<EvaluationState>(
                             EvaluationState{.request = request})) {
  state_->deadline = std::chrono::steady_clock::now() + request.limits.timeout;
  const auto seed =
      static_cast<std::uint64_t>(request.timestamp.time_since_epoch().count()) ^
      static_cast<std::uint64_t>(
          std::chrono::steady_clock::now().time_since_epoch().count());
  state_->random_engine.seed(seed);
  base_environment_ = make_environment();
  install_builtins(*base_environment_);
  environment_ = make_environment(base_environment_);
  const auto byte_offset = node(program_.root).span.end;
  for (const auto &binding : request.bindings) {
    if (!environment_->bindings.contains(std::string{binding.name})) {
      ++state_->environment_bindings_created;
    }
    auto imported =
        import_json(binding.value.get(), request.limits, request.stop_token,
                    state_->deadline, program_.source, byte_offset);
    if (!imported) {
      initialization_failure_ = std::move(imported.error());
      return;
    }
    environment_->bindings.insert_or_assign(std::string{binding.name},
                                            std::move(*imported));
  }
  if (request.input) {
    auto imported =
        import_json(request.input->get(), request.limits, request.stop_token,
                    state_->deadline, program_.source, byte_offset);
    if (!imported) {
      initialization_failure_ = std::move(imported.error());
      return;
    }
    root_ = std::move(*imported);
  } else {
    root_ = undefined();
  }
}

Evaluator::Evaluator(const ProgramData &program,
                     std::shared_ptr<EvaluationState> state,
                     std::size_t eval_depth,
                     std::shared_ptr<const ProgramData> program_owner)
    : program_(program), program_owner_(std::move(program_owner)),
      state_(std::move(state)), eval_depth_(eval_depth) {}

auto Evaluator::run() -> Result<EvaluationSuccess> {
  const auto release_environments =
      dagforge::scope_exit([this] { release_environment_cycles(); });
  return run_impl();
}

auto Evaluator::run_impl() -> Result<EvaluationSuccess> {
  const auto &root_node = node(program_.root);
  if (initialization_failure_) {
    return std::unexpected(*initialization_failure_);
  }
  if (state_->environment_bindings_created >
      state_->request.limits.max_environment_bindings_created) {
    return std::unexpected(host_failure(
        "H2002", "JSONata cumulative environment binding limit exceeded",
        program_.source, root_node.span.end));
  }
  auto observed_root = observe_value(root_, root_node);
  if (!observed_root) {
    return std::unexpected(observed_root.error());
  }
  for (const auto &[name, value] : environment_->bindings) {
    (void)name;
    auto observed_binding = observe_value(value, root_node);
    if (!observed_binding) {
      return std::unexpected(observed_binding.error());
    }
  }
  auto value = evaluate(program_.root, root_, environment_, 0);
  if (!value) {
    return std::unexpected(value.error());
  }
  const auto normalized = normalize_sequence(*value);
  if (is_undefined(normalized)) {
    return EvaluationSuccess{
        .kind = EvaluationValueKind::Undefined,
        .value = std::nullopt,
        .statistics = state_->statistics,
    };
  }
  if (std::holds_alternative<std::shared_ptr<Function>>(normalized.storage) ||
      std::holds_alternative<std::shared_ptr<RegexValue>>(normalized.storage)) {
    return EvaluationSuccess{
        .kind = EvaluationValueKind::Function,
        .value = std::nullopt,
        .statistics = state_->statistics,
    };
  }
  auto converted = to_json(*value, program_.source);
  if (!converted) {
    return std::unexpected(converted.error());
  }
  return EvaluationSuccess{.kind = EvaluationValueKind::Json,
                           .value = std::move(*converted),
                           .statistics = state_->statistics};
}

auto Evaluator::make_environment(std::shared_ptr<const Environment> parent)
    -> std::shared_ptr<Environment> {
  auto environment = std::make_shared<Environment>();
  environment->parent = std::move(parent);
  state_->environments.emplace_back(environment);
  if (--state_->environments_until_prune == 0) {
    std::erase_if(state_->environments,
                  [](const auto &reference) { return reference.expired(); });
    state_->environments_until_prune = kEnvironmentPruneInterval;
  }
  return environment;
}

auto Evaluator::release_environment_cycles() noexcept -> void {
  // Closures own environments so factory-returned functions remain valid
  // throughout evaluation. No callable crosses the public result boundary, so
  // the environment back-edges can be removed once run_impl() has finished.
  for (auto &environment_ref : state_->environments) {
    if (auto environment = environment_ref.lock()) {
      environment->bindings.clear();
      environment->parent.reset();
      environment->ancestors.reset();
    }
  }
  state_->environments.clear();
}

auto Evaluator::consume_step(const Node &current, std::size_t call_depth)
    -> Result<void> {
  if (++state_->statistics.steps > state_->request.limits.max_steps) {
    return std::unexpected(
        host_failure("H2001", "JSONata evaluation step limit exceeded",
                     program_.source, current.span.end));
  }
  state_->statistics.peak_call_depth =
      std::max(state_->statistics.peak_call_depth, call_depth);
  if (call_depth > state_->request.limits.max_call_depth) {
    return std::unexpected(
        dynamic_failure("D1011", "JSONata evaluation stack limit exceeded",
                        program_.source, current.span.end));
  }
  if (--state_->checks_until_clock == 0) {
    state_->checks_until_clock = 64;
    return check_interrupt(current);
  }
  return {};
}

auto Evaluator::check_interrupt(const Node &current) -> Result<void> {
  if (state_->request.stop_token.stop_requested()) {
    return std::unexpected(host_failure("H1001", "JSONata evaluation cancelled",
                                        program_.source, current.span.end));
  }
  if (state_->request.limits.timeout >
          std::chrono::steady_clock::duration::zero() &&
      std::chrono::steady_clock::now() > state_->deadline) {
    return std::unexpected(
        dynamic_failure("D1012", "JSONata evaluation timeout exceeded",
                        program_.source, current.span.end));
  }
  return {};
}

auto Evaluator::observe_value(const Value &value, const Node &current)
    -> Result<void> {
  const auto footprint = value_footprint(value);
  state_->statistics.peak_value_nodes =
      std::max(state_->statistics.peak_value_nodes, footprint.nodes);
  state_->statistics.peak_string_bytes =
      std::max(state_->statistics.peak_string_bytes, footprint.string_bytes);
  state_->statistics.peak_sequence_items = std::max(
      state_->statistics.peak_sequence_items, footprint.peak_sequence_items);
  if (footprint.nodes > state_->request.limits.max_value_nodes) {
    return std::unexpected(
        host_failure("H2100", "JSONata value graph node limit exceeded",
                     program_.source, current.span.end));
  }
  if (footprint.string_bytes > state_->request.limits.max_string_bytes) {
    return std::unexpected(
        host_failure("H2101", "JSONata value string byte limit exceeded",
                     program_.source, current.span.end));
  }
  if (footprint.peak_sequence_items >
      state_->request.limits.max_sequence_items) {
    return std::unexpected(dynamic_failure("D2015",
                                           "Maximum sequence length exceeded",
                                           program_.source, current.span.end));
  }
  return {};
}

auto Evaluator::charge_environment_bindings_created(std::size_t count,
                                                    const Node &current)
    -> Result<void> {
  const auto limit = state_->request.limits.max_environment_bindings_created;
  if (count > limit || state_->environment_bindings_created > limit - count) {
    return std::unexpected(host_failure(
        "H2002", "JSONata cumulative environment binding limit exceeded",
        program_.source, current.span.end));
  }
  state_->environment_bindings_created += count;
  return {};
}

auto Evaluator::check_path_stream_size(const PathStream &stream,
                                       const Node &current) -> Result<void> {
  state_->statistics.peak_sequence_items =
      std::max(state_->statistics.peak_sequence_items, stream.tuples.size());
  if (stream.tuples.size() > state_->request.limits.max_sequence_items) {
    return std::unexpected(dynamic_failure("D2015",
                                           "Maximum sequence length exceeded",
                                           program_.source, current.span.end));
  }
  return {};
}

auto Evaluator::ensure_path_growth(std::size_t current, std::size_t additional,
                                   const Node &node_value) -> Result<void> {
  const auto limit = state_->request.limits.max_sequence_items;
  if (additional > limit || current > limit - additional) {
    return std::unexpected(
        dynamic_failure("D2015", "Maximum sequence length exceeded",
                        program_.source, node_value.span.end));
  }
  return {};
}

auto Evaluator::evaluate(NodeId id, const Value &input,
                         const std::shared_ptr<Environment> &environment,
                         std::size_t call_depth) -> Result<Value> {
  const auto &current = node(id);
  auto budget = consume_step(current, call_depth);
  if (!budget) {
    return std::unexpected(budget.error());
  }
  auto result = [&]() -> Result<Value> {
    switch (current.kind) {
    case NodeKind::Undefined:
      return undefined();
    case NodeKind::Literal:
      return evaluate_literal(current);
    case NodeKind::Name:
      return evaluate_name(current, input);
    case NodeKind::Variable:
      return evaluate_variable(current, input, environment);
    case NodeKind::Regex:
      return evaluate_regex(current);
    case NodeKind::Wildcard:
      return evaluate_wildcard(current, input);
    case NodeKind::Descendant:
      return evaluate_descendant(current, input);
    case NodeKind::Parent:
      return environment->lookup_ancestor(1).value_or(undefined());
    case NodeKind::Placeholder:
      return undefined();
    case NodeKind::Unary:
      return evaluate_unary(current, input, environment, call_depth);
    case NodeKind::Binary:
      return evaluate_binary(current, input, environment, call_depth);
    case NodeKind::Conditional:
      return evaluate_conditional(current, input, environment, call_depth);
    case NodeKind::Array:
      return evaluate_array(current, input, environment, call_depth);
    case NodeKind::Object:
      return evaluate_object(current, input, environment, call_depth);
    case NodeKind::Block:
      return evaluate_block(current, input, environment, call_depth);
    case NodeKind::Bind:
      return evaluate_bind(current, input, environment, call_depth);
    case NodeKind::Lambda:
      return evaluate_lambda(current, input, environment);
    case NodeKind::Call:
      return evaluate_call(current, input, environment, call_depth);
    case NodeKind::Filter:
      return evaluate_filter(current, input, environment, call_depth);
    case NodeKind::Sort:
      return evaluate_sort(current, input, environment, call_depth);
    case NodeKind::IndexBind:
    case NodeKind::FocusBind:
      return std::unexpected(host_failure(
          "H9005", "Unlowered JSONata path binding reached the evaluator",
          program_.source, current.span.end));
    case NodeKind::Group:
      return evaluate_group(current, input, environment, call_depth);
    case NodeKind::Path:
      return evaluate_path(current, input, environment, call_depth);
    case NodeKind::Transform:
      return evaluate_transform(current, input, environment, call_depth);
    }
    return std::unexpected(host_failure("H9001", "Unknown JSONata syntax node",
                                        program_.source, current.span.end));
  }();
  if (!result) {
    return result;
  }
  auto observed = observe_value(*result, current);
  if (!observed) {
    return std::unexpected(observed.error());
  }
  return result;
}

auto Evaluator::evaluate_literal(const Node &current) -> Value {
  return std::visit([](const auto &literal) -> Value { return Value{literal}; },
                    current.literal);
}

auto Evaluator::evaluate_name(const Node &current, const Value &raw_input)
    -> Result<Value> {
  const auto input = normalize_sequence(raw_input);
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&input.storage)) {
    return object_lookup(**object, current.text).value_or(undefined());
  }

  std::vector<Value> result;
  std::vector<Value> pending;
  const auto push_children = [&](const auto &values) {
    pending.insert(pending.end(), values.rbegin(), values.rend());
  };
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&input.storage)) {
    push_children((*array)->values);
  } else if (is_sequence(input)) {
    push_children(as_sequence(input)->values);
  } else {
    return undefined();
  }

  const auto append_lookup_result = [&](const Value &raw) -> Result<void> {
    const auto value = normalize_sequence(raw);
    if (is_undefined(value)) {
      return {};
    }
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&value.storage)) {
      auto growth =
          ensure_path_growth(result.size(), (*array)->values.size(), current);
      if (!growth) {
        return std::unexpected(growth.error());
      }
      result.insert(result.end(), (*array)->values.begin(),
                    (*array)->values.end());
      return {};
    }
    if (is_sequence(value)) {
      auto growth = ensure_path_growth(
          result.size(), as_sequence(value)->values.size(), current);
      if (!growth) {
        return std::unexpected(growth.error());
      }
      result.insert(result.end(), as_sequence(value)->values.begin(),
                    as_sequence(value)->values.end());
      return {};
    }
    auto growth = ensure_path_growth(result.size(), 1, current);
    if (!growth) {
      return std::unexpected(growth.error());
    }
    result.push_back(value);
    return {};
  };

  while (!pending.empty()) {
    auto walk_step = consume_step(current, 0);
    if (!walk_step) {
      return std::unexpected(walk_step.error());
    }
    auto candidate = normalize_sequence(std::move(pending.back()));
    pending.pop_back();
    if (const auto *object =
            std::get_if<std::shared_ptr<Object>>(&candidate.storage)) {
      if (auto found = object_lookup(**object, current.text)) {
        auto appended = append_lookup_result(*found);
        if (!appended) {
          return std::unexpected(appended.error());
        }
      }
    } else if (const auto *array =
                   std::get_if<std::shared_ptr<Array>>(&candidate.storage)) {
      push_children((*array)->values);
    } else if (is_sequence(candidate)) {
      push_children(as_sequence(candidate)->values);
    }
  }
  if (!result.empty()) {
    return normalize_sequence(make_sequence(std::move(result)));
  }
  return undefined();
}

auto Evaluator::evaluate_variable(
    const Node &current, const Value &input,
    const std::shared_ptr<Environment> &environment) -> Value {
  if (current.text.empty()) {
    return input;
  }
  if (current.text == "$") {
    return root_;
  }
  return environment->lookup(current.text).value_or(undefined());
}

auto Evaluator::evaluate_regex(const Node &current) -> Result<Value> {
  const auto separator = current.text.rfind('\n');
  const auto pattern = current.text.substr(0, separator);
  const auto flags = separator == std::string::npos
                         ? std::string{}
                         : current.text.substr(separator + 1);
  auto compiled =
      compile_regex(pattern, flags, program_.source, current.span.begin + 1);
  if (!compiled) {
    return std::unexpected(compiled.error());
  }
  return Value{std::move(*compiled)};
}

auto Evaluator::evaluate_wildcard(const Node &current, const Value &raw_input)
    -> Result<Value> {
  const auto input = normalize_sequence(raw_input);
  std::vector<Value> result;
  std::vector<Value> pending;
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&input.storage)) {
    for (auto iterator = (*object)->members.rbegin();
         iterator != (*object)->members.rend(); ++iterator) {
      pending.push_back(iterator->second);
    }
  } else if (const auto *array =
                 std::get_if<std::shared_ptr<Array>>(&input.storage)) {
    for (auto iterator = (*array)->values.rbegin();
         iterator != (*array)->values.rend(); ++iterator) {
      pending.push_back(*iterator);
    }
  }
  while (!pending.empty()) {
    auto walk_step = consume_step(current, 0);
    if (!walk_step) {
      return std::unexpected(walk_step.error());
    }
    auto value = normalize_sequence(std::move(pending.back()));
    pending.pop_back();
    if (is_undefined(value)) {
      continue;
    }
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&value.storage)) {
      for (auto iterator = (*array)->values.rbegin();
           iterator != (*array)->values.rend(); ++iterator) {
        pending.push_back(*iterator);
      }
      continue;
    }
    if (is_sequence(value)) {
      const auto &items = as_sequence(value)->values;
      for (auto iterator = items.rbegin(); iterator != items.rend();
           ++iterator) {
        pending.push_back(*iterator);
      }
      continue;
    }
    result.push_back(std::move(value));
    if (result.size() > state_->request.limits.max_sequence_items) {
      return std::unexpected(
          dynamic_failure("D2015", "Maximum sequence length exceeded",
                          program_.source, current.span.end));
    }
  }
  return normalize_sequence(make_sequence(std::move(result)));
}

auto Evaluator::evaluate_descendant(const Node &current, const Value &input)
    -> Result<Value> {
  std::vector<Value> result;
  std::vector<Value> pending{input};
  while (!pending.empty()) {
    auto walk_step = consume_step(current, 0);
    if (!walk_step) {
      return std::unexpected(walk_step.error());
    }
    auto value = normalize_sequence(std::move(pending.back()));
    pending.pop_back();
    if (is_undefined(value)) {
      continue;
    }
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&value.storage)) {
      for (auto iterator = (*array)->values.rbegin();
           iterator != (*array)->values.rend(); ++iterator) {
        pending.push_back(*iterator);
      }
      continue;
    }
    result.push_back(value);
    if (result.size() > state_->request.limits.max_sequence_items) {
      return std::unexpected(
          dynamic_failure("D2015", "Maximum sequence length exceeded",
                          program_.source, current.span.end));
    }
    if (const auto *object =
            std::get_if<std::shared_ptr<Object>>(&value.storage)) {
      for (auto iterator = (*object)->members.rbegin();
           iterator != (*object)->members.rend(); ++iterator) {
        pending.push_back(iterator->second);
      }
    }
  }
  return normalize_sequence(make_sequence(std::move(result)));
}

auto Evaluator::evaluate_unary(const Node &current, const Value &input,
                               const std::shared_ptr<Environment> &environment,
                               std::size_t call_depth) -> Result<Value> {
  auto operand =
      evaluate(current.children.front(), input, environment, call_depth);
  if (!operand) {
    return std::unexpected(operand.error());
  }
  const auto normalized = normalize_sequence(*operand);
  if (is_undefined(normalized)) {
    return undefined();
  }
  const auto *number = std::get_if<double>(&normalized.storage);
  if (number == nullptr) {
    return std::unexpected(dynamic_failure(
        "D1002", "The unary minus operator cannot negate this value",
        program_.source, current.span.end, current.text));
  }
  return Value{-*number};
}

auto Evaluator::evaluate_binary(const Node &current, const Value &input,
                                const std::shared_ptr<Environment> &environment,
                                std::size_t call_depth) -> Result<Value> {
  const auto &op = current.text;
  if (op == ".") {
    return evaluate_path(current, input, environment, call_depth);
  }
  if (op == "and" || op == "or") {
    auto left = evaluate(current.children[0], input, environment, call_depth);
    if (!left) {
      return std::unexpected(left.error());
    }
    const auto left_truth = effective_boolean(*left);
    if ((op == "and" && !left_truth) || (op == "or" && left_truth)) {
      return Value{op == "or"};
    }
    auto right = evaluate(current.children[1], input, environment, call_depth);
    if (!right) {
      return std::unexpected(right.error());
    }
    return Value{effective_boolean(*right)};
  }
  if (op == "?:" || op == "??") {
    auto left = evaluate(current.children[0], input, environment, call_depth);
    if (!left) {
      return std::unexpected(left.error());
    }
    const auto normalized = normalize_sequence(*left);
    const bool keep =
        op == "?:" ? effective_boolean(normalized) : !is_undefined(normalized);
    return keep ? left
                : evaluate(current.children[1], input, environment, call_depth);
  }
  if (op == "~>") {
    auto left = evaluate(current.children[0], input, environment, call_depth);
    if (!left) {
      return std::unexpected(left.error());
    }
    const auto &right_node = node(current.children[1]);
    if (is_callable(*left)) {
      auto right =
          evaluate(current.children[1], input, environment, call_depth);
      if (!right) {
        return std::unexpected(right.error());
      }
      if (!is_callable(*right)) {
        return std::unexpected(type_failure(
            "T2006", "The right side of the chain operator must be a function",
            program_.source, current.span.end));
      }
      auto composition = std::make_shared<Function>();
      composition->kind = FunctionKind::Composition;
      composition->arity = function_arity(*left);
      composition->composition = {*left, *right};
      return Value{std::move(composition)};
    }
    if (right_node.kind == NodeKind::Call) {
      return evaluate_chain_call(right_node, *left, input, environment,
                                 call_depth);
    }
    if (right_node.kind == NodeKind::Filter &&
        right_node.children.size() == 1) {
      const auto &procedure = node(right_node.children.front());
      if (procedure.kind == NodeKind::Call) {
        auto chained = evaluate_chain_call(procedure, *left, input, environment,
                                           call_depth);
        if (!chained) {
          return std::unexpected(chained.error());
        }
        return make_sequence(value_list(*chained), true);
      }
    }
    if (right_node.kind == NodeKind::Path &&
        right_node.path_steps.size() == 1 && right_node.pairs.empty()) {
      const auto &step = right_node.path_steps.front();
      const auto &procedure = node(step.expression);
      if (procedure.kind == NodeKind::Call && step.stages.empty()) {
        auto chained = evaluate_chain_call(procedure, *left, input, environment,
                                           call_depth);
        if (!chained) {
          return std::unexpected(chained.error());
        }
        if (!step.keep_array) {
          return chained;
        }
        return make_sequence(value_list(*chained), true);
      }
    }
    auto function =
        evaluate(current.children[1], input, environment, call_depth);
    if (!function) {
      return std::unexpected(function.error());
    }
    if (!is_callable(*function)) {
      return std::unexpected(type_failure(
          "T2006", "The right side of the chain operator must be a function",
          program_.source, current.span.end));
    }
    return apply(*function, {*left}, input, environment, current, call_depth);
  }

  auto left_result =
      evaluate(current.children[0], input, environment, call_depth);
  if (!left_result) {
    return std::unexpected(left_result.error());
  }
  auto right_result =
      evaluate(current.children[1], input, environment, call_depth);
  if (!right_result) {
    return std::unexpected(right_result.error());
  }
  auto left = normalize_sequence(*left_result);
  auto right = normalize_sequence(*right_result);

  if (op == "=" || op == "!=") {
    if (is_undefined(left) || is_undefined(right)) {
      return Value{false};
    }
    const auto equal = value_equal(left, right);
    return Value{op == "=" ? equal : !equal};
  }
  if (op == "in") {
    bool found = false;
    for (const auto &item : value_list(right)) {
      if (value_equal(left, item)) {
        found = true;
        break;
      }
    }
    return Value{found};
  }
  if (op == "&") {
    return Value{value_to_string(left) + value_to_string(right)};
  }
  if (op == "..") {
    return evaluate_range(current, left, right);
  }
  if (op == "<" || op == "<=" || op == ">" || op == ">=") {
    return compare_values(current, left, right);
  }
  const auto *left_number = std::get_if<double>(&left.storage);
  const auto *right_number = std::get_if<double>(&right.storage);
  if (!is_undefined(left) && left_number == nullptr) {
    return std::unexpected(type_failure(
        "T2001", "The left side of an arithmetic operator must be numeric",
        program_.source, current.span.end, current.text));
  }
  if (!is_undefined(right) && right_number == nullptr) {
    return std::unexpected(type_failure(
        "T2002", "The right side of an arithmetic operator must be numeric",
        program_.source, current.span.end, current.text));
  }
  if (is_undefined(left) || is_undefined(right)) {
    return undefined();
  }
  if (!std::isfinite(*left_number) || !std::isfinite(*right_number)) {
    return std::unexpected(dynamic_failure(
        "D1001", "Number cannot be represented as a JSON number",
        program_.source, current.span.end, current.text));
  }
  double result = 0.0;
  if (op == "+") {
    result = *left_number + *right_number;
  } else if (op == "-") {
    result = *left_number - *right_number;
  } else if (op == "*") {
    result = *left_number * *right_number;
  } else if (op == "/") {
    result = *left_number / *right_number;
  } else if (op == "%") {
    result = std::fmod(*left_number, *right_number);
  } else {
    return std::unexpected(syntax_failure(
        "S0204", "Unknown operator", program_.source, current.span.end, op));
  }
  return Value{result};
}

auto Evaluator::evaluate_path(const Node &current, const Value &input,
                              const std::shared_ptr<Environment> &environment,
                              std::size_t call_depth) -> Result<Value> {
  auto stream = make_initial_path_stream(current, input, environment);
  auto evaluated =
      evaluate_path_stream(current, std::move(stream), environment, call_depth);
  if (!evaluated) {
    return std::unexpected(evaluated.error());
  }
  if (!current.pairs.empty()) {
    return group_path_stream(current, *evaluated, environment, call_depth);
  }
  return project_path_stream(std::move(*evaluated));
}

auto Evaluator::make_initial_path_stream(
    const Node &current, const Value &raw_input,
    const std::shared_ptr<Environment> &environment) -> PathStream {
  PathStream stream{.keep_singleton = current.keep_singleton_array};
  if (current.path_steps.empty()) {
    return stream;
  }
  const auto input = normalize_sequence(raw_input);
  const auto ancestors = inherited_ancestors(environment);
  stream.tuples.push_back(PathTuple{.focus = input, .ancestors = ancestors});
  return stream;
}

auto Evaluator::evaluate_path_stream(
    const Node &current, PathStream stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<PathStream> {
  stream.keep_singleton = stream.keep_singleton || current.keep_singleton_array;
  stream.tuple_mode = stream.tuple_mode || current.tuple_path;
  bool previous_focus_binding = false;
  for (std::size_t index = 0; index < current.path_steps.size(); ++index) {
    const auto &step = current.path_steps[index];
    const auto &expression = node(step.expression);
    if (expression.kind == NodeKind::Sort) {
      auto sorted = sort_path_stream(expression, std::move(stream), environment,
                                     call_depth);
      if (!sorted) {
        return std::unexpected(sorted.error());
      }
      stream = std::move(*sorted);
      if (step.index_binding) {
        for (std::size_t tuple_index = 0; tuple_index < stream.tuples.size();
             ++tuple_index) {
          set_tuple_binding(stream.tuples[tuple_index], *step.index_binding,
                            Value{static_cast<double>(tuple_index)});
        }
      }
      stream.keep_singleton = stream.keep_singleton || step.keep_array;
      auto staged =
          apply_path_stages(step, std::move(stream), environment, call_depth);
      if (!staged) {
        return std::unexpected(staged.error());
      }
      stream = std::move(*staged);
    } else {
      auto evaluated = evaluate_path_step(
          step, std::move(stream), environment, call_depth,
          index + 1 == current.path_steps.size(),
          index + 1 == current.path_steps.size() && current.pairs.empty(),
          step.focus_binding.has_value() && !previous_focus_binding);
      if (!evaluated) {
        return std::unexpected(evaluated.error());
      }
      stream = std::move(*evaluated);
    }
    auto checked = check_path_stream_size(stream, expression);
    if (!checked) {
      return std::unexpected(checked.error());
    }
    previous_focus_binding = step.focus_binding.has_value();
    if (stream.tuples.empty()) {
      break;
    }
  }
  return stream;
}

auto Evaluator::evaluate_path_step(
    const PathStep &step, PathStream stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth,
    bool last_step, bool terminal_projection, bool capture_focus_ancestor)
    -> Result<PathStream> {
  const bool activates_tuple_mode =
      stream.tuple_mode || step.focus_binding || step.index_binding ||
      std::ranges::any_of(step.stages, [](const PathStage &stage) {
        return stage.kind == PathStageKind::Index;
      });
  PathStream result{.keep_singleton = stream.keep_singleton || step.keep_array,
                    .tuple_mode = activates_tuple_mode};
  result.tuples.reserve(stream.tuples.size());
  const auto &expression = node(step.expression);
  for (auto &tuple : stream.tuples) {
    auto frame = frame_from_tuple(tuple, environment);
    if (!frame) {
      return std::unexpected(frame.error());
    }

    if (expression.kind == NodeKind::Block && !expression.children.empty() &&
        node(expression.children.back()).kind == NodeKind::Path &&
        node(expression.children.back()).pairs.empty()) {
      for (std::size_t child = 0; child + 1 < expression.children.size();
           ++child) {
        auto ignored = evaluate(expression.children[child], tuple.focus, *frame,
                                call_depth);
        if (!ignored) {
          return std::unexpected(ignored.error());
        }
      }
      PathStream nested_input{
          .tuples = {tuple},
          .keep_singleton = false,
          .tuple_mode = stream.tuple_mode,
      };
      auto nested =
          evaluate_path_stream(node(expression.children.back()),
                               std::move(nested_input), *frame, call_depth);
      if (!nested) {
        return std::unexpected(nested.error());
      }
      auto cohort = std::move(*nested);
      cohort.keep_singleton = cohort.keep_singleton || step.keep_array;
      if (!step.stages.empty() && cohort.tuples.size() == 1) {
        if (const auto *array = std::get_if<std::shared_ptr<Array>>(
                &cohort.tuples.front().focus.storage)) {
          auto growth =
              ensure_path_growth(0, (*array)->values.size(), expression);
          if (!growth) {
            return std::unexpected(growth.error());
          }
          const auto source = cohort.tuples.front();
          cohort.tuples.clear();
          for (const auto &item : (*array)->values) {
            auto expanded = source;
            expanded.focus = item;
            cohort.tuples.push_back(std::move(expanded));
          }
        }
      }
      auto staged =
          apply_path_stages(step, std::move(cohort), environment, call_depth);
      if (!staged) {
        return std::unexpected(staged.error());
      }
      auto growth = ensure_path_growth(result.tuples.size(),
                                       staged->tuples.size(), expression);
      if (!growth) {
        return std::unexpected(growth.error());
      }
      result.tuples.insert(result.tuples.end(), staged->tuples.begin(),
                           staged->tuples.end());
      continue;
    }

    auto value = evaluate(step.expression, tuple.focus, *frame, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    auto normalized = normalize_sequence(std::move(*value));
    if (is_undefined(normalized)) {
      continue;
    }

    if (!activates_tuple_mode && terminal_projection && step.stages.empty()) {
      if (auto *array =
              std::get_if<std::shared_ptr<Array>>(&normalized.storage)) {
        if (expression.kind == NodeKind::Array && *array) {
          normalized = make_array((*array)->values, true);
        }
        PathStream cohort{.keep_singleton = result.keep_singleton,
                          .tuple_mode = false};
        auto output = tuple;
        if (creates_ancestor(expression)) {
          output.ancestors.push_back(tuple.focus);
        }
        output.focus = normalized;
        cohort.tuples.push_back(std::move(output));
        auto growth = ensure_path_growth(result.tuples.size(), 1, expression);
        if (!growth) {
          return std::unexpected(growth.error());
        }
        result.tuples.insert(result.tuples.end(), cohort.tuples.begin(),
                             cohort.tuples.end());
        continue;
      }
    }

    std::vector<Value> items;
    if (is_sequence(normalized)) {
      for (const auto &item : as_sequence(normalized)->values) {
        if (const auto *array =
                std::get_if<std::shared_ptr<Array>>(&item.storage);
            array != nullptr &&
            (activates_tuple_mode || !(*array)->constructed)) {
          items.insert(items.end(), (*array)->values.begin(),
                       (*array)->values.end());
        } else {
          items.push_back(item);
        }
      }
    } else if (const auto *array =
                   std::get_if<std::shared_ptr<Array>>(&normalized.storage);
               array != nullptr &&
               (activates_tuple_mode || !(*array)->constructed)) {
      items = (*array)->values;
    } else {
      items.push_back(normalized);
    }

    if (last_step && items.size() == 1 &&
        std::holds_alternative<std::shared_ptr<Array>>(items.front().storage)) {
      result.keep_singleton = result.keep_singleton || step.keep_array;
    }

    if (terminal_projection && step.focus_binding) {
      auto output = tuple;
      if (capture_focus_ancestor) {
        output.ancestors.push_back(tuple.focus);
      }
      set_tuple_binding(output, *step.focus_binding, normalized);
      result.tuples.push_back(std::move(output));
      continue;
    }

    const bool direct_projection = !activates_tuple_mode && step.stages.empty();
    PathStream cohort{.keep_singleton = result.keep_singleton,
                      .tuple_mode = activates_tuple_mode};
    auto growth = ensure_path_growth(
        direct_projection ? result.tuples.size() : 0, items.size(), expression);
    if (!growth) {
      return std::unexpected(growth.error());
    }
    if (!direct_projection) {
      cohort.tuples.reserve(items.size());
    } else if (items.size() > result.tuples.capacity() - result.tuples.size()) {
      const auto required = result.tuples.size() + items.size();
      result.tuples.reserve(std::max(
          required, std::max<std::size_t>(1, result.tuples.capacity()) * 2));
    }
    for (std::size_t item_index = 0; item_index < items.size(); ++item_index) {
      auto output = items.size() == 1 ? std::move(tuple) : tuple;
      if (step.focus_binding) {
        if (capture_focus_ancestor) {
          output.ancestors.push_back(output.focus);
        }
        set_tuple_binding(output, *step.focus_binding, items[item_index]);
      } else if (expression.kind == NodeKind::Parent) {
        output.focus = items[item_index];
        if (!output.ancestors.empty()) {
          output.ancestors.pop_back();
        }
      } else {
        if (creates_ancestor(expression)) {
          output.ancestors.push_back(output.focus);
        }
        output.focus = items[item_index];
      }
      if (step.index_binding) {
        set_tuple_binding(output, *step.index_binding,
                          Value{static_cast<double>(item_index)});
      }
      auto &destination = direct_projection ? result.tuples : cohort.tuples;
      destination.push_back(std::move(output));
    }
    if (direct_projection) {
      continue;
    }
    if (!activates_tuple_mode) {
      auto staged =
          apply_path_stages(step, std::move(cohort), environment, call_depth);
      if (!staged) {
        return std::unexpected(staged.error());
      }
      auto staged_growth = ensure_path_growth(
          result.tuples.size(), staged->tuples.size(), expression);
      if (!staged_growth) {
        return std::unexpected(staged_growth.error());
      }
      result.tuples.insert(result.tuples.end(),
                           std::make_move_iterator(staged->tuples.begin()),
                           std::make_move_iterator(staged->tuples.end()));
    } else {
      auto cohort_growth = ensure_path_growth(result.tuples.size(),
                                              cohort.tuples.size(), expression);
      if (!cohort_growth) {
        return std::unexpected(cohort_growth.error());
      }
      result.tuples.insert(result.tuples.end(),
                           std::make_move_iterator(cohort.tuples.begin()),
                           std::make_move_iterator(cohort.tuples.end()));
    }
  }
  if (!activates_tuple_mode) {
    if (last_step && result.tuples.size() > 1) {
      std::vector<PathTuple> flattened;
      for (auto &tuple : result.tuples) {
        if (const auto *array =
                std::get_if<std::shared_ptr<Array>>(&tuple.focus.storage);
            array != nullptr && !(*array)->constructed) {
          auto flattened_growth = ensure_path_growth(
              flattened.size(), (*array)->values.size(), expression);
          if (!flattened_growth) {
            return std::unexpected(flattened_growth.error());
          }
          for (const auto &item : (*array)->values) {
            auto expanded = tuple;
            expanded.focus = item;
            flattened.push_back(std::move(expanded));
          }
        } else {
          auto flattened_growth =
              ensure_path_growth(flattened.size(), 1, expression);
          if (!flattened_growth) {
            return std::unexpected(flattened_growth.error());
          }
          flattened.push_back(std::move(tuple));
        }
      }
      result.tuples = std::move(flattened);
    }
    return result;
  }
  return apply_path_stages(step, std::move(result), environment, call_depth);
}

auto Evaluator::apply_path_stages(
    const PathStep &step, PathStream stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<PathStream> {
  bool expand_filter_input = false;
  for (const auto &stage : step.stages) {
    if (stage.kind == PathStageKind::Index) {
      for (std::size_t index = 0; index < stream.tuples.size(); ++index) {
        set_tuple_binding(stream.tuples[index], stage.name,
                          Value{static_cast<double>(index)});
      }
      continue;
    }
    auto filtered =
        filter_path_stream(stage.expression, std::move(stream), environment,
                           call_depth, expand_filter_input);
    if (!filtered) {
      return std::unexpected(filtered.error());
    }
    stream = std::move(*filtered);
    expand_filter_input = true;
  }
  return stream;
}

auto Evaluator::filter_path_stream(
    NodeId predicate_id, PathStream stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth,
    bool expand_arrays) -> Result<PathStream> {
  PathStream candidates{.keep_singleton = stream.keep_singleton,
                        .tuple_mode = stream.tuple_mode};
  for (const auto &tuple : stream.tuples) {
    if (!expand_arrays) {
      auto growth =
          ensure_path_growth(candidates.tuples.size(), 1, node(predicate_id));
      if (!growth) {
        return std::unexpected(growth.error());
      }
      candidates.tuples.push_back(tuple);
      continue;
    }
    const auto focus = normalize_sequence(tuple.focus);
    std::vector<Value> values;
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&focus.storage)) {
      values = (*array)->values;
    } else if (is_sequence(focus)) {
      values = as_sequence(focus)->values;
    }
    if (values.empty()) {
      auto growth =
          ensure_path_growth(candidates.tuples.size(), 1, node(predicate_id));
      if (!growth) {
        return std::unexpected(growth.error());
      }
      candidates.tuples.push_back(tuple);
      continue;
    }
    auto growth = ensure_path_growth(candidates.tuples.size(), values.size(),
                                     node(predicate_id));
    if (!growth) {
      return std::unexpected(growth.error());
    }
    for (const auto &value : values) {
      auto candidate = tuple;
      candidate.focus = value;
      candidates.tuples.push_back(std::move(candidate));
    }
  }

  PathStream result{.keep_singleton = stream.keep_singleton,
                    .tuple_mode = stream.tuple_mode};
  for (std::size_t index = 0; index < candidates.tuples.size(); ++index) {
    const auto &tuple = candidates.tuples[index];
    auto frame = frame_from_tuple(tuple, environment);
    if (!frame) {
      return std::unexpected(frame.error());
    }
    auto predicate = evaluate(predicate_id, tuple.focus, *frame, call_depth);
    if (!predicate) {
      return std::unexpected(predicate.error());
    }
    const auto value = normalize_sequence(std::move(*predicate));
    bool include = false;
    if (const auto *number = std::get_if<double>(&value.storage)) {
      const auto floored = std::floor(*number);
      const auto requested =
          floored < 0 ? static_cast<double>(candidates.tuples.size()) + floored
                      : floored;
      include = requested == static_cast<double>(index);
    } else {
      std::vector<Value> possible_indexes;
      if (is_sequence(value)) {
        possible_indexes = as_sequence(value)->values;
      } else if (const auto *array =
                     std::get_if<std::shared_ptr<Array>>(&value.storage)) {
        possible_indexes = (*array)->values;
      }
      const bool all_numbers =
          !possible_indexes.empty() &&
          std::ranges::all_of(possible_indexes, [](const Value &item) {
            return std::holds_alternative<double>(item.storage);
          });
      if (all_numbers) {
        include = std::ranges::any_of(possible_indexes, [&](const Value &item) {
          auto requested = std::floor(std::get<double>(item.storage));
          if (requested < 0) {
            requested += static_cast<double>(candidates.tuples.size());
          }
          return requested == static_cast<double>(index);
        });
      } else {
        include = effective_boolean(value);
      }
    }
    if (include) {
      result.tuples.push_back(tuple);
    }
  }
  return result;
}

auto Evaluator::sort_path_stream(
    const Node &sort, PathStream stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<PathStream> {
  struct KeyedTuple {
    PathTuple tuple;
    std::vector<Value> keys;
    std::size_t original_index{};
  };
  std::vector<KeyedTuple> keyed;
  keyed.reserve(stream.tuples.size());
  for (std::size_t index = 0; index < stream.tuples.size(); ++index) {
    const auto &tuple = stream.tuples[index];
    auto frame = frame_from_tuple(tuple, environment);
    if (!frame) {
      return std::unexpected(frame.error());
    }
    KeyedTuple item{.tuple = tuple, .original_index = index};
    for (const auto term : sort.children) {
      auto key = evaluate(term, tuple.focus, *frame, call_depth);
      if (!key) {
        return std::unexpected(key.error());
      }
      auto normalized = normalize_sequence(std::move(*key));
      if (!is_undefined(normalized) &&
          !std::holds_alternative<double>(normalized.storage) &&
          !std::holds_alternative<std::string>(normalized.storage)) {
        return std::unexpected(type_failure(
            "T2008", "Sort terms must evaluate to strings or numbers",
            program_.source, sort.span.end));
      }
      item.keys.push_back(std::move(normalized));
    }
    keyed.push_back(std::move(item));
  }
  for (std::size_t term = 0; term < sort.children.size(); ++term) {
    std::optional<bool> numeric;
    for (const auto &item : keyed) {
      if (is_undefined(item.keys[term])) {
        continue;
      }
      const bool item_numeric =
          std::holds_alternative<double>(item.keys[term].storage);
      if (numeric && *numeric != item_numeric) {
        return std::unexpected(
            type_failure("T2007", "Sort terms must have matching scalar types",
                         program_.source, sort.span.end));
      }
      numeric = item_numeric;
    }
  }
  auto interrupted = check_interrupt(sort);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  std::stable_sort(
      keyed.begin(), keyed.end(),
      [&](const KeyedTuple &left, const KeyedTuple &right) {
        for (std::size_t index = 0; index < left.keys.size(); ++index) {
          const auto &lhs = left.keys[index];
          const auto &rhs = right.keys[index];
          int order = 0;
          if (is_undefined(lhs)) {
            order = is_undefined(rhs) ? 0 : 1;
          } else if (is_undefined(rhs)) {
            order = -1;
          } else if (const auto *number = std::get_if<double>(&lhs.storage)) {
            const auto other = std::get<double>(rhs.storage);
            order = *number < other ? -1 : *number > other ? 1 : 0;
          } else {
            order = std::get<std::string>(lhs.storage)
                        .compare(std::get<std::string>(rhs.storage));
          }
          if (order != 0) {
            return index < sort.flags.size() && sort.flags[index] ? order > 0
                                                                  : order < 0;
          }
        }
        return left.original_index < right.original_index;
      });
  interrupted = check_interrupt(sort);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  stream.tuples.clear();
  stream.tuples.reserve(keyed.size());
  for (auto &item : keyed) {
    stream.tuples.push_back(std::move(item.tuple));
  }
  return stream;
}

auto Evaluator::group_path_stream(
    const Node &path, const PathStream &stream,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<Value> {
  struct GroupEntry {
    std::string key;
    std::size_t pair_index{};
    std::vector<PathTuple> tuples;
  };
  std::vector<GroupEntry> groups;
  for (const auto &tuple : stream.tuples) {
    auto frame = frame_from_tuple(tuple, environment);
    if (!frame) {
      return std::unexpected(frame.error());
    }
    for (std::size_t pair_index = 0; pair_index < path.pairs.size();
         ++pair_index) {
      const auto &[key_node, value_node] = path.pairs[pair_index];
      (void)value_node;
      auto key = evaluate(key_node, tuple.focus, *frame, call_depth);
      if (!key) {
        return std::unexpected(key.error());
      }
      const auto normalized = normalize_sequence(std::move(*key));
      if (is_undefined(normalized)) {
        continue;
      }
      const auto *text = std::get_if<std::string>(&normalized.storage);
      if (text == nullptr) {
        return std::unexpected(type_failure(
            "T1003", "Object constructor key must evaluate to a string",
            program_.source, node(key_node).span.end));
      }
      auto existing = std::ranges::find(groups, *text, &GroupEntry::key);
      if (existing != groups.end()) {
        if (existing->pair_index != pair_index) {
          return std::unexpected(dynamic_failure(
              "D1009", "Multiple grouping expressions generated the same key",
              program_.source, path.span.end, *text));
        }
        existing->tuples.push_back(tuple);
      } else {
        groups.push_back(GroupEntry{
            .key = *text, .pair_index = pair_index, .tuples = {tuple}});
      }
    }
  }

  auto result = std::make_shared<Object>();
  for (const auto &group : groups) {
    PathTuple reduced;
    std::vector<Value> focuses;
    for (const auto &tuple : group.tuples) {
      append_flattened(focuses, tuple.focus);
      for (const auto &[name, value] : tuple.bindings) {
        auto found = std::ranges::find(reduced.bindings, name,
                                       &std::pair<std::string, Value>::first);
        if (found == reduced.bindings.end()) {
          reduced.bindings.emplace_back(name, value);
        } else {
          std::vector<Value> values;
          append_flattened(values, found->second);
          append_flattened(values, value);
          found->second = normalize_sequence(make_sequence(std::move(values)));
        }
      }
    }
    reduced.focus = normalize_sequence(make_sequence(std::move(focuses)));
    if (!group.tuples.empty()) {
      reduced.ancestors = group.tuples.front().ancestors;
    }
    auto frame = frame_from_tuple(reduced, environment);
    if (!frame) {
      return std::unexpected(frame.error());
    }
    const auto value_node = path.pairs[group.pair_index].second;
    auto value = evaluate(value_node, reduced.focus, *frame, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (!is_undefined(*value)) {
      object_set(*result, group.key, std::move(*value));
    }
  }
  return Value{std::move(result)};
}

auto Evaluator::frame_from_tuple(
    const PathTuple &tuple, const std::shared_ptr<Environment> &environment)
    -> Result<std::shared_ptr<Environment>> {
  if (tuple.bindings.empty() && tuple.ancestors.empty()) {
    return environment;
  }
  auto charged = charge_environment_bindings_created(tuple.bindings.size(),
                                                     node(program_.root));
  if (!charged) {
    return std::unexpected(charged.error());
  }
  auto frame = make_environment(environment);
  frame->ancestors =
      std::make_shared<const std::vector<Value>>(tuple.ancestors);
  for (const auto &[name, value] : tuple.bindings) {
    frame->bindings.insert_or_assign(name, value);
  }
  return frame;
}

auto Evaluator::project_path_stream(PathStream stream) -> Value {
  if (stream.keep_singleton && stream.tuples.size() == 1) {
    if (const auto *array = std::get_if<std::shared_ptr<Array>>(
            &stream.tuples.front().focus.storage);
        array != nullptr && !(*array)->constructed) {
      return stream.tuples.front().focus;
    }
  }
  std::vector<Value> values;
  values.reserve(stream.tuples.size());
  for (auto &tuple : stream.tuples) {
    values.push_back(std::move(tuple.focus));
  }
  return normalize_sequence(
      make_sequence(std::move(values), stream.keep_singleton));
}

auto Evaluator::evaluate_range(const Node &current, const Value &left,
                               const Value &right) -> Result<Value> {
  constexpr std::uint64_t kJsonataRangeLimit = 10'000'000;
  const auto *begin = std::get_if<double>(&left.storage);
  const auto *end = std::get_if<double>(&right.storage);
  if (!is_undefined(left) && (begin == nullptr || !std::isfinite(*begin) ||
                              std::trunc(*begin) != *begin)) {
    return std::unexpected(type_failure(
        "T2003", "The left side of the range operator must be an integer",
        program_.source, current.span.end));
  }
  if (!is_undefined(right) &&
      (end == nullptr || !std::isfinite(*end) || std::trunc(*end) != *end)) {
    return std::unexpected(type_failure(
        "T2004", "The right side of the range operator must be an integer",
        program_.source, current.span.end));
  }
  if (is_undefined(left) || is_undefined(right) || *begin > *end) {
    return undefined();
  }
  const auto span =
      static_cast<long double>(*end) - static_cast<long double>(*begin) + 1.0L;
  if (!std::isfinite(span) ||
      span > static_cast<long double>(kJsonataRangeLimit)) {
    return std::unexpected(dynamic_failure(
        "D2014", "The size of the range exceeds the JSONata limit",
        program_.source, current.span.end));
  }
  const auto count = static_cast<std::uint64_t>(span);
  if (count > state_->request.limits.max_sequence_items) {
    return std::unexpected(dynamic_failure("D2015",
                                           "Maximum sequence length exceeded",
                                           program_.source, current.span.end));
  }
  std::vector<Value> values;
  values.reserve(static_cast<std::size_t>(count));
  std::size_t generated = 0;
  for (std::uint64_t index = 0; index < count; ++index) {
    values.emplace_back(*begin + static_cast<double>(index));
    if ((++generated & 0xFFFU) == 0) {
      auto interrupted = check_interrupt(current);
      if (!interrupted) {
        return std::unexpected(interrupted.error());
      }
    }
  }
  return normalize_sequence(make_sequence(std::move(values)));
}

auto Evaluator::compare_values(const Node &current, const Value &left,
                               const Value &right) -> Result<Value> {
  const bool left_comparable =
      is_undefined(left) || std::holds_alternative<double>(left.storage) ||
      std::holds_alternative<std::string>(left.storage);
  const bool right_comparable =
      is_undefined(right) || std::holds_alternative<double>(right.storage) ||
      std::holds_alternative<std::string>(right.storage);
  if (!left_comparable || !right_comparable) {
    return std::unexpected(
        type_failure("T2010", "Comparison operands must be strings or numbers",
                     program_.source, current.span.end));
  }
  if (is_undefined(left) || is_undefined(right)) {
    return undefined();
  }
  if (left.storage.index() != right.storage.index()) {
    return std::unexpected(type_failure(
        "T2009", "Comparison operands must have matching scalar types",
        program_.source, current.span.end));
  }
  int comparison = 0;
  if (const auto *left_number = std::get_if<double>(&left.storage)) {
    const auto *right_number = std::get_if<double>(&right.storage);
    comparison = *left_number < *right_number   ? -1
                 : *left_number > *right_number ? 1
                                                : 0;
  } else if (const auto *left_string =
                 std::get_if<std::string>(&left.storage)) {
    const auto *right_string = std::get_if<std::string>(&right.storage);
    comparison = left_string->compare(*right_string);
  }
  const auto &op = current.text;
  return Value{op == "<"    ? comparison < 0
               : op == "<=" ? comparison <= 0
               : op == ">"  ? comparison > 0
                            : comparison >= 0};
}

auto Evaluator::evaluate_conditional(
    const Node &current, const Value &input,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<Value> {
  auto condition =
      evaluate(current.children[0], input, environment, call_depth);
  if (!condition) {
    return std::unexpected(condition.error());
  }
  if (effective_boolean(*condition)) {
    return evaluate(current.children[1], input, environment, call_depth);
  }
  if (current.children.size() == 3) {
    return evaluate(current.children[2], input, environment, call_depth);
  }
  return undefined();
}

auto Evaluator::evaluate_array(const Node &current, const Value &input,
                               const std::shared_ptr<Environment> &environment,
                               std::size_t call_depth) -> Result<Value> {
  std::vector<Value> result;
  for (const auto child : current.children) {
    auto value = evaluate(child, input, environment, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (is_undefined(*value)) {
      continue;
    }
    if (node(child).kind == NodeKind::Array) {
      result.push_back(std::move(*value));
    } else if (is_sequence(*value)) {
      for (const auto &item : as_sequence(*value)->values) {
        result.push_back(item);
      }
    } else if (const auto *array =
                   std::get_if<std::shared_ptr<Array>>(&value->storage)) {
      for (const auto &item : (*array)->values) {
        result.push_back(item);
      }
    } else {
      result.push_back(std::move(*value));
    }
  }
  return make_array(std::move(result), false);
}

auto Evaluator::key_string(const Node &key_node, const Value &value)
    -> Result<std::string> {
  const auto normalized = normalize_sequence(value);
  if (const auto *string = std::get_if<std::string>(&normalized.storage)) {
    return *string;
  }
  return std::unexpected(
      type_failure("T1003", "Object constructor key must evaluate to a string",
                   program_.source, key_node.span.end));
}

auto Evaluator::evaluate_object(const Node &current, const Value &input,
                                const std::shared_ptr<Environment> &environment,
                                std::size_t call_depth) -> Result<Value> {
  std::vector<std::pair<std::string, Value>> result;
  for (const auto &[key_node, value_node] : current.pairs) {
    auto key = evaluate(key_node, input, environment, call_depth);
    if (!key) {
      return std::unexpected(key.error());
    }
    auto converted_key = key_string(node(key_node), *key);
    if (!converted_key) {
      return std::unexpected(converted_key.error());
    }
    auto value = evaluate(value_node, input, environment, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (!is_undefined(normalize_sequence(*value))) {
      if (std::ranges::any_of(result, [&](const auto &member) {
            return member.first == *converted_key;
          })) {
        return std::unexpected(dynamic_failure(
            "D1009", "Multiple object key expressions generated the same key",
            program_.source, current.span.end, *converted_key));
      }
      result.emplace_back(std::move(*converted_key),
                          normalize_sequence(std::move(*value)));
    }
  }
  return make_object(std::move(result));
}

auto Evaluator::evaluate_block(const Node &current, const Value &input,
                               const std::shared_ptr<Environment> &environment,
                               std::size_t call_depth) -> Result<Value> {
  auto frame = make_environment(environment);
  Value result = undefined();
  for (const auto expression : current.children) {
    auto value = evaluate(expression, input, frame, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    result = std::move(*value);
  }
  return result;
}

auto Evaluator::evaluate_bind(const Node &current, const Value &input,
                              const std::shared_ptr<Environment> &environment,
                              std::size_t call_depth) -> Result<Value> {
  auto value =
      evaluate(current.children.front(), input, environment, call_depth);
  if (!value) {
    return std::unexpected(value.error());
  }
  if (!environment->bindings.contains(current.text)) {
    auto charged = charge_environment_bindings_created(1, current);
    if (!charged) {
      return std::unexpected(charged.error());
    }
  }
  environment->bindings.insert_or_assign(current.text, *value);
  return value;
}

auto Evaluator::evaluate_lambda(const Node &current, const Value &input,
                                const std::shared_ptr<Environment> &environment)
    -> Value {
  auto function = std::make_shared<Function>();
  function->kind = FunctionKind::Lambda;
  function->body = current.children.front();
  function->closure = environment;
  function->captured_input = input;
  function->signature = current.signature;
  function->program = &program_;
  function->program_owner = program_owner_;
  for (std::size_t index = 1; index < current.children.size(); ++index) {
    function->parameters.push_back(
        std::get<std::string>(node(current.children[index]).literal));
  }
  function->arity = function->parameters.size();
  return Value{std::move(function)};
}

auto Evaluator::evaluate_call(const Node &current, const Value &input,
                              const std::shared_ptr<Environment> &environment,
                              std::size_t call_depth) -> Result<Value> {
  const auto procedure_name = [&]() -> std::optional<std::string_view> {
    const auto &procedure = node(current.children.front());
    if (procedure.kind == NodeKind::Name) {
      return procedure.text;
    }
    if (procedure.kind == NodeKind::Path && !procedure.path_steps.empty()) {
      const auto &first = node(procedure.path_steps.front().expression);
      if (first.kind == NodeKind::Name) {
        return first.text;
      }
    }
    return std::nullopt;
  }();
  auto function =
      evaluate(current.children.front(), input, environment, call_depth);
  if (!function) {
    return std::unexpected(function.error());
  }
  std::vector<Value> arguments;
  std::vector<std::optional<Value>> partials;
  bool partial = false;
  for (std::size_t index = 1; index < current.children.size(); ++index) {
    if (node(current.children[index]).kind == NodeKind::Placeholder) {
      partial = true;
      partials.emplace_back(std::nullopt);
      continue;
    }
    auto argument =
        evaluate(current.children[index], input, environment, call_depth);
    if (!argument) {
      return std::unexpected(argument.error());
    }
    partials.emplace_back(*argument);
    arguments.push_back(std::move(*argument));
  }
  if (partial) {
    const auto normalized = normalize_sequence(*function);
    const auto *target =
        std::get_if<std::shared_ptr<Function>>(&normalized.storage);
    if (target == nullptr) {
      if (procedure_name && environment->lookup(*procedure_name)) {
        return std::unexpected(type_failure(
            "T1007", "Partial application target may be missing '$'",
            program_.source, current.span.end, std::string{*procedure_name}));
      }
      return std::unexpected(
          type_failure("T1008", "Partial application target must be a function",
                       program_.source, current.span.end));
    }
    auto partial_function = std::make_shared<Function>();
    partial_function->kind = FunctionKind::Partial;
    partial_function->target = *target;
    partial_function->partial_arguments = std::move(partials);
    partial_function->arity = static_cast<std::size_t>(std::ranges::count_if(
        partial_function->partial_arguments,
        [](const std::optional<Value> &value) { return !value.has_value(); }));
    return Value{std::move(partial_function)};
  }
  if (is_undefined(normalize_sequence(*function)) && procedure_name &&
      environment->lookup(*procedure_name)) {
    return std::unexpected(type_failure(
        "T1005", "Function invocation may be missing '$'", program_.source,
        current.span.end, std::string{*procedure_name}));
  }
  if (current.tail_call) {
    if (tail_call_frames_.empty()) {
      return std::unexpected(host_failure(
          "H9006", "Tail call evaluated outside a lambda application",
          program_.source, current.span.end));
    }
    tail_call_frames_.back() = PendingTailCall{
        .function = std::move(*function),
        .arguments = std::move(arguments),
        .input = input,
        .environment = environment,
        .call_node = &current,
    };
    return undefined();
  }
  return apply(*function, std::move(arguments), input, environment, current,
               call_depth);
}

auto Evaluator::evaluate_chain_call(
    const Node &call, Value left, const Value &input,
    const std::shared_ptr<Environment> &environment, std::size_t call_depth)
    -> Result<Value> {
  auto function =
      evaluate(call.children.front(), input, environment, call_depth);
  if (!function) {
    return std::unexpected(function.error());
  }
  std::vector<Value> arguments{std::move(left)};
  for (std::size_t index = 1; index < call.children.size(); ++index) {
    auto argument =
        evaluate(call.children[index], input, environment, call_depth);
    if (!argument) {
      return std::unexpected(argument.error());
    }
    arguments.push_back(std::move(*argument));
  }
  return apply(*function, std::move(arguments), input, environment, call,
               call_depth);
}

auto Evaluator::apply(const Value &raw_function, std::vector<Value> arguments,
                      const Value &input,
                      const std::shared_ptr<Environment> &environment,
                      const Node &call_node, std::size_t call_depth)
    -> Result<Value> {
  Value active_function = raw_function;
  Value active_input = input;
  auto active_environment = environment;
  const Node *active_call_node = &call_node;

  for (;;) {
    const auto function_value = normalize_sequence(active_function);
    if (const auto *regex =
            std::get_if<std::shared_ptr<RegexValue>>(&function_value.storage)) {
      if (!*regex || arguments.empty()) {
        return std::unexpected(type_failure(
            "T0410", "Regular expression matcher requires a string argument",
            program_.source, active_call_node->span.end));
      }
      const auto value = normalize_sequence(arguments[0]);
      const auto *string = std::get_if<std::string>(&value.storage);
      if (string == nullptr) {
        return std::unexpected(type_failure(
            "T0410", "Regular expression matcher requires a string argument",
            program_.source, active_call_node->span.end));
      }
      std::size_t offset = 0;
      if (arguments.size() >= 2) {
        const auto position = normalize_sequence(arguments[1]);
        if (const auto *number = std::get_if<double>(&position.storage)) {
          const auto truncated = std::trunc(static_cast<long double>(*number));
          if (std::isfinite(truncated) && truncated > 0.0L) {
            offset = truncated >= static_cast<long double>(string->size())
                         ? string->size()
                         : static_cast<std::size_t>(truncated);
          }
        }
      }
      auto match =
          next_regex_match(**regex, *string, offset, *active_call_node);
      if (!match) {
        return std::unexpected(match.error());
      }
      return *match ? regex_callable_result(**match, *string) : undefined();
    }

    const auto *function_ptr =
        std::get_if<std::shared_ptr<Function>>(&function_value.storage);
    if (function_ptr == nullptr || !*function_ptr) {
      return std::unexpected(
          type_failure("T1006", "Attempted to invoke a non-function",
                       program_.source, active_call_node->span.end));
    }
    const auto &function = **function_ptr;
    if (function.program != nullptr && function.program != &program_) {
      Evaluator nested(*function.program, state_, eval_depth_,
                       function.program_owner);
      nested.root_ = root_;
      nested.base_environment_ = base_environment_;
      nested.environment_ = active_environment;
      return nested.apply(active_function, std::move(arguments), active_input,
                          active_environment, *active_call_node, call_depth);
    }
    if (function.kind == FunctionKind::Builtin) {
      if (function.signature) {
        auto validated = validate_function_arguments(
            *function.signature, arguments, active_input, program_.source,
            active_call_node->span.end);
        if (!validated) {
          return std::unexpected(validated.error());
        }
        arguments = std::move(*validated);
      }
      return invoke_builtin(function.name, std::move(arguments), active_input,
                            active_environment, *active_call_node, call_depth);
    }
    if (function.kind == FunctionKind::Transform) {
      if (function.signature) {
        auto validated = validate_function_arguments(
            *function.signature, arguments, active_input, program_.source,
            active_call_node->span.end);
        if (!validated) {
          return std::unexpected(validated.error());
        }
        arguments = std::move(*validated);
      }
      return invoke_transform(function, std::move(arguments), *active_call_node,
                              call_depth);
    }
    if (function.kind == FunctionKind::Partial) {
      std::vector<Value> merged;
      std::size_t supplied = 0;
      for (const auto &argument : function.partial_arguments) {
        if (argument) {
          merged.push_back(*argument);
        } else if (supplied < arguments.size()) {
          merged.push_back(arguments[supplied++]);
        } else {
          return std::unexpected(type_failure(
              "T0410", "Insufficient arguments for partial function",
              program_.source, active_call_node->span.end));
        }
      }
      while (supplied < arguments.size()) {
        merged.push_back(arguments[supplied++]);
      }
      active_function = Value{function.target};
      arguments = std::move(merged);
      continue;
    }
    if (function.kind == FunctionKind::Composition) {
      if (function.composition.empty()) {
        return undefined();
      }
      auto result = apply(function.composition.front(), std::move(arguments),
                          active_input, active_environment, *active_call_node,
                          call_depth + 1);
      if (!result) {
        return std::unexpected(result.error());
      }
      for (std::size_t index = 1; index < function.composition.size();
           ++index) {
        result = apply(function.composition[index], {std::move(*result)},
                       active_input, active_environment, *active_call_node,
                       call_depth + 1);
        if (!result) {
          return std::unexpected(result.error());
        }
      }
      return result;
    }

    if (function.signature) {
      auto validated = validate_function_arguments(
          *function.signature, arguments, active_input, program_.source,
          active_call_node->span.end);
      if (!validated) {
        return std::unexpected(validated.error());
      }
      arguments = std::move(*validated);
    }
    auto charged = charge_environment_bindings_created(
        function.parameters.size(), *active_call_node);
    if (!charged) {
      return std::unexpected(charged.error());
    }
    auto frame = make_environment(function.closure);
    for (std::size_t index = 0; index < function.parameters.size(); ++index) {
      frame->bindings.emplace(function.parameters[index],
                              index < arguments.size() ? arguments[index]
                                                       : undefined());
    }

    tail_call_frames_.emplace_back();
    auto result =
        evaluate(function.body, function.captured_input, frame, call_depth + 1);
    auto tail_call = std::move(tail_call_frames_.back());
    tail_call_frames_.pop_back();
    if (!result) {
      return std::unexpected(result.error());
    }
    if (!tail_call) {
      return result;
    }

    if (tail_call->call_node == nullptr || !tail_call->environment) {
      return std::unexpected(
          host_failure("H9006", "Invalid pending JSONata tail call",
                       program_.source, active_call_node->span.end));
    }
    active_function = std::move(tail_call->function);
    arguments = std::move(tail_call->arguments);
    active_input = std::move(tail_call->input);
    active_environment = std::move(tail_call->environment);
    active_call_node = tail_call->call_node;
  }
}

auto Evaluator::evaluate_filter(const Node &current, const Value &input,
                                const std::shared_ptr<Environment> &environment,
                                std::size_t call_depth) -> Result<Value> {
  auto source = evaluate(current.children[0], input, environment, call_depth);
  if (!source) {
    return std::unexpected(source.error());
  }
  if (current.children.size() == 1) {
    return make_sequence(value_list(*source), true);
  }
  const bool keep_singleton =
      is_sequence(*source) && as_sequence(*source)->keep_singleton;
  auto candidates = value_list(*source);
  std::vector<Value> result;
  for (std::size_t index = 0; index < candidates.size(); ++index) {
    auto predicate = evaluate(current.children[1], candidates[index],
                              environment, call_depth);
    if (!predicate) {
      return std::unexpected(predicate.error());
    }
    const auto predicate_value = normalize_sequence(*predicate);
    bool include = false;
    if (const auto *number = std::get_if<double>(&predicate_value.storage)) {
      auto requested = std::floor(*number);
      if (requested < 0) {
        requested += static_cast<double>(candidates.size());
      }
      include = requested == static_cast<double>(index);
    } else {
      std::vector<Value> possible_indexes;
      if (is_sequence(predicate_value)) {
        possible_indexes = as_sequence(predicate_value)->values;
      } else if (const auto *array = std::get_if<std::shared_ptr<Array>>(
                     &predicate_value.storage)) {
        possible_indexes = (*array)->values;
      }
      const bool all_numbers =
          !possible_indexes.empty() &&
          std::ranges::all_of(possible_indexes, [](const Value &item) {
            return std::holds_alternative<double>(item.storage);
          });
      if (all_numbers) {
        include = std::ranges::any_of(possible_indexes, [&](const Value &item) {
          auto requested = std::floor(std::get<double>(item.storage));
          if (requested < 0) {
            requested += static_cast<double>(candidates.size());
          }
          return requested == static_cast<double>(index);
        });
      } else {
        include = effective_boolean(predicate_value);
      }
    }
    if (include) {
      result.push_back(candidates[index]);
    }
  }
  return normalize_sequence(make_sequence(std::move(result), keep_singleton));
}

auto Evaluator::evaluate_sort(const Node &current, const Value &input,
                              const std::shared_ptr<Environment> &environment,
                              std::size_t call_depth) -> Result<Value> {
  auto source = evaluate(current.children[0], input, environment, call_depth);
  if (!source) {
    return std::unexpected(source.error());
  }
  auto values = value_list(*source);
  struct KeyedValue {
    Value value;
    std::vector<Value> keys;
    std::size_t original_index{};
  };
  std::vector<KeyedValue> keyed;
  keyed.reserve(values.size());
  for (std::size_t index = 0; index < values.size(); ++index) {
    KeyedValue item{.value = values[index], .original_index = index};
    for (std::size_t term = 1; term < current.children.size(); ++term) {
      auto key = evaluate(current.children[term], values[index], environment,
                          call_depth);
      if (!key) {
        return std::unexpected(key.error());
      }
      item.keys.push_back(normalize_sequence(std::move(*key)));
    }
    keyed.push_back(std::move(item));
  }
  auto compare = [&](const KeyedValue &left, const KeyedValue &right) {
    for (std::size_t index = 0; index < left.keys.size(); ++index) {
      int order = 0;
      if (const auto *lhs = std::get_if<double>(&left.keys[index].storage)) {
        if (const auto *rhs = std::get_if<double>(&right.keys[index].storage)) {
          order = *lhs < *rhs ? -1 : *lhs > *rhs ? 1 : 0;
        }
      } else if (const auto *lhs =
                     std::get_if<std::string>(&left.keys[index].storage)) {
        if (const auto *rhs =
                std::get_if<std::string>(&right.keys[index].storage)) {
          order = lhs->compare(*rhs);
        }
      }
      if (order != 0) {
        return current.flags[index] ? order > 0 : order < 0;
      }
    }
    return left.original_index < right.original_index;
  };
  auto interrupted = check_interrupt(current);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  std::stable_sort(keyed.begin(), keyed.end(), compare);
  interrupted = check_interrupt(current);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  values.clear();
  for (auto &item : keyed) {
    values.push_back(std::move(item.value));
  }
  return normalize_sequence(make_sequence(std::move(values)));
}

auto Evaluator::evaluate_group(const Node &current, const Value &input,
                               const std::shared_ptr<Environment> &environment,
                               std::size_t call_depth) -> Result<Value> {
  auto source =
      evaluate(current.children.front(), input, environment, call_depth);
  if (!source) {
    return std::unexpected(source.error());
  }
  struct GroupEntry {
    std::string key;
    std::size_t pair_index{};
    std::vector<Value> items;
  };

  auto items = value_list(*source);
  if (items.empty()) {
    items.push_back(undefined());
  }
  std::vector<GroupEntry> groups;
  for (const auto &item : items) {
    for (std::size_t pair_index = 0; pair_index < current.pairs.size();
         ++pair_index) {
      const auto &[key_node, value_node] = current.pairs[pair_index];
      (void)value_node;
      auto key = evaluate(key_node, item, environment, call_depth);
      if (!key) {
        return std::unexpected(key.error());
      }
      auto string_key = key_string(node(key_node), *key);
      if (!string_key) {
        return std::unexpected(string_key.error());
      }

      auto existing = std::ranges::find(groups, *string_key, &GroupEntry::key);
      if (existing != groups.end()) {
        if (existing->pair_index != pair_index) {
          return std::unexpected(dynamic_failure(
              "D1009", "Multiple grouping expressions generated the same key",
              program_.source, current.span.end, *string_key));
        }
        existing->items.push_back(item);
      } else {
        groups.push_back(GroupEntry{.key = std::move(*string_key),
                                    .pair_index = pair_index,
                                    .items = {item}});
      }
    }
  }

  auto result = std::make_shared<Object>();
  for (auto &group : groups) {
    auto grouped_input =
        normalize_sequence(make_sequence(std::move(group.items)));
    const auto value_node = current.pairs[group.pair_index].second;
    auto value = evaluate(value_node, grouped_input, environment, call_depth);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (is_undefined(*value)) {
      continue;
    }
    object_set(*result, std::move(group.key), std::move(*value));
  }
  return Value{std::move(result)};
}

auto Evaluator::deep_copy(const Value &value) -> Value {
  struct Frame {
    const Value *source{};
    Value *target{};
  };

  Value result;
  std::vector<Frame> pending{{.source = &value, .target = &result}};
  while (!pending.empty()) {
    const auto frame = pending.back();
    pending.pop_back();
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&frame.source->storage)) {
      if (!*array) {
        *frame.target = Value{std::shared_ptr<Array>{}};
        continue;
      }
      auto copy = std::make_shared<Array>();
      copy->constructed = (*array)->constructed;
      copy->footprint = (*array)->footprint;
      copy->values.resize((*array)->values.size());
      frame.target->storage = copy;
      for (std::size_t index = copy->values.size(); index > 0; --index) {
        pending.push_back(Frame{.source = &(*array)->values[index - 1],
                                .target = &copy->values[index - 1]});
      }
      continue;
    }
    if (const auto *object =
            std::get_if<std::shared_ptr<Object>>(&frame.source->storage)) {
      if (!*object) {
        *frame.target = Value{std::shared_ptr<Object>{}};
        continue;
      }
      auto copy = std::make_shared<Object>();
      copy->footprint = (*object)->footprint;
      copy->members.reserve((*object)->members.size());
      for (const auto &[key, item] : (*object)->members) {
        (void)item;
        copy->members.emplace_back(key, Value{});
      }
      frame.target->storage = copy;
      for (std::size_t index = copy->members.size(); index > 0; --index) {
        pending.push_back(Frame{.source = &(*object)->members[index - 1].second,
                                .target = &copy->members[index - 1].second});
      }
      continue;
    }
    if (is_sequence(*frame.source)) {
      const auto &sequence = as_sequence(*frame.source);
      if (!sequence) {
        *frame.target = Value{std::shared_ptr<Sequence>{}};
        continue;
      }
      auto copy = std::make_shared<Sequence>();
      copy->keep_singleton = sequence->keep_singleton;
      copy->footprint = sequence->footprint;
      copy->values.resize(sequence->values.size());
      frame.target->storage = copy;
      for (std::size_t index = copy->values.size(); index > 0; --index) {
        pending.push_back(Frame{.source = &sequence->values[index - 1],
                                .target = &copy->values[index - 1]});
      }
      continue;
    }
    *frame.target = *frame.source;
  }
  return result;
}

auto Evaluator::evaluate_transform(
    const Node &current, const Value &,
    const std::shared_ptr<Environment> &environment, std::size_t)
    -> Result<Value> {
  auto function = std::make_shared<Function>();
  function->kind = FunctionKind::Transform;
  function->closure = environment;
  function->transform_nodes = current.children;
  function->program = &program_;
  function->program_owner = program_owner_;
  return Value{std::move(function)};
}

auto Evaluator::invoke_transform(const Function &function,
                                 std::vector<Value> arguments,
                                 const Node &call_node, std::size_t call_depth)
    -> Result<Value> {
  Value target = arguments.empty() ? deep_copy(root_) : deep_copy(arguments[0]);
  auto frame = make_environment(function.closure);
  auto locations =
      evaluate(function.transform_nodes[0], target, frame, call_depth + 1);
  if (!locations) {
    return std::unexpected(locations.error());
  }
  auto update_location = [&](Value location) -> Result<void> {
    auto *object = std::get_if<std::shared_ptr<Object>>(&location.storage);
    if (object == nullptr || !*object) {
      return {};
    }
    auto update =
        evaluate(function.transform_nodes[1], location, frame, call_depth + 1);
    if (!update) {
      return std::unexpected(update.error());
    }
    const auto normalized_update = normalize_sequence(std::move(*update));
    if (!is_undefined(normalized_update)) {
      const auto *update_object =
          std::get_if<std::shared_ptr<Object>>(&normalized_update.storage);
      if (update_object == nullptr || !*update_object) {
        return std::unexpected(
            type_failure("T2011", "Transform update must evaluate to an object",
                         program_.source, call_node.span.end));
      }
      for (const auto &[key, value] : (*update_object)->members) {
        object_set(**object, key, value);
      }
    }
    if (function.transform_nodes.size() == 3) {
      auto remove = evaluate(function.transform_nodes[2], location, frame,
                             call_depth + 1);
      if (!remove) {
        return std::unexpected(remove.error());
      }
      const auto normalized_remove = normalize_sequence(std::move(*remove));
      if (is_undefined(normalized_remove)) {
        return {};
      }
      for (const auto &key_value : value_list(normalized_remove)) {
        const auto *key = std::get_if<std::string>(&key_value.storage);
        if (key == nullptr) {
          return std::unexpected(type_failure(
              "T2012", "Transform delete clause must contain strings",
              program_.source, call_node.span.end));
        }
        object_erase(**object, *key);
      }
    }
    return {};
  };
  for (auto &location : value_list(*locations)) {
    auto updated = update_location(location);
    if (!updated) {
      return std::unexpected(updated.error());
    }
  }
  return target;
}

auto Evaluator::node(NodeId id) const -> const Node & {
  return program_.nodes[id];
}

} // namespace dagforge::jsonata::detail
