#pragma once

#include "model.hpp"
#include "regex_adapter.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge::jsonata::detail {

struct EvaluationState {
  EvaluationRequest request;
  EvaluationStatistics statistics;
  std::chrono::steady_clock::time_point deadline;
  std::size_t checks_until_clock{64};
  std::size_t regex_matches{};
  std::size_t environment_bindings_created{};
  std::mt19937_64 random_engine;
  std::vector<std::weak_ptr<Environment>> environments;
  std::size_t environments_until_prune{1024};
};

struct PathTuple {
  Value focus;
  std::vector<std::pair<std::string, Value>> bindings;
  std::vector<Value> ancestors;
};

struct PathStream {
  std::vector<PathTuple> tuples;
  bool keep_singleton{false};
  bool tuple_mode{false};
};

struct PendingTailCall {
  Value function;
  std::vector<Value> arguments;
  Value input;
  std::shared_ptr<Environment> environment;
  const Node *call_node{};
};

class Evaluator {
public:
  Evaluator(const ProgramData &program, const EvaluationRequest &request);

  [[nodiscard]] auto run() -> Result<EvaluationSuccess>;

private:
  Evaluator(const ProgramData &program, std::shared_ptr<EvaluationState> state,
            std::size_t eval_depth,
            std::shared_ptr<const ProgramData> program_owner = {});
  [[nodiscard]] auto run_impl() -> Result<EvaluationSuccess>;
  [[nodiscard]] auto
  make_environment(std::shared_ptr<const Environment> parent = {})
      -> std::shared_ptr<Environment>;
  auto release_environment_cycles() noexcept -> void;
  [[nodiscard]] auto consume_step(const Node &node, std::size_t call_depth)
      -> Result<void>;
  [[nodiscard]] auto check_interrupt(const Node &node) -> Result<void>;
  [[nodiscard]] auto observe_value(const Value &value, const Node &node)
      -> Result<void>;
  [[nodiscard]] auto charge_environment_bindings_created(std::size_t count,
                                                         const Node &node)
      -> Result<void>;
  [[nodiscard]] auto check_path_stream_size(const PathStream &stream,
                                            const Node &node) -> Result<void>;
  [[nodiscard]] auto ensure_path_growth(std::size_t current,
                                        std::size_t additional,
                                        const Node &node) -> Result<void>;
  [[nodiscard]] auto evaluate(NodeId id, const Value &input,
                              const std::shared_ptr<Environment> &environment,
                              std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto evaluate_literal(const Node &node) -> Value;
  [[nodiscard]] auto evaluate_name(const Node &node, const Value &input)
      -> Result<Value>;
  [[nodiscard]] auto
  evaluate_variable(const Node &node, const Value &input,
                    const std::shared_ptr<Environment> &environment) -> Value;
  [[nodiscard]] auto evaluate_regex(const Node &node) -> Result<Value>;
  [[nodiscard]] auto evaluate_wildcard(const Node &node, const Value &input)
      -> Result<Value>;
  [[nodiscard]] auto evaluate_descendant(const Node &node, const Value &input)
      -> Result<Value>;
  [[nodiscard]] auto
  evaluate_unary(const Node &node, const Value &input,
                 const std::shared_ptr<Environment> &environment,
                 std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_binary(const Node &node, const Value &input,
                  const std::shared_ptr<Environment> &environment,
                  std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_path(const Node &node, const Value &input,
                const std::shared_ptr<Environment> &environment,
                std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_path_stream(const Node &node, PathStream stream,
                       const std::shared_ptr<Environment> &environment,
                       std::size_t call_depth) -> Result<PathStream>;
  [[nodiscard]] auto
  make_initial_path_stream(const Node &node, const Value &input,
                           const std::shared_ptr<Environment> &environment)
      -> PathStream;
  [[nodiscard]] auto
  evaluate_path_step(const PathStep &step, PathStream stream,
                     const std::shared_ptr<Environment> &environment,
                     std::size_t call_depth, bool last_step,
                     bool terminal_projection, bool capture_focus_ancestor)
      -> Result<PathStream>;
  [[nodiscard]] auto
  apply_path_stages(const PathStep &step, PathStream stream,
                    const std::shared_ptr<Environment> &environment,
                    std::size_t call_depth) -> Result<PathStream>;
  [[nodiscard]] auto
  filter_path_stream(NodeId predicate, PathStream stream,
                     const std::shared_ptr<Environment> &environment,
                     std::size_t call_depth, bool expand_arrays)
      -> Result<PathStream>;
  [[nodiscard]] auto
  sort_path_stream(const Node &sort, PathStream stream,
                   const std::shared_ptr<Environment> &environment,
                   std::size_t call_depth) -> Result<PathStream>;
  [[nodiscard]] auto
  group_path_stream(const Node &path, const PathStream &stream,
                    const std::shared_ptr<Environment> &environment,
                    std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  frame_from_tuple(const PathTuple &tuple,
                   const std::shared_ptr<Environment> &environment)
      -> Result<std::shared_ptr<Environment>>;
  [[nodiscard]] auto project_path_stream(PathStream stream) -> Value;
  [[nodiscard]] auto evaluate_range(const Node &node, const Value &left,
                                    const Value &right) -> Result<Value>;
  [[nodiscard]] auto compare_values(const Node &node, const Value &left,
                                    const Value &right) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_conditional(const Node &node, const Value &input,
                       const std::shared_ptr<Environment> &environment,
                       std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_array(const Node &node, const Value &input,
                 const std::shared_ptr<Environment> &environment,
                 std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_object(const Node &node, const Value &input,
                  const std::shared_ptr<Environment> &environment,
                  std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_block(const Node &node, const Value &input,
                 const std::shared_ptr<Environment> &environment,
                 std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_bind(const Node &node, const Value &input,
                const std::shared_ptr<Environment> &environment,
                std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_lambda(const Node &node, const Value &input,
                  const std::shared_ptr<Environment> &environment) -> Value;
  [[nodiscard]] auto
  evaluate_call(const Node &node, const Value &input,
                const std::shared_ptr<Environment> &environment,
                std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_chain_call(const Node &call, Value left, const Value &input,
                      const std::shared_ptr<Environment> &environment,
                      std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto apply(const Value &function, std::vector<Value> arguments,
                           const Value &input,
                           const std::shared_ptr<Environment> &environment,
                           const Node &call_node, std::size_t call_depth)
      -> Result<Value>;
  [[nodiscard]] auto
  evaluate_filter(const Node &node, const Value &input,
                  const std::shared_ptr<Environment> &environment,
                  std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_sort(const Node &node, const Value &input,
                const std::shared_ptr<Environment> &environment,
                std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_group(const Node &node, const Value &input,
                 const std::shared_ptr<Environment> &environment,
                 std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto
  evaluate_transform(const Node &node, const Value &input,
                     const std::shared_ptr<Environment> &environment,
                     std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto deep_copy(const Value &value) -> Value;

  auto install_builtins(Environment &environment) -> void;
  [[nodiscard]] auto invoke_builtin(
      std::string_view name, std::vector<Value> arguments, const Value &input,
      const std::shared_ptr<Environment> &environment, const Node &call_node,
      std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto invoke_higher_order(
      std::string_view name, std::vector<Value> arguments, const Value &input,
      const std::shared_ptr<Environment> &environment, const Node &call_node,
      std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto invoke_transform(const Function &function,
                                      std::vector<Value> arguments,
                                      const Node &call_node,
                                      std::size_t call_depth) -> Result<Value>;
  [[nodiscard]] auto value_list(const Value &value) -> std::vector<Value>;
  [[nodiscard]] auto function_arity(const Value &function) const noexcept
      -> std::size_t;
  [[nodiscard]] auto
  higher_order_arguments(const Value &function,
                         std::initializer_list<Value> candidates) const
      -> std::vector<Value>;
  [[nodiscard]] auto require_string(const Value &value, const Node &call_node,
                                    std::string_view function)
      -> Result<std::string>;
  [[nodiscard]] auto require_number(const Value &value, const Node &call_node,
                                    std::string_view function)
      -> Result<double>;
  [[nodiscard]] auto require_regex(const Value &value, const Node &call_node,
                                   std::string_view function)
      -> Result<std::shared_ptr<RegexValue>>;
  [[nodiscard]] auto
  next_regex_match(const RegexValue &regex, std::string_view input,
                   std::size_t start_offset, const Node &call_node)
      -> Result<std::optional<RegexMatch>>;
  [[nodiscard]] auto regex_limits() const noexcept -> RegexLimits;
  [[nodiscard]] auto key_string(const Node &node, const Value &value)
      -> Result<std::string>;
  [[nodiscard]] auto node(NodeId id) const -> const Node &;

  const ProgramData &program_;
  std::shared_ptr<const ProgramData> program_owner_;
  std::shared_ptr<EvaluationState> state_;
  Value root_;
  std::shared_ptr<Environment> base_environment_;
  std::shared_ptr<Environment> environment_;
  std::optional<Failure> initialization_failure_;
  std::size_t eval_depth_{};
  std::vector<std::optional<PendingTailCall>> tail_call_frames_;
};

} // namespace dagforge::jsonata::detail
