#pragma once

#include "dagforge/util/json.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>

namespace dagforge::jsonata {

struct Version {
  std::uint16_t major{};
  std::uint16_t minor{};
  std::uint16_t patch{};

  auto operator==(const Version &) const -> bool = default;
};

inline constexpr Version kCompatibilityVersion{2, 2, 2};

enum class FailureKind : std::uint8_t {
  Syntax,
  Type,
  Dynamic,
  Host,
};

struct Failure {
  FailureKind kind{FailureKind::Host};
  std::string code;
  std::string message;
  std::size_t byte_offset{};
  std::size_t position{};
  std::string token;

  auto operator==(const Failure &) const -> bool = default;
};

template <typename T> using DiagnosticResult = std::expected<T, Failure>;

struct CompileLimits {
  std::size_t max_source_bytes{1024 * 1024};
  std::size_t max_tokens{262144};
  std::size_t max_nodes{262144};
  std::size_t max_string_bytes{8 * 1024 * 1024};
  std::size_t max_nesting_depth{1024};
  std::size_t max_program_bytes{64 * 1024 * 1024};
};

struct CompileRequest {
  std::string source;
  CompileLimits limits;
};

struct Binding {
  std::string name;
  std::reference_wrapper<const JsonValue> value;
};

struct EvaluationLimits {
  std::uint64_t max_steps{10'000'000};
  std::size_t max_call_depth{1024};
  std::size_t max_sequence_items{10'000'000};
  std::size_t max_value_nodes{2'000'000};
  std::size_t max_string_bytes{64 * 1024 * 1024};
  // Cumulative lexical bindings created during one evaluation. This is an
  // allocation/work budget, not a count of bindings that are simultaneously
  // live.
  std::size_t max_environment_bindings_created{262144};
  std::size_t max_regex_matches{1'000'000};
  std::size_t max_eval_nesting{64};
  std::chrono::steady_clock::duration timeout{std::chrono::seconds(5)};
};

struct EvaluationRequest {
  // Evaluation is synchronous. The input, binding span, and each referenced
  // JsonValue must remain alive until Program::evaluate() returns.
  std::optional<std::reference_wrapper<const JsonValue>> input;
  std::span<const Binding> bindings;
  std::chrono::system_clock::time_point timestamp{
      std::chrono::system_clock::now()};
  std::stop_token stop_token;
  EvaluationLimits limits;
};

struct EvaluationStatistics {
  std::uint64_t steps{};
  std::size_t peak_call_depth{};
  std::size_t peak_sequence_items{};
  std::size_t peak_value_nodes{};
  std::size_t peak_string_bytes{};
};

enum class EvaluationValueKind : std::uint8_t {
  Undefined,
  Json,
  Function,
};

struct EvaluationSuccess {
  EvaluationValueKind kind{EvaluationValueKind::Undefined};
  std::optional<JsonValue> value;
  EvaluationStatistics statistics;
};

class Program {
public:
  Program() = default;

  [[nodiscard]] static auto compile(CompileRequest request)
      -> DiagnosticResult<Program>;

  [[nodiscard]] auto evaluate(const EvaluationRequest &request) const
      -> DiagnosticResult<EvaluationSuccess>;

  [[nodiscard]] auto source() const noexcept -> std::string_view;
  [[nodiscard]] auto valid() const noexcept -> bool;

private:
  class Impl;
  explicit Program(std::shared_ptr<const Impl> impl);

  std::shared_ptr<const Impl> impl_;
};

} // namespace dagforge::jsonata
