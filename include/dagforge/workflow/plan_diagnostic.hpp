#pragma once

#include "dagforge/util/error_json.hpp"
#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

namespace dagforge::workflow {

struct ExecutorCompileFailure {
  Error kind{Error::Unknown};
  std::string code{"executor_compile_failed"};
  std::string description{"Executor configuration could not be compiled"};
  std::string path;
  JsonPayload details;

  [[nodiscard]] auto message() const noexcept -> std::string_view {
    return description;
  }
  [[nodiscard]] auto error_code() const noexcept -> std::error_code {
    return make_error_code(kind);
  }
};

struct PlanDiagnostic {
  Error kind{Error::Unknown};
  std::string code{"plan_operation_failed"};
  std::string description{"Workflow Plan operation failed"};
  std::string path;
  std::optional<WorkflowNodeId> node_id;
  std::optional<std::string> executor;
  JsonPayload details;

  [[nodiscard]] auto message() const noexcept -> std::string_view {
    return description;
  }
  [[nodiscard]] auto error_code() const noexcept -> std::error_code {
    return make_error_code(kind);
  }
};

template <typename T>
using ExecutorCompileResult = std::expected<T, ExecutorCompileFailure>;

template <typename T> using PlanResult = std::expected<T, PlanDiagnostic>;

template <typename T>
  requires ResultValue<std::decay_t<T>>
[[nodiscard]] constexpr auto plan_ok(T &&value) -> PlanResult<std::decay_t<T>> {
  return std::forward<T>(value);
}

[[nodiscard]] constexpr auto plan_ok() -> PlanResult<void> { return {}; }

[[nodiscard]] inline auto plan_fail(PlanDiagnostic diagnostic)
    -> std::unexpected<PlanDiagnostic> {
  return std::unexpected{std::move(diagnostic)};
}

template <typename T>
  requires ResultValue<std::decay_t<T>>
[[nodiscard]] constexpr auto executor_compile_ok(T &&value)
    -> ExecutorCompileResult<std::decay_t<T>> {
  return std::forward<T>(value);
}

[[nodiscard]] inline auto executor_compile_fail(ExecutorCompileFailure failure)
    -> std::unexpected<ExecutorCompileFailure> {
  return std::unexpected{std::move(failure)};
}

[[nodiscard]] inline auto normalize_plan_error(std::error_code error) noexcept
    -> Error {
  if (error.category() == error_category() && error.value() >= 0 &&
      error.value() <= static_cast<int>(Error::Unknown)) {
    const auto normalized = static_cast<Error>(error.value());
    return normalized == Error::Success ? Error::Unknown : normalized;
  }
  return Error::Unknown;
}

[[nodiscard]] inline auto make_executor_compile_failure(
    Error kind, std::string code, std::string description,
    std::string path = {}, JsonPayload details = {}) -> ExecutorCompileFailure {
  return ExecutorCompileFailure{
      .kind = kind,
      .code = std::move(code),
      .description = std::move(description),
      .path = std::move(path),
      .details = std::move(details),
  };
}

[[nodiscard]] inline auto make_executor_compile_failure(
    std::error_code cause, std::string code, std::string description,
    std::string path = {}, JsonPayload details = {}) -> ExecutorCompileFailure {
  return make_executor_compile_failure(normalize_plan_error(cause),
                                       std::move(code), std::move(description),
                                       std::move(path), std::move(details));
}

[[nodiscard]] inline auto
make_plan_diagnostic(Error kind, std::string code, std::string description,
                     std::string path = {},
                     std::optional<WorkflowNodeId> node_id = std::nullopt,
                     std::optional<std::string> executor = std::nullopt,
                     JsonPayload details = {}) -> PlanDiagnostic {
  return PlanDiagnostic{
      .kind = kind,
      .code = std::move(code),
      .description = std::move(description),
      .path = std::move(path),
      .node_id = std::move(node_id),
      .executor = std::move(executor),
      .details = std::move(details),
  };
}

[[nodiscard]] inline auto
make_plan_diagnostic(std::error_code cause, std::string code,
                     std::string description, std::string path = {},
                     std::optional<WorkflowNodeId> node_id = std::nullopt,
                     std::optional<std::string> executor = std::nullopt,
                     JsonPayload details = {}) -> PlanDiagnostic {
  return make_plan_diagnostic(normalize_plan_error(cause), std::move(code),
                              std::move(description), std::move(path),
                              std::move(node_id), std::move(executor),
                              std::move(details));
}

[[nodiscard]] inline auto operator==(const PlanDiagnostic &diagnostic,
                                     const std::error_code &error) noexcept
    -> bool {
  return diagnostic.error_code() == error;
}

[[nodiscard]] inline auto operator==(const std::error_code &error,
                                     const PlanDiagnostic &diagnostic) noexcept
    -> bool {
  return diagnostic == error;
}

[[nodiscard]] inline auto operator==(const ExecutorCompileFailure &failure,
                                     const std::error_code &error) noexcept
    -> bool {
  return failure.error_code() == error;
}

[[nodiscard]] inline auto
operator==(const std::error_code &error,
           const ExecutorCompileFailure &failure) noexcept -> bool {
  return failure == error;
}

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::PlanDiagnostic> {
  using T = dagforge::workflow::PlanDiagnostic;
  static constexpr auto value =
      object("kind", &T::kind, "code", &T::code, "message", &T::description,
             "path", &T::path, "node_id", &T::node_id, "executor", &T::executor,
             "details", &T::details);
};

} // namespace glz
