#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#endif

namespace dagforge::workflow {

struct FailureArtifact {
  std::string name;
  ArtifactRef artifact;
};

struct ExecutionFailure {
  Error kind{Error::Unknown};
  std::string code{"unknown"};
  std::string message{"Execution failed"};
  JsonValue details = JsonValue::object_t{};
  std::vector<FailureArtifact> artifacts;
};

[[nodiscard]] auto normalize_execution_error(std::error_code error) noexcept
    -> Error;

[[nodiscard]] auto
make_execution_failure(Error kind, std::string code, std::string message,
                       JsonValue details = JsonValue::object_t{})
    -> ExecutionFailure;

[[nodiscard]] auto make_execution_failure(
    std::error_code cause, std::string code, std::string message,
    JsonValue details = JsonValue::object_t{}) -> ExecutionFailure;

[[nodiscard]] auto execution_failure_json(const ExecutionFailure &failure)
    -> JsonValue;

} // namespace dagforge::workflow
