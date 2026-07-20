#include "evaluation.hpp"

#include "dagforge/jsonata/program.hpp"
#include "dagforge/util/json.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::executors::transform::detail {

struct NodeConfig {
  std::string expression;
};

struct CompiledTransform {
  jsonata::Program program;
  std::vector<std::string> inputs;
  std::vector<std::string> outputs;
};

} // namespace dagforge::executors::transform::detail

namespace glz {

template <> struct meta<dagforge::executors::transform::detail::NodeConfig> {
  using T = dagforge::executors::transform::detail::NodeConfig;
  static constexpr auto value = object("expression", &T::expression);
};

} // namespace glz

namespace dagforge::executors::transform::detail {

namespace {

inline constexpr std::size_t kTransformMaxCallDepth = 128;

// These stable JSONata diagnostics form the Transform-to-Workflow projection
// contract. Exact language diagnostics remain available in failure details.
inline constexpr std::string_view kCancelledDiagnostic = "H1001";
inline constexpr std::string_view kTimeoutDiagnostic = "D1012";
inline constexpr std::string_view kCallDepthDiagnostic = "D1011";
inline constexpr std::string_view kValueBudgetDiagnostic = "D2015";

[[nodiscard]] auto failure_kind_name(jsonata::FailureKind kind) noexcept
    -> std::string_view {
  switch (kind) {
  case jsonata::FailureKind::Syntax:
    return "syntax";
  case jsonata::FailureKind::Type:
    return "type";
  case jsonata::FailureKind::Dynamic:
    return "dynamic";
  case jsonata::FailureKind::Host:
    return "host";
  }
  return "host";
}

[[nodiscard]] auto jsonata_details(const jsonata::Failure &failure)
    -> Result<JsonPayload> {
  auto details = JsonPayload::from(glz::obj{
      "jsonata_kind", failure_kind_name(failure.kind), "jsonata_code",
      failure.code, "jsonata_message", failure.message, "byte_offset",
      static_cast<std::uint64_t>(failure.byte_offset), "position",
      static_cast<std::uint64_t>(failure.position), "token", failure.token});
  return details ? ok(std::move(*details)) : fail(details.error());
}

[[nodiscard]] auto compile_error(const jsonata::Failure &failure) noexcept
    -> Error {
  if (failure.kind == jsonata::FailureKind::Host &&
      failure.code.starts_with("H11")) {
    return Error::ResourceExhausted;
  }
  return Error::InvalidArgument;
}

[[nodiscard]] auto resource_diagnostic(std::string_view code) noexcept -> bool {
  return code.starts_with("H2") || code == kCallDepthDiagnostic ||
         code == kValueBudgetDiagnostic;
}

[[nodiscard]] auto evaluation_failure(const jsonata::Failure &failure)
    -> workflow::ExecutionFailure {
  auto details = jsonata_details(failure);
  if (!details) {
    return workflow::make_execution_failure(
        details.error(), "transform_diagnostic_encode_failed",
        "Transform failure diagnostics could not be encoded");
  }
  if (failure.code == kCancelledDiagnostic) {
    return workflow::make_execution_failure(
        Error::Cancelled, "transform_cancelled",
        "Transform evaluation was cancelled", std::move(*details));
  }
  if (failure.code == kTimeoutDiagnostic) {
    return workflow::make_execution_failure(
        Error::Timeout, "transform_timed_out", "Transform evaluation timed out",
        std::move(*details));
  }
  if (resource_diagnostic(failure.code)) {
    return workflow::make_execution_failure(
        Error::ResourceExhausted, "transform_resource_exhausted",
        "Transform evaluation exceeded a resource limit", std::move(*details));
  }
  return workflow::make_execution_failure(
      failure.kind == jsonata::FailureKind::Host ? Error::Unknown
                                                 : Error::InvalidArgument,
      "transform_evaluation_failed", failure.message, std::move(*details));
}

[[nodiscard]] auto workflow_value_to_json(const workflow::WorkflowValue &value)
    -> Result<JsonValue> {
  return std::visit(
      [](const auto &typed) -> Result<JsonValue> {
        using T = std::remove_cvref_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          JsonValue converted;
          converted = nullptr;
          return ok(std::move(converted));
        } else if constexpr (std::same_as<T, bool> ||
                             std::same_as<T, std::int64_t> ||
                             std::same_as<T, std::string>) {
          JsonValue converted;
          converted = typed;
          return ok(std::move(converted));
        } else if constexpr (std::same_as<T, double>) {
          if (!std::isfinite(typed)) {
            return fail(Error::ProtocolError);
          }
          JsonValue converted;
          converted = typed;
          return ok(std::move(converted));
        } else if constexpr (std::same_as<T, JsonPayload>) {
          return typed.materialize();
        } else {
          if (typed.size_bytes >
              static_cast<std::uint64_t>(
                  std::numeric_limits<std::int64_t>::max())) {
            return fail(Error::ResourceExhausted);
          }
          JsonValue artifact = JsonValue::object_t{};
          artifact["type"] = "artifact";
          artifact["artifact_id"] = typed.artifact_id.str();
          artifact["media_type"] = typed.media_type;
          artifact["size_bytes"] = static_cast<std::int64_t>(typed.size_bytes);
          artifact["digest"] = typed.digest;
          return ok(std::move(artifact));
        }
      },
      value);
}

[[nodiscard]] auto json_to_workflow_value(JsonValue value)
    -> Result<workflow::WorkflowValue> {
  if (value.is_null()) {
    return ok(workflow::WorkflowValue{std::in_place_index<0>});
  }
  if (value.is_boolean()) {
    return ok(workflow::WorkflowValue{value.get<bool>()});
  }
  if (const auto *integer = value.get_if<std::int64_t>()) {
    return ok(workflow::WorkflowValue{*integer});
  }
  if (const auto *number = value.get_if<double>()) {
    if (!std::isfinite(*number)) {
      return fail(Error::ProtocolError);
    }
    return ok(workflow::WorkflowValue{*number});
  }
  if (value.is_string()) {
    return ok(workflow::WorkflowValue{std::move(value.get_string())});
  }
  auto encoded = JsonPayload::from(value);
  if (!encoded) {
    return fail(encoded.error());
  }
  return ok(workflow::WorkflowValue{std::move(*encoded)});
}

[[nodiscard]] auto input_document(const workflow::ExecutorInputs &inputs)
    -> Result<JsonValue> {
  std::vector<std::pair<std::string_view,
                        const std::shared_ptr<const workflow::WorkflowValue> *>>
      ordered;
  ordered.reserve(inputs.size());
  for (const auto &[name, value] : inputs) {
    ordered.emplace_back(name, &value);
  }
  std::ranges::sort(ordered, {}, &decltype(ordered)::value_type::first);

  JsonValue document = JsonValue::object_t{};
  for (const auto &[name, value] : ordered) {
    if (value == nullptr || !*value) {
      return fail(Error::InvalidArgument);
    }
    auto converted = workflow_value_to_json(**value);
    if (!converted) {
      return fail(converted.error());
    }
    document[name] = std::move(*converted);
  }
  return ok(std::move(document));
}

[[nodiscard]] auto input_bindings(const JsonValue &document,
                                  std::span<const std::string> input_names)
    -> Result<std::vector<jsonata::Binding>> {
  if (!document.is_object()) {
    return fail(Error::InvalidState);
  }
  std::vector<jsonata::Binding> bindings;
  bindings.reserve(input_names.size());
  for (const auto &name : input_names) {
    const auto found = document.get_object().find(name);
    if (found == document.get_object().end()) {
      return fail(Error::InvalidArgument);
    }
    bindings.push_back(
        jsonata::Binding{.name = name, .value = std::cref(found->second)});
  }
  return ok(std::move(bindings));
}

[[nodiscard]] auto
request_matches_compiled(const CompiledTransform &compiled,
                         const workflow::TaskExecutionRequest &request)
    -> bool {
  if (compiled.outputs.size() != request.outputs.size() ||
      compiled.inputs.size() != request.inputs.size()) {
    return false;
  }
  for (std::size_t index = 0; index < compiled.outputs.size(); ++index) {
    if (request.outputs[index] != compiled.outputs[index]) {
      return false;
    }
  }
  return std::ranges::all_of(compiled.inputs, [&](const std::string &input) {
    return request.inputs.contains(input);
  });
}

[[nodiscard]] auto project_outputs(jsonata::EvaluationSuccess evaluation,
                                   std::span<const WorkflowPortId> requested)
    -> workflow::TaskExecutionResult {
  if (evaluation.kind == jsonata::EvaluationValueKind::Undefined) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::Incomplete, "transform_result_undefined",
        "Transform expression evaluated to undefined"));
  }
  if (evaluation.kind == jsonata::EvaluationValueKind::Function) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::ProtocolError, "transform_result_not_json",
        "Transform expression produced a function value"));
  }
  if (!evaluation.value) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::ProtocolError, "transform_result_missing",
        "Transform expression did not produce a JSON value"));
  }

  if (requested.size() == 1) {
    auto converted = json_to_workflow_value(std::move(*evaluation.value));
    if (!converted) {
      return workflow::task_failed(workflow::make_execution_failure(
          converted.error(), "transform_result_encode_failed",
          "Transform result could not be converted to a Workflow value"));
    }
    workflow::ExecutorOutputs outputs;
    outputs.emplace_back(requested.front().clone(), std::move(*converted));
    return workflow::task_succeeded(std::move(outputs));
  }

  if (!evaluation.value->is_object() ||
      evaluation.value->get_object().size() != requested.size()) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::ProtocolError, "transform_output_shape_invalid",
        "Multi-output Transform expressions must return an exact output "
        "object"));
  }

  workflow::ExecutorOutputs outputs;
  outputs.reserve(requested.size());
  auto &result_object = evaluation.value->get_object();
  for (const auto &port : requested) {
    const auto found = result_object.find(port.str());
    if (found == result_object.end()) {
      return workflow::task_failed(workflow::make_execution_failure(
          Error::ProtocolError, "transform_output_shape_invalid",
          "Multi-output Transform result is missing a declared output"));
    }
    auto converted = json_to_workflow_value(std::move(found->second));
    if (!converted) {
      return workflow::task_failed(workflow::make_execution_failure(
          converted.error(), "transform_result_encode_failed",
          "Transform output could not be converted to a Workflow value"));
    }
    outputs.emplace_back(port.clone(), std::move(*converted));
  }
  return workflow::task_succeeded(std::move(outputs));
}

[[nodiscard]] auto cancelled_failure() -> workflow::TaskExecutionResult {
  return workflow::task_failed(
      workflow::make_execution_failure(Error::Cancelled, "transform_cancelled",
                                       "Transform evaluation was cancelled"));
}

[[nodiscard]] auto timeout_failure() -> workflow::TaskExecutionResult {
  return workflow::task_failed(workflow::make_execution_failure(
      Error::Timeout, "transform_timed_out", "Transform evaluation timed out"));
}

} // namespace

auto describe_transform() -> Result<workflow::ExecutorDescription> {
  auto schema = json_schema_payload<NodeConfig>();
  if (!schema) {
    return fail(schema.error());
  }
  auto example = JsonPayload::from(
      glz::obj{"expression", "{\"total\": $amount, \"ok\": true}"});
  if (!example) {
    return fail(example.error());
  }
  auto constraints = JsonPayload::from(glz::obj{
      "language", "jsonata", "language_version", "2.2.2", "input_protocol",
      "root-object-and-lexical-bindings", "single_output_protocol",
      "complete-result", "multi_output_protocol", "exact-object",
      "artifact_access", "metadata-only"});
  if (!constraints) {
    return fail(constraints.error());
  }
  return ok(workflow::ExecutorDescription{
      .type = "transform",
      .summary = "Evaluate a JSONata expression over Workflow inputs",
      .config_schema = std::move(*schema),
      .examples = {std::move(*example)},
      .constraints = std::move(*constraints),
  });
}

auto compile_transform(JsonPayload config,
                       workflow::ExecutorCompileContext context)
    -> workflow::ExecutorCompileResult<workflow::CompiledExecutorConfig> {
  auto parsed = parse_json_as<NodeConfig>(config.encoded());
  if (!parsed) {
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            parsed.error(), "transform_config_invalid",
            "Transform configuration does not match the expected schema"));
  }
  if (parsed->expression.empty()) {
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            Error::InvalidArgument, "transform_expression_required",
            "Transform configuration requires a non-empty expression",
            "/expression"));
  }
  if (context.outputs.empty()) {
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            Error::InvalidArgument, "transform_outputs_required",
            "Transform nodes require at least one declared output"));
  }

  std::unordered_set<std::string> input_names;
  std::vector<std::string> inputs;
  inputs.reserve(context.inputs.size());
  for (const auto &binding : context.inputs) {
    if (binding.input.empty() ||
        !input_names.emplace(binding.input.str()).second) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "transform_input_contract_invalid",
              "Transform input names must be non-empty and unique"));
    }
    inputs.push_back(binding.input.str());
  }
  std::unordered_set<std::string> output_names;
  std::vector<std::string> outputs;
  outputs.reserve(context.outputs.size());
  for (const auto &output : context.outputs) {
    if (output.empty() || !output_names.emplace(output.str()).second) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              Error::InvalidArgument, "transform_output_contract_invalid",
              "Transform output names must be non-empty and unique"));
    }
    outputs.push_back(output.str());
  }

  auto program = jsonata::Program::compile(
      jsonata::CompileRequest{.source = parsed->expression});
  if (!program) {
    auto details = jsonata_details(program.error());
    if (!details) {
      return workflow::executor_compile_fail(
          workflow::make_executor_compile_failure(
              details.error(), "transform_diagnostic_encode_failed",
              "Transform compile diagnostics could not be encoded",
              "/expression"));
    }
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            compile_error(program.error()), "transform_expression_invalid",
            program.error().message, "/expression", std::move(*details)));
  }
  auto encoded = JsonPayload::from(*parsed);
  if (!encoded) {
    return workflow::executor_compile_fail(
        workflow::make_executor_compile_failure(
            encoded.error(), "transform_config_encode_failed",
            "Transform compiled configuration could not be encoded"));
  }
  return workflow::executor_compile_ok(workflow::CompiledExecutorConfig::make(
      std::move(*encoded), CompiledTransform{.program = std::move(*program),
                                             .inputs = std::move(inputs),
                                             .outputs = std::move(outputs)}));
}

auto validate_transform_request(const workflow::TaskExecutionRequest &request)
    -> Result<void> {
  const auto *compiled = request.config.get<CompiledTransform>();
  if (compiled == nullptr) {
    return fail(Error::InvalidState);
  }
  if (request.instance_id.empty() ||
      !request_matches_compiled(*compiled, request)) {
    return fail(Error::InvalidArgument);
  }
  return ok();
}

auto evaluate_transform(const workflow::TaskExecutionRequest &request,
                        std::stop_token stop_token,
                        std::chrono::steady_clock::time_point accepted_at)
    -> workflow::TaskExecutionResult {
  if (stop_token.stop_requested()) {
    return cancelled_failure();
  }
  const auto timeout =
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          request.timeout);
  const auto remaining_timeout =
      [&]() -> std::optional<std::chrono::steady_clock::duration> {
    const auto elapsed = std::chrono::steady_clock::now() - accepted_at;
    if (timeout <= std::chrono::steady_clock::duration::zero() ||
        elapsed >= timeout) {
      return std::nullopt;
    }
    return timeout - elapsed;
  };
  if (!remaining_timeout()) {
    return timeout_failure();
  }

  auto input = input_document(request.inputs);
  if (!input) {
    return workflow::task_failed(workflow::make_execution_failure(
        input.error(), "transform_input_invalid",
        "Transform inputs could not be converted to JSON"));
  }
  const auto *compiled = request.config.get<CompiledTransform>();
  if (compiled == nullptr) {
    return workflow::task_failed(workflow::make_execution_failure(
        Error::InvalidState, "transform_config_missing",
        "Transform request did not contain a compiled expression"));
  }
  auto bindings = input_bindings(*input, compiled->inputs);
  if (!bindings) {
    return workflow::task_failed(workflow::make_execution_failure(
        bindings.error(), "transform_input_invalid",
        "Transform inputs could not be bound to JSONata variables"));
  }
  const auto evaluation_timeout = remaining_timeout();
  if (!evaluation_timeout) {
    return timeout_failure();
  }

  jsonata::EvaluationRequest evaluation{
      .input = std::cref(*input),
      .bindings = *bindings,
      .stop_token = stop_token,
  };
  evaluation.limits.max_call_depth = kTransformMaxCallDepth;
  evaluation.limits.timeout = *evaluation_timeout;
  auto result = compiled->program.evaluate(evaluation);
  if (!result) {
    return workflow::task_failed(evaluation_failure(result.error()));
  }
  if (!remaining_timeout()) {
    return timeout_failure();
  }
  auto projected = project_outputs(std::move(*result), request.outputs);
  return remaining_timeout() ? std::move(projected) : timeout_failure();
}

} // namespace dagforge::executors::transform::detail
