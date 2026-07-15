#include "dagforge/workflow/execution_failure.hpp"

#include <cstdint>
#include <string>
#include <utility>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto normalized_kind(Error kind) noexcept -> Error {
  const auto value = std::to_underlying(kind);
  return value > std::to_underlying(Error::Success) &&
                 value <= std::to_underlying(Error::Unknown)
             ? kind
             : Error::Unknown;
}

auto ensure_object(JsonValue details) -> JsonValue {
  if (details.is_object()) {
    return details;
  }
  JsonValue object = JsonValue::object_t{};
  return object;
}

auto add_cause(JsonValue &details, std::error_code cause) -> void {
  JsonValue value = JsonValue::object_t{};
  value["category"] = std::string{cause.category().name()};
  value["value"] = static_cast<std::int64_t>(cause.value());
  value["message"] = cause.message();
  details["cause"] = std::move(value);
}

} // namespace

auto normalize_execution_error(std::error_code error) noexcept -> Error {
  if (error.category() == error_category() && error.value() > 0 &&
      error.value() <= std::to_underlying(Error::Unknown)) {
    return static_cast<Error>(error.value());
  }
  if (error == std::errc::operation_canceled) {
    return Error::Cancelled;
  }
  if (error == std::errc::timed_out) {
    return Error::Timeout;
  }
  if (error == std::errc::permission_denied) {
    return Error::Unauthorized;
  }
  if (error == std::errc::no_such_file_or_directory) {
    return Error::NotFound;
  }
  if (error == std::errc::not_enough_memory ||
      error == std::errc::no_space_on_device) {
    return Error::ResourceExhausted;
  }
  return Error::Unknown;
}

auto make_execution_failure(Error kind, std::string code, std::string message,
                            JsonValue details) -> ExecutionFailure {
  kind = normalized_kind(kind);
  if (code.empty()) {
    code = std::string{to_string_view(kind)};
  }
  if (message.empty()) {
    message = make_error_code(kind).message();
  }
  return ExecutionFailure{
      .kind = kind,
      .code = std::move(code),
      .message = std::move(message),
      .details = ensure_object(std::move(details)),
  };
}

auto make_execution_failure(std::error_code cause, std::string code,
                            std::string message, JsonValue details)
    -> ExecutionFailure {
  auto normalized_details = ensure_object(std::move(details));
  add_cause(normalized_details, cause);
  if (message.empty()) {
    message = cause.message();
  }
  return make_execution_failure(normalize_execution_error(cause),
                                std::move(code), std::move(message),
                                std::move(normalized_details));
}

auto execution_failure_json(const ExecutionFailure &failure) -> JsonValue {
  JsonValue value = JsonValue::object_t{};
  value["kind"] = std::string{to_string_view(failure.kind)};
  value["code"] = failure.code;
  value["message"] = failure.message;
  value["details"] = failure.details;
  JsonValue artifacts = JsonValue::array_t{};
  for (const auto &attachment : failure.artifacts) {
    JsonValue item = JsonValue::object_t{};
    item["name"] = attachment.name;
    item["artifact_id"] = attachment.artifact.artifact_id.str();
    item["media_type"] = attachment.artifact.media_type;
    item["size_bytes"] =
        static_cast<std::int64_t>(attachment.artifact.size_bytes);
    item["digest"] = attachment.artifact.digest;
    artifacts.get_array().push_back(std::move(item));
  }
  value["artifacts"] = std::move(artifacts);
  return value;
}

} // namespace dagforge::workflow
