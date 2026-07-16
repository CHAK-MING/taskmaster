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

[[nodiscard]] auto normalized_details(JsonPayload details) -> JsonPayload {
  return details.is_object() ? std::move(details) : JsonPayload{};
}

[[nodiscard]] auto cause_details(std::error_code cause) -> FailureCause {
  return FailureCause{
      .category = cause.category().name(),
      .value = static_cast<std::int64_t>(cause.value()),
      .message = cause.message(),
  };
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
                            JsonPayload details) -> ExecutionFailure {
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
      .details = normalized_details(std::move(details)),
  };
}

auto make_execution_failure(std::error_code cause, std::string code,
                            std::string message)
    -> ExecutionFailure {
  auto details = JsonPayload::from(glz::obj{"cause", cause_details(cause)});
  if (message.empty()) {
    message = cause.message();
  }
  return make_execution_failure(normalize_execution_error(cause),
                                std::move(code), std::move(message),
                                details ? std::move(*details)
                                        : JsonPayload{});
}

} // namespace dagforge::workflow
