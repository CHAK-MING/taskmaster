#pragma once

#include "dagforge/http/http_types.hpp"
#include "dagforge/io/result.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/workflow/execution_failure.hpp"
#include "dagforge/workflow/plan_diagnostic.hpp"

#include <string>
#include <string_view>

namespace dagforge::api_detail {

inline auto status_from_error(const std::error_code &ec) -> http::HttpStatus {
  if (ec.category() == io::io_error_category()) {
    switch (static_cast<io::IoError>(ec.value())) {
    case io::IoError::TimedOut:
    case io::IoError::Cancelled:
    case io::IoError::ConnectionReset:
    case io::IoError::BrokenPipe:
      return http::HttpStatus::ServiceUnavailable;
    case io::IoError::InvalidArgument:
      return http::HttpStatus::BadRequest;
    case io::IoError::EndOfFile:
      return http::HttpStatus::NotFound;
    default:
      return http::HttpStatus::InternalServerError;
    }
  }

  if (ec.category() == error_category()) {
    switch (static_cast<Error>(ec.value())) {
    case Error::NotFound:
    case Error::FileNotFound:
      return http::HttpStatus::NotFound;
    case Error::InvalidArgument:
    case Error::ParseError:
    case Error::InvalidUrl:
      return http::HttpStatus::BadRequest;
    case Error::Unauthorized:
      return http::HttpStatus::Unauthorized;
    case Error::ReadOnly:
      return http::HttpStatus::Forbidden;
    case Error::AlreadyExists:
    case Error::InvalidState:
      return http::HttpStatus::Conflict;
    case Error::Timeout:
    case Error::Cancelled:
    case Error::ResourceExhausted:
    case Error::QueueFull:
    case Error::RateLimited:
      return http::HttpStatus::ServiceUnavailable;
    case Error::Unsupported:
      return http::HttpStatus::NotImplemented;
    default:
      return http::HttpStatus::InternalServerError;
    }
  }
  return http::HttpStatus::InternalServerError;
}

inline auto text_response(std::string body, http::HttpStatus status,
                          std::string_view content_type) -> http::HttpResponse {
  http::HttpResponse response;
  response.status = status;
  response.set_header("Content-Type", std::string{content_type});
  response.set_body(std::move(body));
  return response;
}

inline auto error_response(int code, std::string_view message)
    -> http::HttpResponse;

template <typename T>
inline auto typed_json_response(const T &value,
                                http::HttpStatus status = http::HttpStatus::Ok)
    -> http::HttpResponse {
  auto serialized = serialize_json(value);
  if (!serialized) {
    log::error("JSON serialization failed for API response: {}",
               serialized.error().message());
    return error_response(500, "JSON serialization failed");
  }
  http::HttpResponse response;
  response.status = status;
  response.set_header("Content-Type", "application/json");
  response.set_body(std::move(*serialized));
  return response;
}

inline auto error_response(int code, std::string_view message)
    -> http::HttpResponse {
  Error kind = Error::Unknown;
  std::string stable_code{"http_error"};
  switch (code) {
  case 400:
    kind = Error::InvalidArgument;
    stable_code = "invalid_request";
    break;
  case 401:
    kind = Error::Unauthorized;
    stable_code = "unauthorized";
    break;
  case 403:
    kind = Error::Unauthorized;
    stable_code = "forbidden";
    break;
  case 404:
    kind = Error::NotFound;
    stable_code = "not_found";
    break;
  case 409:
    kind = Error::AlreadyExists;
    stable_code = "conflict";
    break;
  case 413:
    kind = Error::ResourceExhausted;
    stable_code = "payload_too_large";
    break;
  case 429:
    kind = Error::RateLimited;
    stable_code = "rate_limited";
    break;
  case 503:
    kind = Error::SystemNotRunning;
    stable_code = "service_unavailable";
    break;
  default:
    break;
  }
  auto failure = workflow::make_execution_failure(kind, std::move(stable_code),
                                                  std::string{message});
  return typed_json_response(glz::obj{"error", failure},
                             static_cast<http::HttpStatus>(code));
}

inline auto result_error_response(const std::error_code &error)
    -> http::HttpResponse {
  auto failure = workflow::make_execution_failure(error, {}, error.message());
  return typed_json_response(glz::obj{"error", failure},
                             status_from_error(error));
}

inline auto
admission_status_from_diagnostic(const workflow::PlanDiagnostic &diagnostic)
    -> http::HttpStatus {
  if (diagnostic.code == "plan_persist_failed" ||
      diagnostic.code == "plan_digest_failed" ||
      diagnostic.code == "plan_digest_mismatch" ||
      diagnostic.code == "executor_compiled_config_invalid" ||
      diagnostic.code.ends_with("_encode_failed")) {
    return http::HttpStatus::InternalServerError;
  }
  switch (diagnostic.kind) {
  case Error::Unauthorized:
  case Error::ReadOnly:
    return http::HttpStatus::Forbidden;
  case Error::InvalidArgument:
  case Error::ParseError:
  case Error::InvalidUrl:
  case Error::NotFound:
  case Error::AlreadyExists:
  case Error::CycleDetected:
  case Error::ResourceExhausted:
  case Error::Unsupported:
    return http::HttpStatus::BadRequest;
  default:
    return http::HttpStatus::InternalServerError;
  }
}

inline auto plan_error_response(const workflow::PlanDiagnostic &diagnostic)
    -> http::HttpResponse {
  return typed_json_response(glz::obj{"error", diagnostic},
                             admission_status_from_diagnostic(diagnostic));
}

} // namespace dagforge::api_detail
