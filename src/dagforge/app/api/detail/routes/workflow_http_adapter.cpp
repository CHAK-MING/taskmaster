#include "workflow_http_adapter.hpp"

#include "dagforge/app/application.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <algorithm>
#include <cassert>
#include <charconv>
#include <format>
#include <system_error>

namespace dagforge::api_detail::workflow_routes_detail {
namespace {

[[nodiscard]] auto query_number(std::string_view query, std::string_view key)
    -> std::optional<std::size_t> {
  while (!query.empty()) {
    const auto separator = query.find('&');
    const auto item = query.substr(0, separator);
    const auto equals = item.find('=');
    if (equals != std::string_view::npos && item.substr(0, equals) == key) {
      std::size_t value = 0;
      const auto token = item.substr(equals + 1);
      const auto [end, error] =
          std::from_chars(token.data(), token.data() + token.size(), value);
      if (error == std::errc{} && end == token.data() + token.size()) {
        return value;
      }
      return std::nullopt;
    }
    if (separator == std::string_view::npos) {
      break;
    }
    query.remove_prefix(separator + 1);
  }
  return std::nullopt;
}

[[nodiscard]] auto unavailable() -> http::HttpResponse {
  return error_response(503, "Workflow runtime is disabled");
}

} // namespace

WorkflowHttpRequest::WorkflowHttpRequest(Application &app,
                                         const http::HttpRequest &request)
    : app_(app), request_(request) {}

auto WorkflowHttpRequest::require_runtime() -> workflow::WorkflowRuntime * {
  if (failure_) {
    return nullptr;
  }
  auto *value = app_.workflow_runtime();
  if (!value) {
    set_failure(unavailable());
  }
  return value;
}

auto WorkflowHttpRequest::require_control_plane()
    -> workflow::WorkflowControlPlane * {
  if (failure_) {
    return nullptr;
  }
  auto *value = app_.workflow_control_plane();
  if (!value) {
    set_failure(unavailable());
  }
  return value;
}

auto WorkflowHttpRequest::page() const -> PageRequest {
  const auto query = std::string_view{request_.query_string};
  return PageRequest{
      .offset = query_number(query, "offset").value_or(0),
      .limit = std::clamp<std::size_t>(
          query_number(query, "limit").value_or(100), 1, 1000),
  };
}

auto WorkflowHttpRequest::idempotency_key(std::string body_value) const
    -> std::string {
  if (!body_value.empty()) {
    return body_value;
  }
  if (auto header = request_.header("Idempotency-Key"); header) {
    return std::move(*header);
  }
  return {};
}

auto WorkflowHttpRequest::take_failure() -> http::HttpResponse {
  assert(failure_.has_value());
  return std::move(*failure_);
}

auto WorkflowHttpRequest::missing_path_message(std::string_view key)
    -> std::string {
  return std::format("Missing {}", key);
}

auto WorkflowHttpRequest::set_failure(http::HttpResponse response) -> void {
  if (!failure_) {
    failure_ = std::move(response);
  }
}

} // namespace dagforge::api_detail::workflow_routes_detail
