#pragma once

#include "../dto_mapper.hpp"
#include "dagforge/http/http_types.hpp"
#include "dagforge/util/json.hpp"

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge {
class Application;
} // namespace dagforge

namespace dagforge::workflow {
class WorkflowControlPlane;
class WorkflowRuntime;
} // namespace dagforge::workflow

namespace dagforge::api_detail::workflow_routes_detail {

struct PageRequest {
  std::size_t offset{0};
  std::size_t limit{100};
};

class WorkflowHttpRequest {
public:
  WorkflowHttpRequest(Application &app, const http::HttpRequest &request);

  [[nodiscard]] auto require_runtime() -> workflow::WorkflowRuntime *;
  [[nodiscard]] auto require_control_plane()
      -> workflow::WorkflowControlPlane *;

  template <typename Id>
  [[nodiscard]] auto require_path_id(std::string_view key,
                                     std::string_view missing_message = {})
      -> std::optional<Id> {
    if (failure_) {
      return std::nullopt;
    }
    auto value = request_.path_param(key);
    if (!value) {
      if (missing_message.empty()) {
        set_failure(error_response(400, missing_path_message(key)));
      } else {
        set_failure(error_response(400, missing_message));
      }
      return std::nullopt;
    }
    return Id{std::move(*value)};
  }

  template <typename T>
  [[nodiscard]] auto require_json(std::string_view invalid_message)
      -> std::optional<T> {
    if (failure_) {
      return std::nullopt;
    }
    auto parsed = parse_json_as<T>(request_.body_as_string());
    if (!parsed) {
      set_failure(error_response(400, invalid_message));
      return std::nullopt;
    }
    return std::move(*parsed);
  }

  template <typename T>
  [[nodiscard]] auto parse_json_or_default(std::string_view invalid_message)
      -> std::optional<T> {
    if (failure_) {
      return std::nullopt;
    }
    if (request_.body.empty()) {
      return T{};
    }
    return require_json<T>(invalid_message);
  }

  [[nodiscard]] auto page() const -> PageRequest;
  [[nodiscard]] auto idempotency_key(std::string body_value) const
      -> std::string;

  [[nodiscard]] auto failed() const noexcept -> bool {
    return failure_.has_value();
  }

  [[nodiscard]] auto take_failure() -> http::HttpResponse;

private:
  [[nodiscard]] static auto missing_path_message(std::string_view key)
      -> std::string;
  auto set_failure(http::HttpResponse response) -> void;

  Application &app_;
  const http::HttpRequest &request_;
  std::optional<http::HttpResponse> failure_;
};

} // namespace dagforge::api_detail::workflow_routes_detail
