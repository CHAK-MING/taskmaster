#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace taskmaster::http {

enum class HttpMethod {
  GET,
  POST,
  PUT,
  DELETE,
  PATCH,
  OPTIONS,
  HEAD
};

enum class HttpStatus {
  Ok = 200,
  Created = 201,
  NoContent = 204,
  BadRequest = 400,
  NotFound = 404,
  InternalServerError = 500
};

using HttpHeaders = std::unordered_map<std::string, std::string>;

struct HttpRequest {
  HttpMethod method;
  std::string path;
  std::string query_string;
  HttpHeaders headers;
  std::vector<uint8_t> body;
  std::unordered_map<std::string, std::string> path_params;

  [[nodiscard]] auto header(std::string_view key) const
      -> std::optional<std::string>;
  [[nodiscard]] auto is_websocket_upgrade() const -> bool;
  [[nodiscard]] auto body_as_string() const -> std::string_view;
  [[nodiscard]] auto path_param(std::string_view key) const
      -> std::optional<std::string>;
};

class QueryParams {
public:
  explicit QueryParams(std::string_view query_string);

  [[nodiscard]] auto get(std::string_view key) const
      -> std::optional<std::string>;
  [[nodiscard]] auto has(std::string_view key) const -> bool;
  [[nodiscard]] auto size() const -> std::size_t { return params_.size(); }

private:
  std::unordered_map<std::string, std::string> params_;
};

struct HttpResponse {
  HttpStatus status;
  HttpHeaders headers;
  std::vector<uint8_t> body;

  static auto ok() -> HttpResponse;
  static auto json(std::string_view json_str) -> HttpResponse;
  static auto not_found() -> HttpResponse;
  static auto internal_error() -> HttpResponse;

  auto set_header(std::string key, std::string value) -> HttpResponse&;
  auto set_body(std::string body_str) -> HttpResponse&;
};

[[nodiscard]] auto method_to_string(HttpMethod method) -> std::string_view;
[[nodiscard]] auto status_to_int(HttpStatus status) -> int;

}  // namespace taskmaster::http
