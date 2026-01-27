#pragma once

#include "taskmaster/core/error.hpp"

#include <cstdint>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace taskmaster::http {

enum class HttpMethod : std::uint8_t {
  GET,
  POST,
  PUT,
  DELETE,
  PATCH,
  OPTIONS,
  HEAD
};

enum class HttpStatus : std::uint16_t {
  Ok = 200,
  Created = 201,
  Accepted = 202,
  NoContent = 204,

  MovedPermanently = 301,
  Found = 302,
  NotModified = 304,

  BadRequest = 400,
  Unauthorized = 401,
  Forbidden = 403,
  NotFound = 404,
  MethodNotAllowed = 405,
  Conflict = 409,

  InternalServerError = 500,
  NotImplemented = 501,
  BadGateway = 502,
  ServiceUnavailable = 503
};

using HttpHeaders = std::unordered_map<std::string, std::string, taskmaster::StringHash, taskmaster::StringEqual>;

class QueryParams {
public:
  QueryParams() = default;
  explicit QueryParams(std::string_view query_string);

  [[nodiscard]] auto get(std::string_view key) const
      -> std::optional<std::string>;
  [[nodiscard]] auto has(std::string_view key) const -> bool;
  [[nodiscard]] auto size() const -> std::size_t { return params_.size(); }

private:
  std::unordered_map<std::string, std::string, taskmaster::StringHash, taskmaster::StringEqual> params_;
};

struct HttpRequest {
  HttpMethod method{HttpMethod::GET};
  std::string path;
  std::string query_string;
  HttpHeaders headers;
  std::vector<uint8_t> body;
  std::unordered_map<std::string, std::string, taskmaster::StringHash, taskmaster::StringEqual> path_params;

  [[nodiscard]] auto header(std::string_view key) const
      -> std::optional<std::string>;
  [[nodiscard]] auto is_websocket_upgrade() const -> bool;
  [[nodiscard]] auto body_as_string() const -> std::string_view;
  [[nodiscard]] auto path_param(std::string_view key) const
      -> std::optional<std::string>;

  [[nodiscard]] auto serialize() const -> std::vector<uint8_t>;
};

struct HttpResponse {
  HttpStatus status{HttpStatus::Ok};
  HttpHeaders headers;
  std::vector<uint8_t> body;

  static auto ok() -> HttpResponse;
  static auto json(std::string_view json_str) -> HttpResponse;
  static auto not_found() -> HttpResponse;
  static auto bad_request() -> HttpResponse;
  static auto internal_error() -> HttpResponse;

  auto set_header(std::string key, std::string value) -> HttpResponse&;
  auto set_body(std::string body_str) -> HttpResponse&;
  auto set_body(std::vector<uint8_t> body_data) -> HttpResponse&;

  [[nodiscard]] auto serialize() const -> std::vector<uint8_t>;
};

[[nodiscard]] auto status_reason_phrase(HttpStatus status) -> std::string_view;

}  // namespace taskmaster::http

template <>
struct std::formatter<taskmaster::http::HttpMethod>
    : std::formatter<std::string_view> {
  auto format(taskmaster::http::HttpMethod method, auto& ctx) const {
    using enum taskmaster::http::HttpMethod;
    std::string_view name = [method] {
      switch (method) {
        case GET: return "GET";
        case POST: return "POST";
        case PUT: return "PUT";
        case DELETE: return "DELETE";
        case PATCH: return "PATCH";
        case OPTIONS: return "OPTIONS";
        case HEAD: return "HEAD";
      }
      return "UNKNOWN";
    }();
    return std::formatter<std::string_view>::format(name, ctx);
  }
};

template <>
struct std::formatter<taskmaster::http::HttpStatus>
    : std::formatter<std::uint16_t> {
  auto format(taskmaster::http::HttpStatus status, auto& ctx) const {
    return std::formatter<std::uint16_t>::format(
        static_cast<std::uint16_t>(status), ctx);
  }
};
