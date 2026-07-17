#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/string_hash.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/beast/http/status.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <format>
#include <iterator>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::http {

enum class HttpMethod : std::uint8_t {
  GET,
  POST,
  PUT,
  DELETE,
  PATCH,
  OPTIONS,
  HEAD
};

[[nodiscard]] constexpr auto http_method_name(HttpMethod method) noexcept
    -> std::string_view {
  using enum HttpMethod;
  switch (method) {
  case GET:
    return "GET";
  case POST:
    return "POST";
  case PUT:
    return "PUT";
  case DELETE:
    return "DELETE";
  case PATCH:
    return "PATCH";
  case OPTIONS:
    return "OPTIONS";
  case HEAD:
    return "HEAD";
  }
  return "UNKNOWN";
}

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
  PayloadTooLarge = 413,
  TooManyRequests = 429,

  InternalServerError = 500,
  NotImplemented = 501,
  BadGateway = 502,
  ServiceUnavailable = 503
};

class HttpHeaders {
public:
  struct Field {
    std::string name;
    std::string value;
  };

  using const_iterator = std::vector<Field>::const_iterator;

  HttpHeaders() = default;
  HttpHeaders(std::initializer_list<Field> fields) : fields_(fields) {}

  auto add(std::string name, std::string value) -> void {
    fields_.push_back(Field{std::move(name), std::move(value)});
  }

  auto set(std::string name, std::string value) -> void {
    std::erase_if(fields_, [&](const Field &field) {
      return boost::algorithm::iequals(field.name, name);
    });
    add(std::move(name), std::move(value));
  }

  [[nodiscard]] auto get(std::string_view name) const -> Result<std::string> {
    for (const auto &field : fields_) {
      if (boost::algorithm::iequals(field.name, name)) {
        return ok(field.value);
      }
    }
    return fail(Error::NotFound);
  }

  [[nodiscard]] auto contains(std::string_view name) const -> bool {
    return std::ranges::any_of(fields_, [&](const Field &field) {
      return boost::algorithm::iequals(field.name, name);
    });
  }

  [[nodiscard]] auto begin() const noexcept -> const_iterator {
    return fields_.begin();
  }

  [[nodiscard]] auto end() const noexcept -> const_iterator {
    return fields_.end();
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return fields_.size();
  }

private:
  std::vector<Field> fields_;
};

[[nodiscard]] inline auto status_reason_phrase(HttpStatus status)
    -> std::string_view {
  namespace beast_http = boost::beast::http;
  return beast_http::obsolete_reason(static_cast<beast_http::status>(status));
}

struct HttpRequest {
  HttpMethod method{HttpMethod::GET};
  std::string path;
  std::string query_string;
  int version_major{1};
  int version_minor{1};
  HttpHeaders headers;
  std::vector<std::uint8_t> body;
  // Mutable: populated by Router during const route() method
  mutable std::unordered_map<std::string, std::string, dagforge::StringHash,
                             dagforge::StringEqual>
      path_params;

  [[nodiscard]] auto header(std::string_view key) const -> Result<std::string> {
    return headers.get(key);
  }

  [[nodiscard]] auto body_as_string() const -> std::string_view {
    return {reinterpret_cast<const char *>(body.data()), body.size()};
  }

  [[nodiscard]] auto path_param(std::string_view key) const
      -> Result<std::string> {
    auto it = path_params.find(key);
    if (it != path_params.end()) {
      return ok(it->second);
    }
    return fail(Error::NotFound);
  }

  [[nodiscard]] auto serialize() const -> std::vector<std::uint8_t> {
    std::vector<std::uint8_t> result;

    result.reserve(256 + body.size());
    std::format_to(std::back_inserter(result), "{} {} HTTP/1.1\r\n",
                   http_method_name(method), path);

    const bool has_content_length = headers.contains("Content-Length");
    const bool has_host = headers.contains("Host");

    for (const auto &field : headers) {
      std::format_to(std::back_inserter(result), "{}: {}\r\n", field.name,
                     field.value);
    }

    if (!has_host) {
      constexpr std::string_view host_header = "Host: localhost\r\n";
      result.insert(result.end(), host_header.begin(), host_header.end());
    }

    if (!has_content_length && !body.empty()) {
      std::format_to(std::back_inserter(result), "Content-Length: {}\r\n",
                     body.size());
    }

    result.emplace_back('\r');
    result.emplace_back('\n');
    result.insert(result.end(), body.begin(), body.end());
    return result;
  }
};

struct HttpResponse {
  HttpStatus status{HttpStatus::Ok};
  HttpHeaders headers;
  std::vector<std::uint8_t> body;

  [[nodiscard]] static auto ok() -> HttpResponse {
    return {.status = HttpStatus::Ok, .headers = {}, .body = {}};
  }

  [[nodiscard]] static auto json(std::string_view json_str) -> HttpResponse {
    HttpResponse resp{.status = HttpStatus::Ok, .headers = {}, .body = {}};
    resp.headers.set("Content-Type", "application/json");
    resp.body.assign(json_str.begin(), json_str.end());
    return resp;
  }

  [[nodiscard]] static auto not_found() -> HttpResponse {
    return {.status = HttpStatus::NotFound, .headers = {}, .body = {}};
  }

  [[nodiscard]] static auto bad_request() -> HttpResponse {
    return {.status = HttpStatus::BadRequest, .headers = {}, .body = {}};
  }

  [[nodiscard]] static auto internal_error() -> HttpResponse {
    return {
        .status = HttpStatus::InternalServerError, .headers = {}, .body = {}};
  }

  auto set_header(std::string key, std::string value) -> HttpResponse & {
    headers.set(key, value);
    return *this;
  }

  auto set_body(std::string body_str) -> HttpResponse & {
    body.assign(body_str.begin(), body_str.end());
    return *this;
  }

  auto set_body(std::vector<std::uint8_t> body_data) -> HttpResponse & {
    body = std::move(body_data);
    return *this;
  }

  [[nodiscard]] auto body_as_string() const -> std::string_view {
    return {reinterpret_cast<const char *>(body.data()), body.size()};
  }

  [[nodiscard]] auto serialize() const -> std::vector<std::uint8_t> {
    std::vector<std::uint8_t> result;

    result.reserve(256 + body.size());
    std::format_to(std::back_inserter(result), "HTTP/1.1 {} {}\r\n",
                   static_cast<std::uint16_t>(status),
                   status_reason_phrase(status));

    const bool has_content_length = headers.contains("Content-Length");
    for (const auto &field : headers) {
      std::format_to(std::back_inserter(result), "{}: {}\r\n", field.name,
                     field.value);
    }

    if (!has_content_length && !body.empty()) {
      std::format_to(std::back_inserter(result), "Content-Length: {}\r\n",
                     body.size());
    }

    constexpr std::string_view kSecurityHeaders =
        "X-Frame-Options: DENY\r\n"
        "X-Content-Type-Options: nosniff\r\n"
        "Cache-Control: no-store\r\n";
    result.insert(result.end(), kSecurityHeaders.begin(),
                  kSecurityHeaders.end());

    result.emplace_back('\r');
    result.emplace_back('\n');
    result.insert(result.end(), body.begin(), body.end());
    return result;
  }
};

} // namespace dagforge::http

template <>
struct std::formatter<dagforge::http::HttpMethod>
    : std::formatter<std::string_view> {
  auto format(dagforge::http::HttpMethod method, auto &ctx) const {
    using enum dagforge::http::HttpMethod;
    std::string_view name = dagforge::http::http_method_name(method);
    return std::formatter<std::string_view>::format(name, ctx);
  }
};

template <>
struct std::formatter<dagforge::http::HttpStatus>
    : std::formatter<std::uint16_t> {
  auto format(dagforge::http::HttpStatus status, auto &ctx) const {
    return std::formatter<std::uint16_t>::format(
        static_cast<std::uint16_t>(status), ctx);
  }
};
