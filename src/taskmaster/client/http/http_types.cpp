#include "taskmaster/client/http/http_types.hpp"

#include <algorithm>
#include <format>

namespace taskmaster::http {

auto HttpRequest::header(std::string_view key) const
    -> std::optional<std::string> {
  auto it = headers.find(std::string(key));
  if (it != headers.end()) {
    return it->second;
  }
  return std::nullopt;
}

auto HttpRequest::is_websocket_upgrade() const -> bool {
  auto upgrade = header("Upgrade");
  auto connection = header("Connection");

  if (!upgrade || !connection) {
    return false;
  }

  auto to_lower = [](std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s;
  };

  return to_lower(*upgrade) == "websocket" &&
         to_lower(*connection).find("upgrade") != std::string::npos;
}

auto HttpRequest::body_as_string() const -> std::string_view {
  return std::string_view(reinterpret_cast<const char*>(body.data()),
                          body.size());
}

auto HttpRequest::path_param(std::string_view key) const
    -> std::optional<std::string> {
  auto it = path_params.find(std::string(key));
  if (it != path_params.end()) {
    return it->second;
  }
  return std::nullopt;
}

auto HttpRequest::serialize() const -> std::vector<uint8_t> {
  std::vector<uint8_t> result;
  
  // Pre-allocate: request line (~40) + headers (~40 each) + body
  result.reserve(256 + body.size());

  // Write request line directly to vector (zero-copy)
  std::format_to(std::back_inserter(result), "{} {} HTTP/1.1\r\n", method, path);

  // Write headers directly (zero-copy)
  bool has_content_length = false;
  bool has_host = false;

  for (const auto& [key, value] : headers) {
    std::format_to(std::back_inserter(result), "{}: {}\r\n", key, value);
    if (key == "Content-Length") {
      has_content_length = true;
    }
    if (key == "Host") {
      has_host = true;
    }
  }

  // Add missing Host header
  if (!has_host) {
    constexpr std::string_view host_header = "Host: localhost\r\n";
    result.insert(result.end(), host_header.begin(), host_header.end());
  }

  // Add Content-Length if missing
  if (!has_content_length && !body.empty()) {
    std::format_to(std::back_inserter(result), "Content-Length: {}\r\n", 
                   body.size());
  }

  // End of headers
  result.push_back('\r');
  result.push_back('\n');

  // Append body (single copy, unavoidable)
  result.insert(result.end(), body.begin(), body.end());

  return result;
}

QueryParams::QueryParams(std::string_view query_string) {
  if (query_string.empty())
    return;

  std::size_t pos = 0;
  while (pos < query_string.size()) {
    auto eq_pos = query_string.find('=', pos);
    auto amp_pos = query_string.find('&', pos);

    if (eq_pos == std::string_view::npos)
      break;

    auto key = query_string.substr(pos, eq_pos - pos);
    std::size_t value_end =
        (amp_pos == std::string_view::npos) ? query_string.size() : amp_pos;
    auto value = query_string.substr(eq_pos + 1, value_end - eq_pos - 1);

    params_[std::string(key)] = std::string(value);

    if (amp_pos == std::string_view::npos)
      break;
    pos = amp_pos + 1;
  }
}

auto QueryParams::get(std::string_view key) const -> std::optional<std::string> {
  auto it = params_.find(std::string(key));
  if (it != params_.end())
    return it->second;
  return std::nullopt;
}

auto QueryParams::has(std::string_view key) const -> bool {
  return params_.contains(std::string(key));
}

auto HttpResponse::ok() -> HttpResponse {
  return {HttpStatus::Ok, {}, {}};
}

auto HttpResponse::json(std::string_view json_str) -> HttpResponse {
  HttpResponse resp{HttpStatus::Ok, {}, {}};
  resp.headers["Content-Type"] = "application/json";
  resp.body.assign(json_str.begin(), json_str.end());
  return resp;
}

auto HttpResponse::not_found() -> HttpResponse {
  return {HttpStatus::NotFound, {}, {}};
}

auto HttpResponse::bad_request() -> HttpResponse {
  return {HttpStatus::BadRequest, {}, {}};
}

auto HttpResponse::internal_error() -> HttpResponse {
  return {HttpStatus::InternalServerError, {}, {}};
}

auto HttpResponse::set_header(std::string key, std::string value)
    -> HttpResponse& {
  headers[std::move(key)] = std::move(value);
  return *this;
}

auto HttpResponse::set_body(std::string body_str) -> HttpResponse& {
  body.assign(body_str.begin(), body_str.end());
  return *this;
}

auto HttpResponse::set_body(std::vector<uint8_t> body_data) -> HttpResponse& {
  body = std::move(body_data);
  return *this;
}

auto HttpResponse::serialize() const -> std::vector<uint8_t> {
  std::vector<uint8_t> result;
  
  // Pre-allocate: status line (~30) + headers (~40 each) + body
  result.reserve(256 + body.size());

  // Write status line directly to vector (zero-copy)
  std::format_to(std::back_inserter(result), "HTTP/1.1 {} {}\r\n", 
                 status, status_reason_phrase(status));

  // Write headers directly (zero-copy)
  bool has_content_length = false;
  for (const auto& [key, value] : headers) {
    std::format_to(std::back_inserter(result), "{}: {}\r\n", key, value);
    if (key == "Content-Length") {
      has_content_length = true;
    }
  }

  // Add Content-Length if missing
  if (!has_content_length && !body.empty()) {
    std::format_to(std::back_inserter(result), "Content-Length: {}\r\n", 
                   body.size());
  }

  // End of headers
  result.push_back('\r');
  result.push_back('\n');

  // Append body (single copy, unavoidable)
  result.insert(result.end(), body.begin(), body.end());

  return result;
}

auto status_reason_phrase(HttpStatus status) -> std::string_view {
  switch (status) {
    case HttpStatus::Ok: return "OK";
    case HttpStatus::Created: return "Created";
    case HttpStatus::Accepted: return "Accepted";
    case HttpStatus::NoContent: return "No Content";
    case HttpStatus::MovedPermanently: return "Moved Permanently";
    case HttpStatus::Found: return "Found";
    case HttpStatus::NotModified: return "Not Modified";
    case HttpStatus::BadRequest: return "Bad Request";
    case HttpStatus::Unauthorized: return "Unauthorized";
    case HttpStatus::Forbidden: return "Forbidden";
    case HttpStatus::NotFound: return "Not Found";
    case HttpStatus::MethodNotAllowed: return "Method Not Allowed";
    case HttpStatus::Conflict: return "Conflict";
    case HttpStatus::InternalServerError: return "Internal Server Error";
    case HttpStatus::NotImplemented: return "Not Implemented";
    case HttpStatus::BadGateway: return "Bad Gateway";
    case HttpStatus::ServiceUnavailable: return "Service Unavailable";
  }
  return "Unknown";
}

}  // namespace taskmaster::http
