#include "taskmaster/app/http/http_types.hpp"

#include <algorithm>

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

auto QueryParams::get(std::string_view key) const
    -> std::optional<std::string> {
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

auto method_to_string(HttpMethod method) -> std::string_view {
  switch (method) {
    case HttpMethod::GET:
      return "GET";
    case HttpMethod::POST:
      return "POST";
    case HttpMethod::PUT:
      return "PUT";
    case HttpMethod::DELETE:
      return "DELETE";
    case HttpMethod::PATCH:
      return "PATCH";
    case HttpMethod::OPTIONS:
      return "OPTIONS";
    case HttpMethod::HEAD:
      return "HEAD";
  }
  return "UNKNOWN";
}

auto status_to_int(HttpStatus status) -> int {
  return static_cast<int>(status);
}

}  // namespace taskmaster::http
