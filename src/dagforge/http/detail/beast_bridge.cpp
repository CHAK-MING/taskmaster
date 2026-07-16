#include "beast_bridge.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <format>
#include <string>
#include <utility>

namespace dagforge::http::detail {
namespace {

[[nodiscard]] auto safe_http_token(std::string_view value) noexcept -> bool {
  return !value.empty() && !value.contains('\r') && !value.contains('\n') &&
         !value.contains('\0');
}

} // namespace

auto from_beast_method(beast_http::verb method) noexcept -> Result<HttpMethod> {
  switch (method) {
  case beast_http::verb::get:
    return ok(HttpMethod::GET);
  case beast_http::verb::post:
    return ok(HttpMethod::POST);
  case beast_http::verb::put:
    return ok(HttpMethod::PUT);
  case beast_http::verb::delete_:
    return ok(HttpMethod::DELETE);
  case beast_http::verb::patch:
    return ok(HttpMethod::PATCH);
  case beast_http::verb::options:
    return ok(HttpMethod::OPTIONS);
  case beast_http::verb::head:
    return ok(HttpMethod::HEAD);
  default:
    return fail(Error::Unsupported);
  }
}

auto to_beast_method(HttpMethod method) noexcept -> beast_http::verb {
  switch (method) {
  case HttpMethod::GET:
    return beast_http::verb::get;
  case HttpMethod::POST:
    return beast_http::verb::post;
  case HttpMethod::PUT:
    return beast_http::verb::put;
  case HttpMethod::DELETE:
    return beast_http::verb::delete_;
  case HttpMethod::PATCH:
    return beast_http::verb::patch;
  case HttpMethod::OPTIONS:
    return beast_http::verb::options;
  case HttpMethod::HEAD:
    return beast_http::verb::head;
  }
  return beast_http::verb::unknown;
}

auto from_beast_request(const BeastRequest &request) -> Result<HttpRequest> {
  auto method = from_beast_method(request.method());
  if (!method) {
    return fail(method.error());
  }

  HttpRequest converted;
  converted.method = *method;
  converted.version_major = request.version() / 10;
  converted.version_minor = request.version() % 10;

  std::string target(request.target());
  if (target.empty() || !safe_http_token(target) || target.contains('#')) {
    return fail(Error::InvalidUrl);
  }
  if (!target.starts_with('/')) {
    converted.path = std::move(target);
  } else if (const auto query = target.find('?'); query != std::string::npos) {
    converted.path = target.substr(0, query);
    converted.query_string = target.substr(query + 1);
  } else {
    converted.path = std::move(target);
  }

  for (const auto &field : request.base()) {
    converted.headers.add(std::string(field.name_string()),
                          std::string(field.value()));
  }
  converted.body = request.body();
  return ok(std::move(converted));
}

auto from_beast_response(const BeastResponse &response) -> HttpResponse {
  HttpResponse converted;
  converted.status = static_cast<HttpStatus>(response.result_int());
  for (const auto &field : response.base()) {
    converted.headers.add(std::string(field.name_string()),
                          std::string(field.value()));
  }
  converted.body = response.body();
  return converted;
}

auto to_beast_request(HttpRequest request, std::string_view host,
                      bool keep_alive) -> Result<BeastRequest> {
  auto target = request.query_string.empty()
                    ? request.path
                    : std::format("{}?{}", request.path, request.query_string);
  if (target.empty() || !target.starts_with('/') || !safe_http_token(target) ||
      host.empty() || !safe_http_token(host)) {
    return fail(Error::InvalidArgument);
  }

  BeastRequest message{to_beast_method(request.method), target, 11};
  if (message.method() == beast_http::verb::unknown) {
    return fail(Error::Unsupported);
  }
  try {
    for (const auto &field : request.headers) {
      if (!safe_http_token(field.name) ||
          (!field.value.empty() && !safe_http_token(field.value)) ||
          boost::algorithm::iequals(field.name, "Content-Length") ||
          boost::algorithm::iequals(field.name, "Transfer-Encoding")) {
        return fail(Error::InvalidArgument);
      }
      message.insert(field.name, field.value);
    }
  } catch (const std::exception &) {
    return fail(Error::InvalidArgument);
  }
  if (message.find(beast_http::field::host) == message.end()) {
    message.set(beast_http::field::host, host);
  }
  message.keep_alive(keep_alive);
  message.body() = std::move(request.body);
  message.prepare_payload();
  return ok(std::move(message));
}

auto to_beast_response(const HttpResponse &response, unsigned version,
                       bool keep_alive) -> BeastResponse {
  BeastResponse converted{static_cast<beast_http::status>(response.status),
                          version};
  converted.keep_alive(keep_alive);
  for (const auto &field : response.headers) {
    converted.insert(field.name, field.value);
  }
  converted.body() = response.body;
  converted.prepare_payload();
  return converted;
}

} // namespace dagforge::http::detail
