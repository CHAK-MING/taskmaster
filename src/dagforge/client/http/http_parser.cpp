#include "dagforge/client/http/http_parser.hpp"
#include "dagforge/util/log.hpp"


#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <iterator>


namespace dagforge::http {

namespace {

namespace beast = boost::beast;
namespace beast_http = beast::http;
// Accept larger header sections to avoid benchmark/proxy scenarios tripping
// parser limits on otherwise valid requests.
constexpr std::uint32_t kParserHeaderLimit = 256 * 1024;

auto to_method(beast_http::verb verb) noexcept -> HttpMethod {
  switch (verb) {
  case beast_http::verb::get:
    return HttpMethod::GET;
  case beast_http::verb::post:
    return HttpMethod::POST;
  case beast_http::verb::put:
    return HttpMethod::PUT;
  case beast_http::verb::delete_:
    return HttpMethod::DELETE;
  case beast_http::verb::patch:
    return HttpMethod::PATCH;
  case beast_http::verb::options:
    return HttpMethod::OPTIONS;
  case beast_http::verb::head:
    return HttpMethod::HEAD;
  default:
    return HttpMethod::GET;
  }
}

auto to_request(
    const beast_http::request<beast_http::vector_body<uint8_t>> &msg)
    -> HttpRequest {
  HttpRequest out;
  out.method = to_method(msg.method());
  out.version_major = msg.version() / 10;
  out.version_minor = msg.version() % 10;
  for (const auto &field : msg.base()) {
    out.headers.add(std::string(field.name_string()), std::string(field.value()));
  }

  std::string target(msg.target());
  if (const auto query_pos = target.find('?'); query_pos != std::string::npos) {
    out.path = target.substr(0, query_pos);
    out.query_string = target.substr(query_pos + 1);
  } else {
    out.path = std::move(target);
  }

  out.body = msg.body();
  return out;
}

auto to_response(
    const beast_http::response<beast_http::vector_body<uint8_t>> &msg)
    -> HttpResponse {
  HttpResponse out;
  out.status = static_cast<HttpStatus>(msg.result_int());
  for (const auto &field : msg.base()) {
    out.headers.add(std::string(field.name_string()), std::string(field.value()));
  }
  out.body = msg.body();
  return out;
}

} // namespace

struct HttpRequestParser::Impl {
  std::unique_ptr<beast_http::request_parser<beast_http::vector_body<uint8_t>>>
      parser;
};

HttpRequestParser::HttpRequestParser() : impl_(std::make_unique<Impl>()) {
  reset();
}
HttpRequestParser::~HttpRequestParser() = default;

auto HttpRequestParser::parse(std::span<const uint8_t> data)
    -> Result<HttpRequest> {
  if (data.empty()) {
    return fail(Error::Incomplete);
  }

  boost::system::error_code ec;
  std::size_t consumed = 0;
  while (consumed < data.size()) {
    const auto n = impl_->parser->put(
        boost::asio::buffer(data.data() + consumed, data.size() - consumed),
        ec);
    consumed += n;
    if (ec || impl_->parser->is_done() || n == 0) {
      break;
    }
  }

  if (ec == beast_http::error::need_more) {
    return fail(Error::Incomplete);
  }
  if (ec) {
    log::debug("HTTP request parse error (beast): {}", ec.message());
    reset();
    return fail(Error::ProtocolError);
  }
  if (!impl_->parser->is_done()) {
    return fail(Error::Incomplete);
  }

  auto msg = impl_->parser->release();
  reset();
  return to_request(msg);
}

auto HttpRequestParser::reset() -> void {
  impl_->parser = std::make_unique<
      beast_http::request_parser<beast_http::vector_body<uint8_t>>>();
  impl_->parser->header_limit(kParserHeaderLimit);
}

struct HttpResponseParser::Impl {
  std::unique_ptr<beast_http::response_parser<beast_http::vector_body<uint8_t>>>
      parser;
};

HttpResponseParser::HttpResponseParser() : impl_(std::make_unique<Impl>()) {
  reset();
}
HttpResponseParser::~HttpResponseParser() = default;

auto HttpResponseParser::parse(std::span<const uint8_t> data)
    -> Result<HttpResponse> {
  if (data.empty()) {
    return fail(Error::Incomplete);
  }

  boost::system::error_code ec;
  std::size_t consumed = 0;
  while (consumed < data.size()) {
    const auto n = impl_->parser->put(
        boost::asio::buffer(data.data() + consumed, data.size() - consumed),
        ec);
    consumed += n;
    if (ec || impl_->parser->is_done() || n == 0) {
      break;
    }
  }

  if (ec == beast_http::error::need_more) {
    return fail(Error::Incomplete);
  }
  if (ec) {
    log::debug("HTTP response parse error (beast): {}", ec.message());
    reset();
    return fail(Error::ProtocolError);
  }
  if (!impl_->parser->is_done()) {
    return fail(Error::Incomplete);
  }

  auto msg = impl_->parser->release();
  reset();
  return to_response(msg);
}

auto HttpResponseParser::reset() -> void {
  impl_->parser = std::make_unique<
      beast_http::response_parser<beast_http::vector_body<uint8_t>>>();
  impl_->parser->header_limit(kParserHeaderLimit);
}

} // namespace dagforge::http
