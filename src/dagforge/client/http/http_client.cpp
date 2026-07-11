#include "dagforge/client/http/http_client.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"


#include <boost/asio/cancel_after.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <string>

namespace dagforge::http {

namespace {

namespace beast = boost::beast;
namespace beast_http = beast::http;

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
  return "GET";
}

auto to_response(
    const beast_http::response<beast_http::vector_body<uint8_t>> &msg)
    -> HttpResponse {
  HttpResponse out;
  out.status = static_cast<HttpStatus>(msg.result_int());
  out.headers.reserve(static_cast<std::size_t>(
      std::distance(msg.base().begin(), msg.base().end())));
  for (const auto &field : msg.base()) {
    out.headers.emplace(field.name_string(), field.value());
  }
  out.body = msg.body();
  return out;
}

template <typename Stream>
auto close_stream(Stream &stream) -> void {
  boost::system::error_code ec;
  stream.close(ec);
}

template <typename Stream>
auto request_over_stream(Stream &stream, HttpRequest req,
                         HttpClientConfig config, const std::string &host)
    -> task<Result<HttpResponse>> {
  const auto method = req.method;
  const auto target = req.query_string.empty()
                          ? req.path
                          : std::format("{}?{}", req.path, req.query_string);
  if (!req.headers.contains("Host")) {
    req.headers["Host"] = host;
  }
  if (config.keep_alive && !req.headers.contains("Connection")) {
    req.headers["Connection"] = "keep-alive";
  }

  auto request_data = req.serialize();
  auto write_res = co_await co_as_result(boost::asio::async_write(
      stream, boost::asio::buffer(request_data),
      boost::asio::cancel_after(config.read_timeout, use_nothrow)));
  if (!write_res) {
    close_stream(stream);
    log::error("HTTP request write failed host={} method={} target={}: {}",
               host, method_to_string(method), target,
               write_res.error().message());
    co_return fail(write_res.error());
  }
  (void)*write_res;

  beast::flat_buffer read_buffer;
  beast_http::response_parser<beast_http::vector_body<uint8_t>> parser;
  parser.header_limit(256 * 1024);
  parser.body_limit(config.max_response_size);

  auto read_res = co_await co_as_result(beast_http::async_read(
      stream, read_buffer, parser,
      boost::asio::cancel_after(config.read_timeout, use_nothrow)));
  if (!read_res) {
    close_stream(stream);
    log::error("HTTP response read failed host={} method={} target={}: {}",
               host, method_to_string(method), target,
               read_res.error().message());
    co_return fail(read_res.error());
  }
  (void)*read_res;

  co_return ok(to_response(parser.release()));
}

} // namespace

struct HttpClient::Impl {
  SocketVariant socket;
  HttpClientConfig config;
  std::string host;

  Impl(SocketVariant socket_in, HttpClientConfig cfg)
      : socket(std::move(socket_in)), config(cfg) {}
};

HttpClient::HttpClient(SocketVariant socket, HttpClientConfig config)
    : impl_(std::make_unique<Impl>(std::move(socket), config)) {}

HttpClient::~HttpClient() = default;

HttpClient::HttpClient(HttpClient &&) noexcept = default;
auto HttpClient::operator=(HttpClient &&) noexcept -> HttpClient & = default;

auto HttpClient::connect_tcp(io::IoContext &ctx, std::string_view host,
                             uint16_t port, HttpClientConfig config)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  boost::system::error_code ec;
  boost::asio::ip::tcp::resolver resolver(ctx);
  std::string host_str(host);
  std::string port_str = std::to_string(port);
  auto endpoints = resolver.resolve(host_str, port_str, ec);
  if (ec) {
    log::debug("Failed to resolve {}:{} - {}", host, port, ec.message());
    co_return fail(Error::InvalidUrl);
  }

  boost::asio::ip::tcp::socket socket(ctx);
  auto connect_res = co_await co_as_result(boost::asio::async_connect(
      socket, endpoints,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!connect_res) {
    log::debug("Failed to connect to {}:{} - {}", host, port,
               connect_res.error().message());
    co_return fail(connect_res.error());
  }
  (void)*connect_res;

  auto client = std::make_unique<HttpClient>(std::move(socket), config);
  client->impl_->host = std::string(host);
  co_return ok(std::move(client));
}

auto HttpClient::connect_unix(io::IoContext &ctx, std::string_view socket_path,
                              HttpClientConfig config)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  boost::system::error_code ec;
  boost::asio::local::stream_protocol::socket socket(ctx);
  socket.open(boost::asio::local::stream_protocol(), ec);
  if (ec) {
    log::debug("Failed to open unix socket {} - {}", socket_path, ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  socket.non_blocking(true, ec);
  if (ec) {
    log::debug("Failed to set non-blocking for {} - {}", socket_path,
               ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  auto connect_res = co_await co_as_result(socket.async_connect(
      boost::asio::local::stream_protocol::endpoint(std::string(socket_path)),
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!connect_res) {
    log::debug("Failed to connect to {} - {}", socket_path,
               connect_res.error().message());
    co_return fail(connect_res.error());
  }

  auto client = std::make_unique<HttpClient>(std::move(socket), config);
  client->impl_->host = "localhost";
  co_return ok(std::move(client));
}

auto HttpClient::request(HttpRequest req) -> task<Result<HttpResponse>> {
  if (!is_connected()) {
    co_return fail(Error::InvalidState);
  }

  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    auto response_res =
        co_await request_over_stream(*tcp, std::move(req), impl_->config,
                                     impl_->host);
    if (!response_res) {
      co_return fail(response_res.error());
    }
    co_return ok(std::move(*response_res));
  }
  auto *unix_socket =
      std::get_if<boost::asio::local::stream_protocol::socket>(&impl_->socket);
  auto response_res = co_await request_over_stream(*unix_socket, std::move(req),
                                                   impl_->config, impl_->host);
  if (!response_res) {
    co_return fail(response_res.error());
  }
  co_return ok(std::move(*response_res));
}

auto HttpClient::get(std::string_view path, const HttpHeaders &headers)
    -> task<Result<HttpResponse>> {
  HttpRequest req;
  req.method = HttpMethod::GET;
  req.path = std::string(path);
  req.headers = headers;
  return request(std::move(req));
}

auto HttpClient::post(std::string_view path, std::vector<uint8_t> body,
                      const HttpHeaders &headers) -> task<Result<HttpResponse>> {
  HttpRequest req;
  req.method = HttpMethod::POST;
  req.path = std::string(path);
  req.body = std::move(body);
  req.headers = headers;
  return request(std::move(req));
}

auto HttpClient::post_json(std::string_view path, std::string_view json,
                           const HttpHeaders &headers)
    -> task<Result<HttpResponse>> {
  HttpRequest req;
  req.method = HttpMethod::POST;
  req.path = std::string(path);
  req.body = std::vector<uint8_t>(json.begin(), json.end());
  req.headers = headers;
  req.headers["Content-Type"] = "application/json";
  return request(std::move(req));
}

auto HttpClient::delete_(std::string_view path, const HttpHeaders &headers)
    -> task<Result<HttpResponse>> {
  HttpRequest req;
  req.method = HttpMethod::DELETE;
  req.path = std::string(path);
  req.headers = headers;
  return request(std::move(req));
}

auto HttpClient::put(std::string_view path, std::vector<uint8_t> body,
                     const HttpHeaders &headers)
    -> task<Result<HttpResponse>> {
  HttpRequest req;
  req.method = HttpMethod::PUT;
  req.path = std::string(path);
  req.body = std::move(body);
  req.headers = headers;
  return request(std::move(req));
}

auto HttpClient::is_connected() const noexcept -> bool {
  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    return tcp->is_open();
  }
  if (auto *unix_socket =
          std::get_if<boost::asio::local::stream_protocol::socket>(
              &impl_->socket)) {
    return unix_socket->is_open();
  }
  return false;
}

auto HttpClient::close() -> void {
  boost::system::error_code ec;
  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    tcp->close(ec);
    return;
  }
  if (auto *unix_socket =
          std::get_if<boost::asio::local::stream_protocol::socket>(
              &impl_->socket)) {
    unix_socket->close(ec);
  }
}

} // namespace dagforge::http
