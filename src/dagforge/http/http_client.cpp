#include "dagforge/http/http_client.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <openssl/ssl.h>

#include <algorithm>
#include <experimental/scope>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

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

[[nodiscard]] auto safe_http_token(std::string_view value) noexcept -> bool {
  return !value.empty() && !value.contains('\r') && !value.contains('\n') &&
         !value.contains('\0');
}

[[nodiscard]] auto make_beast_request(HttpRequest request,
                                      const HttpClientConfig &config,
                                      const std::string &host)
    -> Result<beast_http::request<beast_http::vector_body<uint8_t>>> {
  auto target = request.query_string.empty()
                    ? request.path
                    : std::format("{}?{}", request.path, request.query_string);
  if (target.empty() || !target.starts_with('/') || !safe_http_token(target)) {
    return fail(Error::InvalidArgument);
  }

  beast_http::request<beast_http::vector_body<uint8_t>> message{
      to_beast_method(request.method), target, 11};
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
  message.keep_alive(config.keep_alive);
  message.body() = std::move(request.body);
  message.prepare_payload();
  return ok(std::move(message));
}

[[nodiscard]] auto permitted_endpoints(
    const boost::asio::ip::tcp::resolver::results_type &resolved,
    const HttpClientConfig &config)
    -> Result<std::vector<boost::asio::ip::tcp::endpoint>> {
  std::vector<boost::asio::ip::tcp::endpoint> endpoints;
  endpoints.reserve(resolved.size());
  for (const auto &entry : resolved) {
    const auto &endpoint = entry.endpoint();
    if (!config.endpoint_allowed || config.endpoint_allowed(endpoint.address())) {
      endpoints.push_back(endpoint);
    }
  }
  if (endpoints.empty()) {
    return fail(Error::Unauthorized);
  }
  return ok(std::move(endpoints));
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

template <typename Stream>
auto close_stream(Stream &stream) -> void {
  boost::system::error_code ec;
  if constexpr (requires { stream.next_layer(); }) {
    stream.next_layer().close(ec);
  } else {
    stream.close(ec);
  }
}

template <typename Stream>
auto request_over_stream(Stream &stream, HttpRequest req,
                         HttpClientConfig config, const std::string &host,
                         boost::asio::cancellation_slot cancellation)
    -> task<Result<HttpResponse>> {
  const auto method = req.method;
  const auto target = req.query_string.empty()
                          ? req.path
                          : std::format("{}?{}", req.path, req.query_string);
  auto request_message = make_beast_request(std::move(req), config, host);
  if (!request_message) {
    co_return fail(request_message.error());
  }
  auto write_res = co_await co_as_result(beast_http::async_write(
      stream, *request_message,
      boost::asio::bind_cancellation_slot(
          cancellation,
          boost::asio::cancel_after(config.read_timeout, use_nothrow))));
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
  parser.header_limit(config.max_response_header_size);
  parser.body_limit(config.max_response_size);
  parser.skip(method == HttpMethod::HEAD);

  auto read_res = co_await co_as_result(beast_http::async_read(
      stream, read_buffer, parser,
      boost::asio::bind_cancellation_slot(
          cancellation,
          boost::asio::cancel_after(config.read_timeout, use_nothrow))));
  if (!read_res) {
    close_stream(stream);
    log::error("HTTP response read failed host={} method={} target={}: {}",
               host, method_to_string(method), target,
               read_res.error().message());
    co_return fail(read_res.error());
  }
  (void)*read_res;

  const auto header_count = static_cast<std::size_t>(
      std::distance(parser.get().base().begin(), parser.get().base().end()));
  if (header_count > config.max_response_headers) {
    close_stream(stream);
    co_return fail(Error::ResourceExhausted);
  }

  co_return ok(to_response(parser.release()));
}

} // namespace

struct HttpClient::Impl {
  SocketVariant socket;
  HttpClientConfig config;
  std::string host;
  std::shared_ptr<boost::asio::ssl::context> tls_context;

  Impl(SocketVariant socket_in, HttpClientConfig cfg,
       std::shared_ptr<boost::asio::ssl::context> context = {})
      : socket(std::move(socket_in)), config(cfg),
        tls_context(std::move(context)) {}
};

HttpClient::HttpClient(SocketVariant socket, HttpClientConfig config)
    : impl_(std::make_unique<Impl>(std::move(socket), config)) {}

HttpClient::HttpClient(
    SocketVariant socket, HttpClientConfig config,
    std::shared_ptr<boost::asio::ssl::context> tls_context)
    : impl_(std::make_unique<Impl>(std::move(socket), config,
                                   std::move(tls_context))) {}

HttpClient::~HttpClient() = default;

HttpClient::HttpClient(HttpClient &&) noexcept = default;
auto HttpClient::operator=(HttpClient &&) noexcept -> HttpClient & = default;

auto HttpClient::connect_tcp(io::IoContext &ctx, std::string host,
                             uint16_t port, HttpClientConfig config,
                             boost::asio::cancellation_slot cancellation)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  auto resolver = std::make_shared<boost::asio::ip::tcp::resolver>(ctx);
  if (cancellation.is_connected()) {
    cancellation.assign([resolver](boost::asio::cancellation_type) {
      resolver->cancel();
    });
  }
  const auto clear_cancellation =
      std::experimental::scope_exit([cancellation]() mutable {
        if (cancellation.is_connected()) {
          cancellation.clear();
        }
      });
  std::string port_str = std::to_string(port);
  auto resolve_res = co_await co_as_result(resolver->async_resolve(
      host, port_str,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!resolve_res) {
    log::debug("Failed to resolve {}:{} - {}", host, port,
               resolve_res.error().message());
    co_return fail(resolve_res.error());
  }
  auto endpoints = permitted_endpoints(*resolve_res, config);
  if (!endpoints) {
    log::warn("Resolved endpoints for {}:{} were rejected by policy", host,
              port);
    co_return fail(endpoints.error());
  }

  auto socket = std::make_shared<boost::asio::ip::tcp::socket>(ctx);
  if (cancellation.is_connected()) {
    cancellation.assign([socket](boost::asio::cancellation_type) {
      close_stream(*socket);
    });
  }
  auto connect_res = co_await co_as_result(boost::asio::async_connect(
      *socket, *endpoints,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!connect_res) {
    log::debug("Failed to connect to {}:{} - {}", host, port,
               connect_res.error().message());
    co_return fail(connect_res.error());
  }
  (void)*connect_res;

  if (cancellation.is_connected()) {
    cancellation.clear();
  }
  auto client = std::make_unique<HttpClient>(std::move(*socket), config);
  client->impl_->host = std::move(host);
  co_return ok(std::move(client));
}

auto HttpClient::connect_tls(io::IoContext &ctx, std::string host,
                             uint16_t port, HttpClientConfig config,
                             boost::asio::cancellation_slot cancellation)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  if ((config.tls_min_version != "1.2" &&
       config.tls_min_version != "1.3") ||
      (config.tls_client_cert_file.empty() !=
       config.tls_client_key_file.empty())) {
    co_return fail(Error::InvalidArgument);
  }
  boost::system::error_code ec;
  auto resolver = std::make_shared<boost::asio::ip::tcp::resolver>(ctx);
  if (cancellation.is_connected()) {
    cancellation.assign([resolver](boost::asio::cancellation_type) {
      resolver->cancel();
    });
  }
  const auto clear_cancellation =
      std::experimental::scope_exit([cancellation]() mutable {
        if (cancellation.is_connected()) {
          cancellation.clear();
        }
      });
  auto resolve_res = co_await co_as_result(resolver->async_resolve(
      host, std::to_string(port),
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!resolve_res) {
    log::debug("Failed to resolve TLS endpoint {}:{} - {}", host, port,
               resolve_res.error().message());
    co_return fail(resolve_res.error());
  }
  auto endpoints = permitted_endpoints(*resolve_res, config);
  if (!endpoints) {
    log::warn("Resolved TLS endpoints for {}:{} were rejected by policy", host,
              port);
    co_return fail(endpoints.error());
  }

  auto tls_context = std::make_shared<boost::asio::ssl::context>(
      boost::asio::ssl::context::tls_client);
  tls_context->set_default_verify_paths(ec);
  if (ec) {
    log::debug("Failed to load default TLS trust store: {}", ec.message());
    co_return fail(ec);
  }
  if (!config.tls_ca_file.empty()) {
    tls_context->load_verify_file(config.tls_ca_file, ec);
    if (ec) {
      log::debug("Failed to load TLS CA file '{}': {}", config.tls_ca_file,
                 ec.message());
      co_return fail(Error::InvalidArgument);
    }
  }
  if (!config.tls_client_cert_file.empty()) {
    tls_context->use_certificate_chain_file(config.tls_client_cert_file, ec);
    if (ec) {
      log::debug("Failed to load TLS client certificate '{}': {}",
                 config.tls_client_cert_file, ec.message());
      co_return fail(Error::InvalidArgument);
    }
    tls_context->use_private_key_file(config.tls_client_key_file,
                                      boost::asio::ssl::context::pem, ec);
    if (ec || ::SSL_CTX_check_private_key(tls_context->native_handle()) != 1) {
      log::debug("Invalid TLS client private key '{}': {}",
                 config.tls_client_key_file, ec.message());
      co_return fail(Error::InvalidArgument);
    }
  }
  ::SSL_CTX_set_options(tls_context->native_handle(), SSL_OP_NO_COMPRESSION);
  const auto minimum_protocol =
      config.tls_min_version == "1.3" ? TLS1_3_VERSION : TLS1_2_VERSION;
  if (::SSL_CTX_set_min_proto_version(tls_context->native_handle(),
                                      minimum_protocol) != 1) {
    co_return fail(Error::InvalidArgument);
  }
  tls_context->set_verify_mode(boost::asio::ssl::verify_peer);

  auto stream = std::make_shared<TlsStream>(ctx, *tls_context);
  if (cancellation.is_connected()) {
    cancellation.assign([stream](boost::asio::cancellation_type) {
      close_stream(*stream);
    });
  }
  stream->set_verify_callback(boost::asio::ssl::host_name_verification(host));
  if (SSL_set_tlsext_host_name(stream->native_handle(), host.c_str()) != 1) {
    log::debug("Failed to configure TLS SNI for {}", host);
    co_return fail(Error::InvalidUrl);
  }

  auto connect_res = co_await co_as_result(boost::asio::async_connect(
      stream->next_layer(), *endpoints,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!connect_res) {
    log::debug("Failed to connect TLS endpoint {}:{} - {}", host, port,
               connect_res.error().message());
    co_return fail(connect_res.error());
  }

  auto handshake_res = co_await co_as_result(stream->async_handshake(
      boost::asio::ssl::stream_base::client,
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!handshake_res) {
    log::debug("TLS handshake failed for {}:{} - {}", host, port,
               handshake_res.error().message());
    close_stream(*stream);
    co_return fail(handshake_res.error());
  }

  if (cancellation.is_connected()) {
    cancellation.clear();
  }
  auto client = std::unique_ptr<HttpClient>(new HttpClient(
      SocketVariant{std::move(*stream)}, config, std::move(tls_context)));
  client->impl_->host = std::move(host);
  co_return ok(std::move(client));
}

auto HttpClient::connect_unix(io::IoContext &ctx, std::string socket_path,
                              HttpClientConfig config,
                              boost::asio::cancellation_slot cancellation)
    -> task<Result<std::unique_ptr<HttpClient>>> {
  boost::system::error_code ec;
  auto socket =
      std::make_shared<boost::asio::local::stream_protocol::socket>(ctx);
  if (cancellation.is_connected()) {
    cancellation.assign([socket](boost::asio::cancellation_type) {
      close_stream(*socket);
    });
  }
  const auto clear_cancellation =
      std::experimental::scope_exit([cancellation]() mutable {
        if (cancellation.is_connected()) {
          cancellation.clear();
        }
      });
  socket->open(boost::asio::local::stream_protocol(), ec);
  if (ec) {
    log::debug("Failed to open unix socket {} - {}", socket_path, ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  socket->non_blocking(true, ec);
  if (ec) {
    log::debug("Failed to set non-blocking for {} - {}", socket_path,
               ec.message());
    co_return fail(std::error_code(ec.value(), std::system_category()));
  }
  auto connect_res = co_await co_as_result(socket->async_connect(
      boost::asio::local::stream_protocol::endpoint(socket_path),
      boost::asio::cancel_after(config.connect_timeout, use_nothrow)));
  if (!connect_res) {
    log::debug("Failed to connect to {} - {}", socket_path,
               connect_res.error().message());
    co_return fail(connect_res.error());
  }

  if (cancellation.is_connected()) {
    cancellation.clear();
  }
  auto client = std::make_unique<HttpClient>(std::move(*socket), config);
  client->impl_->host = "localhost";
  co_return ok(std::move(client));
}

auto HttpClient::request(HttpRequest req,
                         boost::asio::cancellation_slot cancellation)
    -> task<Result<HttpResponse>> {
  if (!is_connected()) {
    co_return fail(Error::InvalidState);
  }

  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    auto response_res =
        co_await request_over_stream(*tcp, std::move(req), impl_->config,
                                     impl_->host, cancellation);
    if (!response_res) {
      co_return fail(response_res.error());
    }
    co_return ok(std::move(*response_res));
  }
  if (auto *unix_socket =
          std::get_if<boost::asio::local::stream_protocol::socket>(
              &impl_->socket)) {
    auto response_res =
        co_await request_over_stream(*unix_socket, std::move(req),
                                     impl_->config, impl_->host, cancellation);
    if (!response_res) {
      co_return fail(response_res.error());
    }
    co_return ok(std::move(*response_res));
  }
  auto *tls = std::get_if<TlsStream>(&impl_->socket);
  auto response_res = co_await request_over_stream(
      *tls, std::move(req), impl_->config, impl_->host, cancellation);
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
  req.headers.set("Content-Type", "application/json");
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
  if (auto *tls = std::get_if<TlsStream>(&impl_->socket)) {
    return tls->next_layer().is_open();
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
    return;
  }
  if (auto *tls = std::get_if<TlsStream>(&impl_->socket)) {
    tls->next_layer().close(ec);
  }
}

} // namespace dagforge::http
