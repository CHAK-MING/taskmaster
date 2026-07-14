#include "dagforge/http/http_client.hpp"

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/util/log.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>

#include <openssl/ssl.h>

#include <algorithm>
#include <atomic>
#include <experimental/scope>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

namespace dagforge::http {

namespace {

namespace beast = boost::beast;
namespace beast_http = beast::http;

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

struct StreamResponse {
  HttpResponse response;
  bool reusable{false};
};

class OperationDeadline {
public:
  template <typename Executor, typename Callback>
  OperationDeadline(Executor executor, std::chrono::milliseconds timeout,
                    Callback callback)
      : state_(std::make_shared<State>(executor, std::move(callback))) {
    state_->timer.expires_after(timeout);
    state_->timer.async_wait([state = state_](boost::system::error_code error) {
      if (!error &&
          !state->completed.exchange(true, std::memory_order_acq_rel)) {
        state->timed_out.store(true, std::memory_order_release);
        state->on_timeout();
      }
    });
  }

  ~OperationDeadline() { cancel(); }

  OperationDeadline(const OperationDeadline &) = delete;
  auto operator=(const OperationDeadline &) -> OperationDeadline & = delete;

  [[nodiscard]] auto timed_out() const noexcept -> bool {
    return state_->timed_out.load(std::memory_order_acquire);
  }

  auto cancel() noexcept -> void {
    if (!state_) {
      return;
    }
    state_->completed.store(true, std::memory_order_release);
    try {
      (void)state_->timer.cancel();
    } catch (const std::exception &) {
    }
  }

private:
  struct State {
    template <typename Executor, typename Callback>
    State(Executor executor, Callback callback)
        : timer(executor), on_timeout(std::move(callback)) {}

    boost::asio::steady_timer timer;
    std::function<void()> on_timeout;
    std::atomic_bool timed_out{false};
    std::atomic_bool completed{false};
  };

  std::shared_ptr<State> state_;
};

[[nodiscard]] auto operation_timed_out(std::error_code error) noexcept -> bool {
  return error == std::errc::timed_out;
}

[[nodiscard]] auto stage_error(std::error_code error,
                               HttpClientError failure,
                               HttpClientError timeout,
                               bool deadline_expired = false,
                               bool externally_cancelled = false)
    -> std::error_code {
  if (deadline_expired || operation_timed_out(error)) {
    return make_error_code(timeout);
  }
  if (externally_cancelled || error == std::errc::operation_canceled) {
    return std::make_error_code(std::errc::operation_canceled);
  }
  return make_error_code(failure);
}

[[nodiscard]] auto response_read_error(std::error_code error,
                                       HttpClientError failure,
                                       HttpClientError timeout,
                                       bool deadline_expired = false,
                                       bool externally_cancelled = false)
    -> std::error_code {
  if (error.value() == static_cast<int>(beast_http::error::header_limit) ||
      error.value() == static_cast<int>(beast_http::error::body_limit)) {
    return make_error_code(Error::ResourceExhausted);
  }
  if (error.category() ==
      beast_http::make_error_code(beast_http::error::bad_method).category()) {
    return make_error_code(Error::ProtocolError);
  }
  return stage_error(error, failure, timeout, deadline_expired,
                     externally_cancelled);
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
    -> task<Result<StreamResponse>> {
  auto externally_cancelled = std::make_shared<std::atomic_bool>(false);
  if (cancellation.is_connected()) {
    cancellation.assign(
        [stream_ptr = &stream,
         externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
          close_stream(*stream_ptr);
        });
  }
  const auto clear_cancellation =
      std::experimental::scope_exit([cancellation]() mutable {
        if (cancellation.is_connected()) {
          cancellation.clear();
        }
      });

  const auto method = req.method;
  const auto target = req.query_string.empty()
                          ? req.path
                          : std::format("{}?{}", req.path, req.query_string);
  auto request_message = make_beast_request(std::move(req), config, host);
  if (!request_message) {
    co_return fail(request_message.error());
  }
  OperationDeadline write_deadline(
      stream.get_executor(), config.write_timeout,
      [stream_ptr = &stream] { close_stream(*stream_ptr); });
  auto write_res = co_await co_as_result(beast_http::async_write(
      stream, *request_message, use_nothrow));
  write_deadline.cancel();
  if (!write_res) {
    close_stream(stream);
    log::error("HTTP request write failed host={} method={} target={}: {}",
               host, http_method_name(method), target,
               write_res.error().message());
    co_return fail(stage_error(write_res.error(), HttpClientError::WriteFailure,
                               HttpClientError::WriteTimeout,
                               write_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
  }
  (void)*write_res;

  beast::flat_buffer read_buffer;
  beast_http::response_parser<beast_http::vector_body<uint8_t>> parser;
  parser.header_limit(config.max_response_header_size);
  parser.body_limit(config.max_response_size);
  parser.skip(method == HttpMethod::HEAD);

  OperationDeadline first_byte_deadline(
      stream.get_executor(), config.first_byte_timeout,
      [stream_ptr = &stream] { close_stream(*stream_ptr); });
  auto header_res = co_await co_as_result(beast_http::async_read_header(
      stream, read_buffer, parser, use_nothrow));
  first_byte_deadline.cancel();
  if (!header_res) {
    close_stream(stream);
    log::error("HTTP response first byte failed host={} method={} target={}: {}",
               host, http_method_name(method), target,
               header_res.error().message());
    co_return fail(response_read_error(
        header_res.error(), HttpClientError::FirstByteFailure,
        HttpClientError::FirstByteTimeout, first_byte_deadline.timed_out(),
        externally_cancelled->load(std::memory_order_acquire)));
  }
  (void)*header_res;

  const auto header_count = static_cast<std::size_t>(
      std::distance(parser.get().base().begin(), parser.get().base().end()));
  if (header_count > config.max_response_headers) {
    close_stream(stream);
    co_return fail(Error::ResourceExhausted);
  }

  while (!parser.is_done()) {
    OperationDeadline read_deadline(
        stream.get_executor(), config.read_timeout,
        [stream_ptr = &stream] { close_stream(*stream_ptr); });
    auto read_res = co_await co_as_result(beast_http::async_read_some(
        stream, read_buffer, parser, use_nothrow));
    read_deadline.cancel();
    if (!read_res) {
      close_stream(stream);
      log::error("HTTP response read failed host={} method={} target={}: {}",
                 host, http_method_name(method), target,
                 read_res.error().message());
      co_return fail(response_read_error(
          read_res.error(), HttpClientError::ReadFailure,
          HttpClientError::ReadTimeout, read_deadline.timed_out(),
          externally_cancelled->load(std::memory_order_acquire)));
    }
  }

  auto message = parser.release();
  const bool reusable = config.keep_alive && message.keep_alive();
  if (!reusable) {
    close_stream(stream);
  }
  co_return ok(StreamResponse{.response = to_response(message),
                              .reusable = reusable});
}

} // namespace

struct HttpClient::Impl {
  SocketVariant socket;
  HttpClientConfig config;
  std::string host;
  std::shared_ptr<boost::asio::ssl::context> tls_context;
  bool reusable{false};

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
  auto externally_cancelled = std::make_shared<std::atomic_bool>(false);
  if (cancellation.is_connected()) {
    cancellation.assign(
        [resolver,
         externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
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
  OperationDeadline dns_deadline(resolver->get_executor(), config.dns_timeout,
                                 [resolver] { resolver->cancel(); });
  auto resolve_res = co_await co_as_result(
      resolver->async_resolve(host, port_str, use_nothrow));
  dns_deadline.cancel();
  if (!resolve_res) {
    log::debug("Failed to resolve {}:{} - {}", host, port,
               resolve_res.error().message());
    co_return fail(stage_error(resolve_res.error(), HttpClientError::DnsFailure,
                               HttpClientError::DnsTimeout,
                               dns_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
  }
  auto endpoints = permitted_endpoints(*resolve_res, config);
  if (!endpoints) {
    log::warn("Resolved endpoints for {}:{} were rejected by policy", host,
              port);
    co_return fail(endpoints.error());
  }

  auto socket = std::make_shared<boost::asio::ip::tcp::socket>(ctx);
  if (cancellation.is_connected()) {
    cancellation.assign(
        [socket, externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
          close_stream(*socket);
        });
  }
  OperationDeadline connect_deadline(
      socket->get_executor(), config.connect_timeout,
      [socket] { close_stream(*socket); });
  auto connect_res = co_await co_as_result(
      boost::asio::async_connect(*socket, *endpoints, use_nothrow));
  connect_deadline.cancel();
  if (!connect_res) {
    log::debug("Failed to connect to {}:{} - {}", host, port,
               connect_res.error().message());
    co_return fail(stage_error(connect_res.error(),
                               HttpClientError::ConnectFailure,
                               HttpClientError::ConnectTimeout,
                               connect_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
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
  auto externally_cancelled = std::make_shared<std::atomic_bool>(false);
  if (cancellation.is_connected()) {
    cancellation.assign(
        [resolver,
         externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
          resolver->cancel();
        });
  }
  const auto clear_cancellation =
      std::experimental::scope_exit([cancellation]() mutable {
        if (cancellation.is_connected()) {
          cancellation.clear();
        }
      });
  OperationDeadline dns_deadline(resolver->get_executor(), config.dns_timeout,
                                 [resolver] { resolver->cancel(); });
  auto resolve_res = co_await co_as_result(
      resolver->async_resolve(host, std::to_string(port), use_nothrow));
  dns_deadline.cancel();
  if (!resolve_res) {
    log::debug("Failed to resolve TLS endpoint {}:{} - {}", host, port,
               resolve_res.error().message());
    co_return fail(stage_error(resolve_res.error(), HttpClientError::DnsFailure,
                               HttpClientError::DnsTimeout,
                               dns_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
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
    cancellation.assign(
        [stream, externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
          close_stream(*stream);
        });
  }
  stream->set_verify_callback(boost::asio::ssl::host_name_verification(host));
  if (SSL_set_tlsext_host_name(stream->native_handle(), host.c_str()) != 1) {
    log::debug("Failed to configure TLS SNI for {}", host);
    co_return fail(Error::InvalidUrl);
  }

  OperationDeadline connect_deadline(
      stream->get_executor(), config.connect_timeout,
      [stream] { close_stream(*stream); });
  auto connect_res = co_await co_as_result(
      boost::asio::async_connect(stream->next_layer(), *endpoints,
                                 use_nothrow));
  connect_deadline.cancel();
  if (!connect_res) {
    log::debug("Failed to connect TLS endpoint {}:{} - {}", host, port,
               connect_res.error().message());
    co_return fail(stage_error(connect_res.error(),
                               HttpClientError::ConnectFailure,
                               HttpClientError::ConnectTimeout,
                               connect_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
  }

  OperationDeadline handshake_deadline(
      stream->get_executor(), config.tls_handshake_timeout,
      [stream] { close_stream(*stream); });
  auto handshake_res = co_await co_as_result(stream->async_handshake(
      boost::asio::ssl::stream_base::client, use_nothrow));
  handshake_deadline.cancel();
  if (!handshake_res) {
    log::debug("TLS handshake failed for {}:{} - {}", host, port,
               handshake_res.error().message());
    close_stream(*stream);
    co_return fail(stage_error(handshake_res.error(),
                               HttpClientError::TlsHandshakeFailure,
                               HttpClientError::TlsHandshakeTimeout,
                               handshake_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
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
  auto externally_cancelled = std::make_shared<std::atomic_bool>(false);
  if (cancellation.is_connected()) {
    cancellation.assign(
        [socket, externally_cancelled](boost::asio::cancellation_type) {
          externally_cancelled->store(true, std::memory_order_release);
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
  OperationDeadline connect_deadline(
      socket->get_executor(), config.connect_timeout,
      [socket] { close_stream(*socket); });
  auto connect_res = co_await co_as_result(socket->async_connect(
      boost::asio::local::stream_protocol::endpoint(socket_path),
      use_nothrow));
  connect_deadline.cancel();
  if (!connect_res) {
    log::debug("Failed to connect to {} - {}", socket_path,
               connect_res.error().message());
    co_return fail(stage_error(connect_res.error(),
                               HttpClientError::ConnectFailure,
                               HttpClientError::ConnectTimeout,
                               connect_deadline.timed_out(),
                               externally_cancelled->load(
                                   std::memory_order_acquire)));
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
  impl_->reusable = false;

  if (auto *tcp = std::get_if<boost::asio::ip::tcp::socket>(&impl_->socket)) {
    auto response_res =
        co_await request_over_stream(*tcp, std::move(req), impl_->config,
                                     impl_->host, cancellation);
    if (!response_res) {
      co_return fail(response_res.error());
    }
    impl_->reusable = response_res->reusable;
    co_return ok(std::move(response_res->response));
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
    impl_->reusable = response_res->reusable;
    co_return ok(std::move(response_res->response));
  }
  auto *tls = std::get_if<TlsStream>(&impl_->socket);
  auto response_res = co_await request_over_stream(
      *tls, std::move(req), impl_->config, impl_->host, cancellation);
  if (!response_res) {
    co_return fail(response_res.error());
  }
  impl_->reusable = response_res->reusable;
  co_return ok(std::move(response_res->response));
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

auto HttpClient::is_reusable() const noexcept -> bool {
  return impl_->reusable && is_connected();
}

auto HttpClient::close() -> void {
  impl_->reusable = false;
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
