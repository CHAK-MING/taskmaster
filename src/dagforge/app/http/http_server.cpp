#include "dagforge/app/http/http_server.hpp"
#include "dagforge/app/http/router.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/util/log.hpp"


#include <boost/asio/buffer.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/url/parse.hpp>

#include <boost/asio/error.hpp>
#include <cerrno>
#include <chrono>
#include <sys/socket.h>
#include <vector>

namespace dagforge::http {

namespace {
constexpr auto kHttpIoTimeout = std::chrono::seconds(30);
constexpr std::uint32_t kParserHeaderLimit = 256 * 1024;
constexpr std::uint64_t kParserBodyLimit = 10ULL * 1024ULL * 1024ULL;

namespace beast = boost::beast;
namespace beast_http = beast::http;

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

  std::string target(msg.target());
  if (auto parsed = boost::urls::parse_origin_form(target); parsed) {
    out.path = std::string(parsed->encoded_path());
    if (auto query = parsed->encoded_query(); !query.empty()) {
      out.query_string.assign(query.data(), query.size());
    }
  } else {
    out.path = std::move(target);
  }

  out.headers.reserve(static_cast<std::size_t>(
      std::distance(msg.base().begin(), msg.base().end())));
  for (const auto &field : msg.base()) {
    out.headers.emplace(field.name_string(), field.value());
  }
  out.body = msg.body();
  return out;
}

auto to_beast_response(const HttpResponse &resp, unsigned version,
                       bool keep_alive)
    -> beast_http::response<beast_http::vector_body<uint8_t>> {
  beast_http::response<beast_http::vector_body<uint8_t>> out{
      static_cast<beast_http::status>(resp.status), version};
  out.keep_alive(keep_alive);
  for (const auto &[k, v] : resp.headers) {
    out.set(k, v);
  }
  out.body() = resp.body;
  out.prepare_payload();
  return out;
}
} // namespace

struct HttpServer::Impl {
  Runtime &runtime;
  Router router_;
  WebSocketHandler ws_handler_;
  std::shared_ptr<boost::asio::ssl::context> tls_ctx;
  struct ShardState {
    std::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor;
  };
  std::vector<ShardState> shard_states;
  std::atomic<bool> running{false};

  explicit Impl(Runtime &rt) : runtime(rt), shard_states(rt.shard_count()) {}

  static auto handle_connection(std::shared_ptr<Impl> self,
                                boost::asio::ip::tcp::socket socket,
                                beast::flat_buffer read_buffer = {})
      -> spawn_task {
    const int fd_num = socket.native_handle();

    log::debug("HTTP connection start: fd={}", fd_num);

    try {
      while (self->running.load(std::memory_order_acquire)) {
        beast_http::request_parser<beast_http::vector_body<uint8_t>> parser;
        parser.header_limit(kParserHeaderLimit);
        parser.body_limit(kParserBodyLimit);

        auto read_res = co_await co_as_result(beast_http::async_read(
            socket, read_buffer, parser,
            boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
        if (!read_res) {
          const auto &read_ec = read_res.error();
          if (read_ec != std::error_code(boost::asio::error::make_error_code(
                             boost::asio::error::eof)) &&
              read_ec !=
                  std::error_code(beast::error_code(beast::error::timeout)) &&
              read_ec != std::error_code(
                             beast::error_code(
                                 beast_http::error::end_of_stream))) {
            log::warn("HTTP read failed: fd={} err={}", fd_num,
                      read_ec.message());
          }
          break;
        }
        (void)*read_res;

        auto beast_req = parser.release();
        auto req = to_request(beast_req);
        log::debug("HTTP request: {} {} (fd={})", req.method, req.path, fd_num);

        if (req.is_websocket_upgrade()) {
          log::debug("WebSocket upgrade request: {}", req.path);
          if (self->ws_handler_) {
            boost::asio::generic::stream_protocol::socket ws_socket(
                current_io_context());
            boost::system::error_code assign_ec;
            const int raw_fd = socket.release(assign_ec);
            if (assign_ec || raw_fd < 0) {
              log::warn("WebSocket socket release failed: {}",
                        assign_ec.message());
              co_return;
            }
            int family = AF_INET;
            boost::system::error_code ep_ec;
            auto local_ep = socket.local_endpoint(ep_ec);
            if (!ep_ec) {
              family = local_ep.protocol().family();
            }
            ws_socket.assign(
                boost::asio::generic::stream_protocol(family, SOCK_STREAM),
                raw_fd, assign_ec);
            if (assign_ec) {
              log::warn("WebSocket socket assign failed: {}",
                        assign_ec.message());
              co_return;
            }
            auto conn = std::make_shared<WebSocketConnection>(
                std::move(ws_socket), std::move(req));
            co_await self->ws_handler_(std::move(conn));
          } else {
            log::warn("WebSocket upgrade but no handler set");
          }
          co_return;
        }

        auto resp = co_await self->router_.route(req);
        auto beast_resp = to_beast_response(resp, beast_req.version(),
                                            beast_req.keep_alive());

        auto write_res = co_await co_as_result(beast_http::async_write(
            socket, beast_resp,
            boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
        if (!write_res) {
          log::warn("HTTP write failed: fd={} err={}", fd_num,
                    write_res.error().message());
          break;
        }
        (void)*write_res;

        if (!beast_req.keep_alive()) {
          break;
        }
      }
    } catch (const std::exception &e) {
      log::error("Exception in connection handler: {}", e.what());
    }

    log::debug("HTTP connection close: fd={}", fd_num);
  }

  static auto handle_tls_connection(
      std::shared_ptr<Impl> self, boost::asio::ip::tcp::socket socket,
      boost::beast::flat_buffer detect_buffer) -> spawn_task {
    namespace beast = boost::beast;
    namespace net = boost::asio;
    if (!self->tls_ctx) {
      boost::system::error_code ec;
      socket.close(ec);
      co_return;
    }

    net::ssl::stream<boost::asio::ip::tcp::socket> stream(
        std::move(socket), *self->tls_ctx);

    if (const auto buffered = boost::asio::buffer_size(detect_buffer.data());
        buffered > 0) {
      std::vector<uint8_t> preface(buffered);
      boost::asio::buffer_copy(boost::asio::buffer(preface),
                               detect_buffer.data());
      auto handshake_res = co_await co_as_result(stream.async_handshake(
          net::ssl::stream_base::server, boost::asio::buffer(preface),
          boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
      if (!handshake_res) {
        log::warn("TLS handshake failed: {}", handshake_res.error().message());
        co_return;
      }
      (void)*handshake_res;
    } else {
      auto handshake_res = co_await co_as_result(stream.async_handshake(
          net::ssl::stream_base::server,
          boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
      if (!handshake_res) {
        log::warn("TLS handshake failed: {}", handshake_res.error().message());
        co_return;
      }
    }

    beast::flat_buffer read_buffer;
    if (const auto buffered = boost::asio::buffer_size(detect_buffer.data());
        buffered > 0) {
      read_buffer.commit(boost::asio::buffer_copy(read_buffer.prepare(buffered),
                                                  detect_buffer.data()));
      detect_buffer.consume(buffered);
    }

    while (self->running.load(std::memory_order_acquire)) {
      beast_http::request_parser<beast_http::vector_body<uint8_t>> parser;
      parser.header_limit(kParserHeaderLimit);
      parser.body_limit(kParserBodyLimit);
      auto read_res = co_await co_as_result(beast_http::async_read(
          stream, read_buffer, parser,
          boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
      if (!read_res) {
        const auto &read_ec = read_res.error();
        if (read_ec != std::error_code(
                           boost::asio::error::make_error_code(net::error::eof)) &&
            read_ec !=
                std::error_code(beast::error_code(beast::error::timeout)) &&
            read_ec != std::error_code(boost::system::error_code(
                           net::ssl::error::stream_truncated,
                           net::error::get_ssl_category())) &&
            read_ec != std::error_code(
                           beast::error_code(
                               beast_http::error::end_of_stream))) {
          log::debug("HTTPS read failed: {}", read_ec.message());
        }
        break;
      }
      (void)*read_res;
      auto beast_req = parser.release();
      auto req = to_request(beast_req);

      if (req.is_websocket_upgrade()) {
        if (self->ws_handler_) {
          auto conn = std::make_shared<TlsWebSocketConnection>(
              std::move(stream), std::move(req));
          co_await self->ws_handler_(std::move(conn));
        } else {
          log::warn("WebSocket upgrade over TLS but no handler set");
        }
        co_return;
      }

      auto resp = co_await self->router_.route(req);
      auto beast_resp =
          to_beast_response(resp, beast_req.version(), beast_req.keep_alive());
      auto write_res = co_await co_as_result(beast_http::async_write(
          stream, beast_resp,
          boost::asio::cancel_after(kHttpIoTimeout, dagforge::use_nothrow)));
      if (!write_res) {
        log::warn("HTTPS write failed: {}", write_res.error().message());
        break;
      }
      (void)*write_res;

      if (!beast_req.keep_alive()) {
        break;
      }
    }

    boost::system::error_code shutdown_ec;
    stream.shutdown(shutdown_ec);
  }

  static auto accept_loop(
      std::shared_ptr<Impl> self,
      std::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor,
      unsigned shard_index) -> spawn_task {
    auto &io_ctx = current_io_context();
    while (self->running.load(std::memory_order_acquire)) {
      boost::asio::ip::tcp::socket socket(io_ctx);
      auto accept_res =
          co_await co_as_result(acceptor->async_accept(socket, dagforge::use_nothrow));
      if (!accept_res) {
        const auto &accept_ec = accept_res.error();
        if (self->running.load(std::memory_order_acquire) &&
            accept_ec != std::error_code(
                             boost::asio::error::make_error_code(
                                 boost::asio::error::operation_aborted))) {
          log::error("Accept failed: {}", accept_ec.message());
        }
        break;
      }

      boost::system::error_code nodelay_ec;
      socket.set_option(boost::asio::ip::tcp::no_delay(true), nodelay_ec);
      if (nodelay_ec) {
        log::warn("Failed to set TCP_NODELAY: {}", nodelay_ec.message());
      }

      boost::beast::flat_buffer detect_buffer;
      auto detect_res = co_await co_as_result(
          boost::beast::async_detect_ssl(socket, detect_buffer,
                                         dagforge::use_nothrow));
      if (!detect_res) {
        log::warn("SSL detection failed on fd={} err={}",
                  socket.native_handle(), detect_res.error().message());
        boost::system::error_code close_ec;
        socket.close(close_ec);
        continue;
      }
      const auto is_ssl = *detect_res;

      if (is_ssl) {
        if (!self->tls_ctx) {
          log::warn(
              "TLS client detected but TLS context is not configured; closing");
          boost::system::error_code close_ec;
          socket.close(close_ec);
          continue;
        }
        self->runtime.spawn_external(handle_tls_connection(
            self, std::move(socket), std::move(detect_buffer)));
        continue;
      }

      log::debug("Accepted connection: fd={}", socket.native_handle());
      self->runtime.spawn_external(
          handle_connection(self, std::move(socket), std::move(detect_buffer)));
    }

    if (self->shard_states[shard_index].acceptor == acceptor) {
      self->shard_states[shard_index].acceptor.reset();
    }
  }
};

HttpServer::HttpServer(Runtime &runtime)
    : impl_(std::make_shared<Impl>(runtime)) {}

HttpServer::~HttpServer() { stop(); }

auto HttpServer::router() -> Router & { return impl_->router_; }

auto HttpServer::set_websocket_handler(WebSocketHandler handler) -> void {
  impl_->ws_handler_ = std::move(handler);
}

auto HttpServer::set_tls_credentials(std::string cert_chain_file,
                                     std::string private_key_file)
    -> Result<void> {
  namespace net = boost::asio;
  auto ctx = std::make_shared<net::ssl::context>(net::ssl::context::tls_server);
  boost::system::error_code ec;
  ctx->set_options(
      net::ssl::context::default_workarounds | net::ssl::context::no_sslv2 |
          net::ssl::context::no_sslv3 | net::ssl::context::single_dh_use,
      ec);
  if (ec) {
    log::error("TLS context option setup failed: {}", ec.message());
    return fail(Error::InvalidArgument);
  }

  ctx->use_certificate_chain_file(cert_chain_file, ec);
  if (ec) {
    log::error("Failed to load TLS certificate '{}': {}", cert_chain_file,
               ec.message());
    return fail(Error::InvalidArgument);
  }

  ctx->use_private_key_file(private_key_file, net::ssl::context::pem, ec);
  if (ec) {
    log::error("Failed to load TLS private key '{}': {}", private_key_file,
               ec.message());
    return fail(Error::InvalidArgument);
  }

  impl_->tls_ctx = std::move(ctx);
  log::debug("TLS enabled for HTTP server (cert='{}', key='{}')",
             cert_chain_file, private_key_file);
  return ok();
}

auto HttpServer::start(std::string_view host, uint16_t port) -> Result<void> {
  return start(host, port, false);
}

auto HttpServer::start(std::string_view host, uint16_t port, bool reuse_port)
    -> Result<void> {
  auto impl = impl_;

  auto cleanup = [&](std::error_code ec) -> Result<void> {
    impl->running = false;
    for (auto &state : impl->shard_states) {
      if (auto acc = std::exchange(state.acceptor, nullptr)) {
        boost::system::error_code close_ec;
        acc->cancel(close_ec);
        acc->close(close_ec);
      }
    }
    return fail(ec);
  };

  boost::system::error_code addr_ec;
  boost::asio::ip::address bind_address;
  if (host == "0.0.0.0" || host.empty()) {
    bind_address = boost::asio::ip::address_v4::any();
  } else {
    bind_address = boost::asio::ip::make_address(std::string(host), addr_ec);
  }
  if (addr_ec) {
    log::error("Invalid host address '{}': {}", host, addr_ec.message());
    return cleanup(make_error_code(Error::InvalidArgument));
  }

  for (auto &state : impl->shard_states) {
    state.acceptor.reset();
  }
  const auto acceptor_count =
      reuse_port ? std::max(1U, impl->runtime.shard_count()) : 1U;

  for (unsigned i = 0; i < acceptor_count; ++i) {
    unsigned shard_idx =
        reuse_port ? (i % std::max(1U, impl->runtime.shard_count())) : 0;

    auto acceptor = std::make_shared<boost::asio::ip::tcp::acceptor>(
        impl->runtime.shard(shard_idx).ctx());
    boost::system::error_code ec;

    acceptor->open(bind_address.is_v6() ? boost::asio::ip::tcp::v6()
                                        : boost::asio::ip::tcp::v4(),
                   ec);
    if (ec) {
      log::error("Failed to open acceptor: {}", ec.message());
      return cleanup(ec);
    }

    acceptor->set_option(boost::asio::socket_base::reuse_address(true), ec);
    if (ec) {
      log::warn("Failed to set SO_REUSEADDR: {}", ec.message());
      ec.clear();
    }

#ifdef SO_REUSEPORT
    if (reuse_port) {
      int reuse = 1;
      if (::setsockopt(acceptor->native_handle(), SOL_SOCKET, SO_REUSEPORT,
                       &reuse, sizeof(reuse)) < 0) {
        const auto opt_ec = std::error_code(errno, std::system_category());
        log::error("Failed to set SO_REUSEPORT: {}", opt_ec.message());
        return cleanup(opt_ec);
      }
    }
#else
    if (reuse_port) {
      log::error("SO_REUSEPORT is not supported on this platform");
      return cleanup(make_error_code(Error::InvalidArgument));
    }
#endif

    acceptor->bind({bind_address, port}, ec);
    if (ec) {
      log::error("Failed to bind {}:{}: {}", bind_address.to_string(), port,
                 ec.message());
      return cleanup(ec);
    }

    acceptor->listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
      log::error("Failed to listen on {}:{}: {}", bind_address.to_string(),
                 port, ec.message());
      return cleanup(ec);
    }

    impl->shard_states[shard_idx].acceptor = acceptor;
  }

  impl->running = true;

  log::debug("HTTP server listening on {}:{} (acceptors={}, reuse_port={})",
             host, port, acceptor_count, reuse_port);

  for (unsigned i = 0; i < acceptor_count; ++i) {
    const auto shard_idx =
        reuse_port ? (i % std::max(1U, impl->runtime.shard_count())) : 0;
    auto acceptor = impl->shard_states[shard_idx].acceptor;
    impl->runtime.spawn_on(
        shard_idx,
        Impl::accept_loop(impl, std::move(acceptor), shard_idx));
  }

  return ok();
}

auto HttpServer::stop() -> void {
  if (!impl_->running.exchange(false)) {
    return;
  }

  log::debug("Stopping HTTP server...");

  for (unsigned i = 0; i < impl_->runtime.shard_count(); ++i) {
    boost::asio::post(impl_->runtime.executor_for(i), [impl = impl_, i]() {
      if (auto acc = impl->shard_states[i].acceptor) {
        boost::system::error_code close_ec;
        acc->cancel(close_ec);
        acc->close(close_ec);
      }
    });
  }

  log::debug("HTTP server stopped");
}

auto HttpServer::is_running() const -> bool { return impl_->running.load(); }

} // namespace dagforge::http
