#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/scope_exit.hpp"
#include "dagforge/util/log.hpp"

#include "detail/beast_bridge.hpp"

#include <boost/asio/buffer.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/system/error_code.hpp>

#include <openssl/ssl.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <sys/socket.h>
#include <unordered_map>
#include <vector>

namespace dagforge::http {

namespace {
namespace beast = boost::beast;
namespace beast_http = beast::http;
} // namespace

struct HttpServer::Impl {
  Runtime &runtime;
  Router router_;
  std::shared_ptr<boost::asio::ssl::context> tls_ctx;
  HttpServerConfig config;
  struct ShardState {
    std::shared_ptr<boost::asio::ip::tcp::acceptor> acceptor;
  };
  std::vector<ShardState> shard_states;
  std::atomic<bool> running{false};
  std::atomic<std::size_t> active_connections{0};
  std::mutex connections_mutex;
  std::condition_variable connections_changed;
  std::uint64_t next_connection_id{1};
  std::unordered_map<std::uint64_t, std::function<void()>> connections;

  explicit Impl(Runtime &rt) : runtime(rt), shard_states(rt.shard_count()) {}

  [[nodiscard]] auto try_acquire_connection() noexcept -> bool {
    auto current = active_connections.load(std::memory_order_relaxed);
    while (current < config.max_connections) {
      if (active_connections.compare_exchange_weak(
              current, current + 1, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }

  auto release_connection() noexcept -> void {
    active_connections.fetch_sub(1, std::memory_order_acq_rel);
    connections_changed.notify_all();
  }

  [[nodiscard]] auto track_connection(std::function<void()> close) -> std::uint64_t {
    std::lock_guard lock(connections_mutex);
    const auto id = next_connection_id++;
    connections.emplace(id, std::move(close));
    return id;
  }

  auto untrack_connection(std::uint64_t id) -> void {
    std::lock_guard lock(connections_mutex);
    connections.erase(id);
  }

  auto close_connections() noexcept -> void {
    std::vector<std::function<void()>> close_actions;
    {
      std::lock_guard lock(connections_mutex);
      close_actions.reserve(connections.size());
      for (auto &[_, close] : connections) {
        close_actions.push_back(close);
      }
    }
    for (auto &close : close_actions) {
      close();
    }
  }

  auto wait_for_connections() noexcept -> void {
    std::unique_lock lock(connections_mutex);
    if (!connections_changed.wait_for(
            lock, std::chrono::seconds(5),
            [this] {
              return active_connections.load(std::memory_order_acquire) == 0;
            })) {
      log::error("Timed out while closing {} HTTP connections",
                 active_connections.load(std::memory_order_relaxed));
    }
  }

  template <typename Stream>
  static auto serve_http(std::shared_ptr<Impl> self, Stream &stream,
                         int fd_num) -> task<void> {
    beast::flat_buffer read_buffer;
    std::size_t request_count = 0;
    try {
      while (self->running.load(std::memory_order_acquire)) {
        beast_http::request_parser<beast_http::vector_body<uint8_t>> parser;
        parser.header_limit(static_cast<std::uint32_t>(std::min<std::uint64_t>(
            self->config.max_request_header_bytes,
            std::numeric_limits<std::uint32_t>::max())));
        parser.body_limit(self->config.max_request_body_bytes);

        auto read_res = co_await co_as_result(beast_http::async_read(
            stream, read_buffer, parser,
            boost::asio::cancel_after(self->config.connection_idle_timeout,
                                      dagforge::use_nothrow)));
        if (!read_res) {
          const auto &read_ec = read_res.error();
          if (read_ec != std::error_code(boost::asio::error::make_error_code(
                             boost::asio::error::eof)) &&
              read_ec != std::error_code(
                             beast::error_code(
                                 beast_http::error::end_of_stream))) {
            log::debug("HTTP read ended: fd={} err={}", fd_num,
                       read_ec.message());
          }
          break;
        }

        auto beast_req = parser.release();
        ++request_count;
        const bool keep_alive =
            beast_req.keep_alive() &&
            request_count < self->config.max_requests_per_connection;

        HttpResponse response;
        auto request = detail::from_beast_request(beast_req);
        if (!request) {
          if (request.error() == make_error_code(Error::Unsupported)) {
            response.status = HttpStatus::MethodNotAllowed;
            response.headers.set(
                "Allow", "GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD");
            response.set_body("Unsupported HTTP method");
          } else {
            response.status = HttpStatus::BadRequest;
            response.set_body("Invalid HTTP request target");
          }
        } else {
          log::debug("HTTP request: {} {} (fd={})", request->method,
                     request->path, fd_num);
          response = co_await self->router_.route(*request);
        }

        auto beast_resp = detail::to_beast_response(
            response, beast_req.version(), keep_alive);
        auto write_res = co_await co_as_result(beast_http::async_write(
            stream, beast_resp,
            boost::asio::cancel_after(self->config.connection_idle_timeout,
                                      dagforge::use_nothrow)));
        if (!write_res) {
          log::debug("HTTP write ended: fd={} err={}", fd_num,
                     write_res.error().message());
          break;
        }
        if (!keep_alive) {
          break;
        }
      }
    } catch (const std::exception &error) {
      log::error("Exception in HTTP connection handler: {}", error.what());
    }
  }

  static auto handle_connection(
      std::shared_ptr<Impl> self,
      std::shared_ptr<boost::asio::ip::tcp::socket> socket)
      -> spawn_task {
    const int fd_num = socket->native_handle();
    const auto executor = socket->get_executor();
    const auto connection_id = self->track_connection([socket, executor] {
      boost::asio::post(executor, [socket] {
        boost::system::error_code ignored;
        socket->cancel(ignored);
        socket->close(ignored);
      });
    });
    const auto release = dagforge::scope_exit(
        [self, connection_id] {
          self->untrack_connection(connection_id);
          self->release_connection();
        });
    log::debug("HTTP connection start: fd={}", fd_num);
    co_await serve_http(self, *socket, fd_num);
    boost::system::error_code close_error;
    socket->shutdown(boost::asio::ip::tcp::socket::shutdown_both, close_error);
    socket->close(close_error);
    log::debug("HTTP connection close: fd={}", fd_num);
  }

  static auto handle_tls_connection(std::shared_ptr<Impl> self,
                                    boost::asio::ip::tcp::socket socket)
      -> spawn_task {
    namespace net = boost::asio;
    if (!self->tls_ctx) {
      boost::system::error_code close_error;
      socket.close(close_error);
      self->release_connection();
      co_return;
    }

    auto stream = std::make_shared<
        net::ssl::stream<boost::asio::ip::tcp::socket>>(
        std::move(socket), *self->tls_ctx);
    const int fd_num = stream->next_layer().native_handle();
    const auto executor = stream->get_executor();
    const auto connection_id = self->track_connection([stream, executor] {
      boost::asio::post(executor, [stream] {
        boost::system::error_code ignored;
        stream->next_layer().cancel(ignored);
        stream->next_layer().close(ignored);
      });
    });
    const auto release = dagforge::scope_exit(
        [self, connection_id] {
          self->untrack_connection(connection_id);
          self->release_connection();
        });
    auto handshake_res = co_await co_as_result(stream->async_handshake(
        net::ssl::stream_base::server,
        boost::asio::cancel_after(self->config.connection_idle_timeout,
                                  dagforge::use_nothrow)));
    if (!handshake_res) {
      log::debug("TLS handshake failed: fd={} err={}", fd_num,
                 handshake_res.error().message());
      co_return;
    }

    co_await serve_http(self, *stream, fd_num);

    boost::system::error_code shutdown_ec;
    stream->shutdown(shutdown_ec);
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

      if (!self->try_acquire_connection()) {
        log::warn("HTTP connection capacity exhausted");
        boost::system::error_code close_error;
        socket.close(close_error);
        continue;
      }

      log::debug("Accepted connection: fd={}", socket.native_handle());
      if (self->tls_ctx) {
        self->runtime.spawn(handle_tls_connection(self, std::move(socket)));
      } else {
        self->runtime.spawn(handle_connection(
            self, std::make_shared<boost::asio::ip::tcp::socket>(
                      std::move(socket))));
      }
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

auto HttpServer::set_tls_credentials(std::string cert_chain_file,
                                     std::string private_key_file,
                                     std::string minimum_version)
    -> Result<void> {
  if (is_running() || cert_chain_file.empty() || private_key_file.empty() ||
      (minimum_version != "1.2" && minimum_version != "1.3")) {
    return fail(Error::InvalidArgument);
  }
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
  ::SSL_CTX_set_options(ctx->native_handle(), SSL_OP_NO_COMPRESSION);
  const auto minimum_protocol =
      minimum_version == "1.3" ? TLS1_3_VERSION : TLS1_2_VERSION;
  if (::SSL_CTX_set_min_proto_version(ctx->native_handle(), minimum_protocol) !=
      1) {
    log::error("Failed to configure minimum TLS version {}", minimum_version);
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
  if (::SSL_CTX_check_private_key(ctx->native_handle()) != 1) {
    log::error("TLS private key does not match certificate '{}', key='{}'",
               cert_chain_file, private_key_file);
    return fail(Error::InvalidArgument);
  }

  impl_->tls_ctx = std::move(ctx);
  log::debug(
      "TLS-only mode enabled for HTTP server (cert='{}', key='{}', min={})",
      cert_chain_file, private_key_file, minimum_version);
  return ok();
}

auto HttpServer::configure(HttpServerConfig config) -> Result<void> {
  if (is_running() || config.max_request_header_bytes == 0 ||
      config.max_request_body_bytes == 0 ||
      config.connection_idle_timeout <= std::chrono::milliseconds::zero() ||
      config.max_connections == 0 ||
      config.max_requests_per_connection == 0) {
    return fail(Error::InvalidArgument);
  }
  impl_->config = std::move(config);
  return ok();
}

auto HttpServer::set_request_body_limit(std::uint64_t bytes) -> Result<void> {
  if (bytes == 0 || is_running()) {
    return fail(Error::InvalidState);
  }
  impl_->config.max_request_body_bytes = bytes;
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
    impl_->runtime.post_to(i, [impl = impl_, i]() {
      if (auto acc = impl->shard_states[i].acceptor) {
        boost::system::error_code close_ec;
        acc->cancel(close_ec);
        acc->close(close_ec);
      }
    });
  }

  impl_->close_connections();
  impl_->wait_for_connections();

  log::debug("HTTP server stopped");
}

auto HttpServer::is_running() const -> bool { return impl_->running.load(); }

} // namespace dagforge::http
