#include "taskmaster/app/http/http_server.hpp"

#include "taskmaster/app/http/http_parser.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/util/log.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <format>

namespace taskmaster::http {

struct HttpServer::Impl {
  Runtime &runtime;
  Router router_;
  WebSocketHandler ws_handler_;
  int listen_fd = -1;
  std::atomic<bool> running{false};

  explicit Impl(Runtime &rt) : runtime(rt) {}

  static auto is_websocket_upgrade(const HttpRequest &req) -> bool {
    auto upgrade = req.header("upgrade");
    auto connection = req.header("connection");
    return upgrade && *upgrade == "websocket" && connection &&
           connection->find("Upgrade") != std::string::npos;
  }

  auto handle_connection(io::AsyncFd client_fd) -> task<void> {
    HttpRequestParser request_parser;

    client_fd.rebind(current_io_context());
    log::info("HTTP connection start: fd={}", client_fd.fd());

    try {
      std::vector<uint8_t> buffer(8192);
      while (true) {
        auto result = co_await client_fd.async_read(io::buffer(buffer));
        if (!result) {
          log::warn("HTTP read failed: fd={} err={}", client_fd.fd(),
                    result.error.message());
          break;
        }

        if (result.bytes_transferred == 0) {
          break;
        }

        auto req_opt = request_parser.parse(
            std::span{buffer.data(), result.bytes_transferred});
        if (!req_opt) {
          log::warn("HTTP parse failed: fd={} bytes={} (sending 400)",
                    client_fd.fd(), result.bytes_transferred);
          auto bad = HttpResponse::bad_request();
          auto bad_data = bad.serialize();
          auto bad_write_result =
              co_await client_fd.async_write(io::buffer(bad_data));
          if (!bad_write_result) {
            log::debug("HTTP 400 write failed: fd={} err={}", client_fd.fd(),
                       bad_write_result.error.message());
          }
          break;
        }

        auto &req = *req_opt;
        log::info("HTTP request: {} {} (fd={})", req.method, req.path,
                  client_fd.fd());

        if (is_websocket_upgrade(req)) {
          log::debug("WebSocket upgrade request: {}", req.path);
          if (ws_handler_) {
            auto sec_key = req.header("sec-websocket-key");
            if (sec_key) {
              co_await ws_handler_(std::move(client_fd), req.path, std::string(*sec_key));
              co_return;
            }
          } else {
            log::warn("WebSocket upgrade but no handler set");
          }
        }

        auto resp = co_await router_.route(req);
        auto resp_data = resp.serialize();

        auto write_result =
            co_await client_fd.async_write(io::buffer(resp_data));

        if (!write_result) {
          log::warn("HTTP write failed: fd={} err={}", client_fd.fd(),
                    write_result.error.message());
          break;
        }

        auto connection = req.header("Connection");
        if (connection && *connection == "close") {
          break;
        }

        request_parser.reset();
      }
    } catch (const std::exception &e) {
      log::error("Exception in connection handler: {}", e.what());
    }

    log::info("HTTP connection close: fd={}", client_fd.fd());
  }

  auto accept_loop(int listen_fd) -> task<void> {
    auto &io_ctx = current_io_context();

    while (running) {
      auto result = co_await io_ctx.async_accept(listen_fd);

      if (!result) {
        if (running) {
          log::error("Accept failed: {}", result.error.message());
        }
        break;
      }

      int raw_fd = static_cast<int>(result.bytes_transferred);
      auto client_fd = io::AsyncFd::from_raw(io_ctx, raw_fd, io::Ownership::Owned);
      log::debug("Accepted connection: fd={}", raw_fd);

      runtime.schedule(handle_connection(std::move(client_fd)).take());
    }
  }
};

HttpServer::HttpServer(Runtime &runtime)
    : impl_(std::make_unique<Impl>(runtime)) {}

HttpServer::~HttpServer() { stop(); }

auto HttpServer::router() -> Router & { return impl_->router_; }

auto HttpServer::set_websocket_handler(WebSocketHandler handler) -> void {
  impl_->ws_handler_ = std::move(handler);
}

auto HttpServer::start(std::string_view host, uint16_t port) -> task<void> {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (fd < 0) {
    log::error("Failed to create socket");
    co_return;
  }

  int reuse = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  ::inet_pton(AF_INET, host.data(), &addr.sin_addr);

  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    log::error("Failed to bind to {}:{}", host, port);
    ::close(fd);
    co_return;
  }

  if (::listen(fd, 128) < 0) {
    log::error("Failed to listen on {}:{}", host, port);
    ::close(fd);
    co_return;
  }

  impl_->listen_fd = fd;
  impl_->running = true;

  log::info("HTTP server listening on {}:{}", host, port);

  co_await impl_->accept_loop(fd);
}

auto HttpServer::stop() -> void {
  impl_->running = false;
  if (impl_->listen_fd >= 0) {
    ::close(impl_->listen_fd);
    impl_->listen_fd = -1;
  }
}

auto HttpServer::is_running() const -> bool { return impl_->running; }

} // namespace taskmaster::http
