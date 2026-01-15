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
  Runtime& runtime;
  Router router_;
  int listen_fd = -1;
  std::atomic<bool> running{false};

  explicit Impl(Runtime& rt) : runtime(rt) {
  }

  auto handle_connection(int client_fd) -> task<void> {
    auto& io_ctx = current_io_context();
    HttpRequestParser request_parser;

    try {
      std::vector<uint8_t> buffer(8192);
      while (true) {
        auto result = co_await io_ctx.async_read(client_fd, io::buffer(buffer));
        if (!result) {
          break;
        }
        
        if (result.bytes_transferred == 0) {
          break;
        }

        auto req_opt = request_parser.parse(std::span{buffer.data(), result.bytes_transferred});
        if (req_opt) {
          auto& req = *req_opt;
          log::debug("Received {} {}", req.method, req.path);

          auto resp = co_await router_.route(req);
          auto resp_data = resp.serialize();

          auto write_result =
              co_await io_ctx.async_write(client_fd, io::buffer(resp_data));

          if (!write_result) {
            break;
          }

          auto connection = req.header("Connection");
          if (connection && *connection == "close") {
            break;
          }

          request_parser.reset();
        }
      }
    } catch (const std::exception& e) {
      log::error("Exception in connection handler: {}", e.what());
    }

    ::close(client_fd);
  }

  auto accept_loop(int listen_fd) -> task<void> {
    auto& io_ctx = current_io_context();

    while (running) {
      auto result = co_await io_ctx.async_accept(listen_fd);

      if (!result) {
        if (running) {
          log::error("Accept failed: {}", result.error.message());
        }
        break;
      }

      int client_fd = static_cast<int>(result.bytes_transferred);
      log::debug("Accepted connection: fd={}", client_fd);

      runtime.schedule(handle_connection(client_fd).take());
    }
  }
};

HttpServer::HttpServer(Runtime& runtime)
    : impl_(std::make_unique<Impl>(runtime)) {
}

HttpServer::~HttpServer() {
  stop();
}

auto HttpServer::router() -> Router& {
  return impl_->router_;
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

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
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

auto HttpServer::is_running() const -> bool {
  return impl_->running;
}

}  // namespace taskmaster::http
