#include "taskmaster/app/http/http_client.hpp"
#include "taskmaster/app/http/http_parser.hpp"

#include "taskmaster/util/log.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cstring>

namespace taskmaster::http {

struct HttpClient::Impl {
  io::IoContext* ctx{nullptr};
  int fd{-1};
  HttpClientConfig config;
  std::string host;

  Impl(io::IoContext& context, int socket_fd, HttpClientConfig cfg)
      : ctx(&context), fd(socket_fd), config(std::move(cfg)) {}

  ~Impl() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

HttpClient::HttpClient(io::IoContext& ctx, int fd, HttpClientConfig config)
    : impl_(std::make_unique<Impl>(ctx, fd, std::move(config))) {}

HttpClient::~HttpClient() = default;

HttpClient::HttpClient(HttpClient&&) noexcept = default;
auto HttpClient::operator=(HttpClient&&) noexcept -> HttpClient& = default;

auto HttpClient::connect_tcp(io::IoContext& ctx, std::string_view host,
                             uint16_t port, HttpClientConfig config)
    -> task<std::unique_ptr<HttpClient>> {
  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (fd < 0) {
    log::error("Failed to create socket: {}", std::strerror(errno));
    co_return nullptr;
  }

  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo* result = nullptr;
  std::string host_str(host);
  std::string port_str = std::to_string(port);

  int ret = ::getaddrinfo(host_str.c_str(), port_str.c_str(), &hints, &result);
  if (ret != 0 || result == nullptr) {
    log::error("Failed to resolve {}:{} - {}", host, port, gai_strerror(ret));
    ::close(fd);
    co_return nullptr;
  }

  auto addr_guard = std::unique_ptr<addrinfo, decltype(&freeaddrinfo)>(
      result, freeaddrinfo);

  auto connect_result = co_await ctx.async_connect(
      fd, result->ai_addr, static_cast<std::uint32_t>(result->ai_addrlen));

  if (!connect_result && connect_result.error.value() != EINPROGRESS) {
    log::error("Failed to connect to {}:{} - {}", host, port,
               connect_result.error.message());
    ::close(fd);
    co_return nullptr;
  }

  auto client = std::make_unique<HttpClient>(ctx, fd, std::move(config));
  client->impl_->host = std::string(host);
  co_return client;
}

auto HttpClient::connect_unix(io::IoContext& ctx, std::string_view socket_path,
                              HttpClientConfig config)
    -> task<std::unique_ptr<HttpClient>> {
  int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (fd < 0) {
    log::error("Failed to create unix socket: {}", std::strerror(errno));
    co_return nullptr;
  }

  sockaddr_un addr{};
  addr.sun_family = AF_UNIX;

  if (socket_path.size() >= sizeof(addr.sun_path)) {
    log::error("Socket path too long: {}", socket_path);
    ::close(fd);
    co_return nullptr;
  }

  std::memcpy(addr.sun_path, socket_path.data(), socket_path.size());
  addr.sun_path[socket_path.size()] = '\0';

  auto connect_result = co_await ctx.async_connect(
      fd, &addr, static_cast<std::uint32_t>(sizeof(addr)));

  if (!connect_result && connect_result.error.value() != EINPROGRESS) {
    log::error("Failed to connect to {} - {}", socket_path,
               connect_result.error.message());
    ::close(fd);
    co_return nullptr;
  }

  auto client = std::make_unique<HttpClient>(ctx, fd, std::move(config));
  client->impl_->host = "localhost";
  co_return client;
}

auto HttpClient::request(HttpRequest req) -> task<HttpResponse> {
  if (!is_connected()) {
    co_return HttpResponse{HttpStatus::InternalServerError, {}, {}};
  }

  if (!req.headers.contains("Host")) {
    req.headers["Host"] = impl_->host;
  }

  if (impl_->config.keep_alive && !req.headers.contains("Connection")) {
    req.headers["Connection"] = "keep-alive";
  }

  auto request_data = req.serialize();

  auto write_result =
      co_await impl_->ctx->async_write(impl_->fd, io::buffer(request_data));

  if (!write_result) {
    log::error("Failed to write request: {}", write_result.error.message());
    co_return HttpResponse{HttpStatus::InternalServerError, {}, {}};
  }

  HttpResponseParser parser;
  std::vector<uint8_t> buffer(8192);
  std::size_t total_read = 0;

  while (total_read < impl_->config.max_response_size) {
    auto read_result =
        co_await impl_->ctx->async_read(impl_->fd, io::buffer(buffer));

    if (!read_result) {
      log::error("Failed to read response: {}", read_result.error.message());
      co_return HttpResponse{HttpStatus::InternalServerError, {}, {}};
    }

    if (read_result.bytes_transferred == 0) {
      break;
    }

    total_read += read_result.bytes_transferred;

    auto response = parser.parse(
        std::span{buffer.data(), read_result.bytes_transferred});

    if (response) {
      co_return std::move(*response);
    }
  }

  log::error("Failed to parse response or response too large");
  co_return HttpResponse{HttpStatus::InternalServerError, {}, {}};
}

auto HttpClient::get(std::string_view path, const HttpHeaders& headers)
    -> task<HttpResponse> {
  HttpRequest req{HttpMethod::GET, std::string(path), {}, headers, {}, {}};
  co_return co_await request(std::move(req));
}

auto HttpClient::post(std::string_view path, std::vector<uint8_t> body,
                      const HttpHeaders& headers) -> task<HttpResponse> {
  HttpRequest req{
      HttpMethod::POST, std::string(path), {}, headers, std::move(body), {}};
  co_return co_await request(std::move(req));
}

auto HttpClient::post_json(std::string_view path, std::string_view json,
                           const HttpHeaders& headers) -> task<HttpResponse> {
  HttpHeaders merged_headers = headers;
  merged_headers["Content-Type"] = "application/json";

  std::vector<uint8_t> body(json.begin(), json.end());
  HttpRequest req{HttpMethod::POST, std::string(path), {},
                  std::move(merged_headers), std::move(body), {}};
  co_return co_await request(std::move(req));
}

auto HttpClient::delete_(std::string_view path, const HttpHeaders& headers)
    -> task<HttpResponse> {
  HttpRequest req{HttpMethod::DELETE, std::string(path), {}, headers, {}, {}};
  co_return co_await request(std::move(req));
}

auto HttpClient::put(std::string_view path, std::vector<uint8_t> body,
                     const HttpHeaders& headers) -> task<HttpResponse> {
  HttpRequest req{
      HttpMethod::PUT, std::string(path), {}, headers, std::move(body), {}};
  co_return co_await request(std::move(req));
}

auto HttpClient::is_connected() const noexcept -> bool {
  return impl_ && impl_->fd >= 0;
}

auto HttpClient::close() -> void {
  if (impl_ && impl_->fd >= 0) {
    ::close(impl_->fd);
    impl_->fd = -1;
  }
}

}  // namespace taskmaster::http
