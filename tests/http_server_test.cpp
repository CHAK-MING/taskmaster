#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"
#include "dagforge/core/runtime.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace dagforge::http::test {
namespace {

class ListeningSocket {
public:
  ListeningSocket() = default;
  ~ListeningSocket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  ListeningSocket(const ListeningSocket &) = delete;
  auto operator=(const ListeningSocket &) -> ListeningSocket & = delete;

  auto bind_loopback(std::uint16_t port) -> bool {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
      return false;
    }

    int reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
      return false;
    }
    return ::listen(fd_, SOMAXCONN) == 0;
  }

private:
  int fd_{-1};
};

[[nodiscard]] auto send_raw_request(std::uint16_t port,
                                    std::string_view request) -> std::string {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return {};
  }
  const auto close_socket = std::unique_ptr<int, void (*)(int *)>{
      new int(fd), [](int *value) {
        if (value != nullptr) {
          ::close(*value);
          delete value;
        }
      }};

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bool connected = false;
  for (int attempt = 0; attempt < 100; ++attempt) {
    if (::connect(fd, reinterpret_cast<sockaddr *>(&address),
                  sizeof(address)) == 0) {
      connected = true;
      break;
    }
    if (errno != ECONNREFUSED && errno != EINTR) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  if (!connected) {
    return {};
  }

  std::size_t sent = 0;
  while (sent < request.size()) {
    const auto count =
        ::send(fd, request.data() + sent, request.size() - sent, 0);
    if (count <= 0) {
      return {};
    }
    sent += static_cast<std::size_t>(count);
  }

  std::string response;
  std::array<char, 4096> buffer{};
  while (true) {
    const auto count = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (count <= 0) {
      break;
    }
    response.append(buffer.data(), static_cast<std::size_t>(count));
  }
  return response;
}

} // namespace

TEST(HttpServerTest, StartFailsWhenPortAlreadyInUse) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  ListeningSocket occupied;
  ASSERT_TRUE(occupied.bind_loopback(port));

  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());

  HttpServer server(runtime);
  const auto result = server.start("127.0.0.1", port, false);

  EXPECT_FALSE(result.has_value());
  EXPECT_FALSE(server.is_running());

  server.stop();
  runtime.stop();
}

TEST(HttpServerTest, RejectsUnsupportedMethodWithoutRoutingAsGet) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());
  HttpServer server(runtime);
  std::atomic_bool routed{false};
  server.router().get(
      "/resource", [&routed](HttpRequest) -> task<HttpResponse> {
        routed.store(true, std::memory_order_release);
        co_return HttpResponse::ok();
      });
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  const auto response = send_raw_request(
      port,
      "TRACE /resource HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  EXPECT_NE(response.find(" 405 "), std::string::npos) << response;
  EXPECT_FALSE(routed.load(std::memory_order_acquire));

  server.stop();
  runtime.stop();
}

TEST(HttpServerTest, StopClosesIdleConnections) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());
  HttpServer server(runtime);
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr *>(&address),
                      sizeof(address)),
            0);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  const auto started = std::chrono::steady_clock::now();
  server.stop();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  EXPECT_LT(elapsed, std::chrono::seconds(5));

  std::array<char, 16> data{};
  const auto received = ::recv(fd, data.data(), data.size(), 0);
  EXPECT_LE(received, 0);
  ::close(fd);
  runtime.stop();
}

TEST(HttpServerTest, ValidatesConfigurationTlsAndRunningMutations) {
  Runtime runtime(2);
  ASSERT_TRUE(runtime.start().has_value());
  HttpServer server(runtime);

  HttpServerConfig invalid;
  invalid.max_request_header_bytes = 0;
  EXPECT_EQ(server.configure(invalid).error(),
            make_error_code(Error::InvalidArgument));
  invalid = HttpServerConfig{};
  invalid.max_request_body_bytes = 0;
  EXPECT_EQ(server.configure(invalid).error(),
            make_error_code(Error::InvalidArgument));
  invalid = HttpServerConfig{};
  invalid.connection_idle_timeout = std::chrono::milliseconds::zero();
  EXPECT_EQ(server.configure(invalid).error(),
            make_error_code(Error::InvalidArgument));
  invalid = HttpServerConfig{};
  invalid.max_connections = 0;
  EXPECT_EQ(server.configure(invalid).error(),
            make_error_code(Error::InvalidArgument));
  invalid = HttpServerConfig{};
  invalid.max_requests_per_connection = 0;
  EXPECT_EQ(server.configure(invalid).error(),
            make_error_code(Error::InvalidArgument));

  EXPECT_EQ(server.set_request_body_limit(0).error(),
            make_error_code(Error::InvalidState));
  EXPECT_TRUE(server.set_request_body_limit(1024).has_value());
  EXPECT_EQ(server.set_tls_credentials({}, "key.pem", "1.2").error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(server.set_tls_credentials("cert.pem", {}, "1.2").error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(server.set_tls_credentials("cert.pem", "key.pem", "1.1").error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(server.set_tls_credentials("missing-cert.pem", "missing-key.pem",
                                       "1.3")
                .error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(server.start("not-an-ip", 12345, false).error(),
            make_error_code(Error::InvalidArgument));

  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  server.router().get("/ping", [](HttpRequest) -> task<HttpResponse> {
    co_return HttpResponse::ok().set_body("pong");
  });
  ASSERT_TRUE(server.start("0.0.0.0", port, true).has_value());
  EXPECT_TRUE(server.is_running());
  EXPECT_EQ(server.configure(HttpServerConfig{}).error(),
            make_error_code(Error::InvalidArgument));
  EXPECT_EQ(server.set_request_body_limit(2048).error(),
            make_error_code(Error::InvalidState));
  EXPECT_EQ(server.set_tls_credentials("cert.pem", "key.pem", "1.2").error(),
            make_error_code(Error::InvalidArgument));

  const auto response = send_raw_request(
      port,
      "GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  EXPECT_NE(response.find(" 200 "), std::string::npos) << response;
  EXPECT_NE(response.find("pong"), std::string::npos) << response;
  server.stop();
  server.stop();
  EXPECT_FALSE(server.is_running());
  runtime.stop();
}

TEST(HttpServerTest, EnforcesConnectionBodyAndKeepAliveLimits) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  Runtime runtime(1);
  ASSERT_TRUE(runtime.start().has_value());
  HttpServer server(runtime);
  HttpServerConfig config;
  config.max_request_body_bytes = 4;
  config.max_connections = 1;
  config.max_requests_per_connection = 1;
  config.connection_idle_timeout = std::chrono::milliseconds(100);
  ASSERT_TRUE(server.configure(config).has_value());
  server.router().post("/echo", [](HttpRequest request) -> task<HttpResponse> {
    co_return HttpResponse::ok().set_body(
        std::string{request.body_as_string()});
  });
  ASSERT_TRUE(server.start("127.0.0.1", port).has_value());

  const int idle_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(idle_fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::connect(idle_fd, reinterpret_cast<sockaddr *>(&address),
                      sizeof(address)),
            0);
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  const auto rejected = send_raw_request(
      port,
      "GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
  EXPECT_TRUE(rejected.empty()) << rejected;
  ::close(idle_fd);
  std::this_thread::sleep_for(std::chrono::milliseconds(30));

  const auto oversized = send_raw_request(
      port,
      "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: 8\r\n"
      "Connection: close\r\n\r\n12345678");
  EXPECT_TRUE(oversized.empty()) << oversized;

  const auto absolute_target = send_raw_request(
      port,
      "GET http://localhost/missing HTTP/1.1\r\nHost: localhost\r\n"
      "Connection: keep-alive\r\n\r\n");
  EXPECT_NE(absolute_target.find(" 404 "), std::string::npos)
      << absolute_target;

  server.stop();
  runtime.stop();
}

} // namespace dagforge::http::test
