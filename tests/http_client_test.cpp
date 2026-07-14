#include "dagforge/http/http_client.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <boost/asio/cancellation_signal.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace dagforge::http::test {
namespace {

auto connect_unix_after_yield(std::shared_ptr<std::atomic_bool> completed,
                              std::shared_ptr<std::atomic_bool> failed)
    -> spawn_task {
  auto connect_op = HttpClient::connect_unix(
      current_io_context(), std::string{"/tmp/nonexistent_deferred.sock"});
  co_await async_yield();
  auto client_res = co_await std::move(connect_op);
  failed->store(!client_res.has_value(), std::memory_order_release);
  completed->store(true, std::memory_order_release);
}

auto response_text(const HttpResponse &response) -> std::string_view {
  return {reinterpret_cast<const char *>(response.body.data()),
          response.body.size()};
}

class StallingHttpServer {
public:
  explicit StallingHttpServer(std::uint16_t port,
                              std::string response_prefix = {})
      : port_(port), response_prefix_(std::move(response_prefix)) {}

  ~StallingHttpServer() { stop(); }

  StallingHttpServer(const StallingHttpServer &) = delete;
  auto operator=(const StallingHttpServer &) -> StallingHttpServer & = delete;

  auto start() -> bool {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0) {
      return false;
    }
    int reuse = 1;
    (void)::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse,
                       sizeof(reuse));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(listener_, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener_, 1) != 0) {
      stop();
      return false;
    }
    worker_ = std::jthread([this](std::stop_token stop_token) {
      sockaddr_in peer{};
      socklen_t peer_size = sizeof(peer);
      const int connection =
          ::accept(listener_, reinterpret_cast<sockaddr *>(&peer), &peer_size);
      if (connection < 0) {
        return;
      }
      std::array<char, 4096> buffer{};
      (void)::recv(connection, buffer.data(), buffer.size(), 0);
      if (!response_prefix_.empty()) {
        (void)::send(connection, response_prefix_.data(),
                     response_prefix_.size(), 0);
      }
      while (!stop_token.stop_requested()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
      ::close(connection);
    });
    return true;
  }

  auto stop() -> void {
    if (worker_.joinable()) {
      worker_.request_stop();
    }
    if (listener_ >= 0) {
      ::shutdown(listener_, SHUT_RDWR);
      ::close(listener_);
      listener_ = -1;
    }
    if (worker_.joinable()) {
      worker_.join();
    }
  }

private:
  std::uint16_t port_{};
  std::string response_prefix_;
  int listener_{-1};
  std::jthread worker_;
};

class UnixResponseServer {
public:
  explicit UnixResponseServer(std::string path) : path_(std::move(path)) {}
  ~UnixResponseServer() { stop(); }

  auto start() -> bool {
    ::unlink(path_.c_str());
    listener_ = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (listener_ < 0) {
      return false;
    }
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (path_.size() >= sizeof(address.sun_path)) {
      stop();
      return false;
    }
    std::copy(path_.begin(), path_.end(), address.sun_path);
    if (::bind(listener_, reinterpret_cast<sockaddr *>(&address),
               sizeof(address)) != 0 ||
        ::listen(listener_, 1) != 0) {
      stop();
      return false;
    }
    worker_ = std::jthread([this] {
      const int connection = ::accept(listener_, nullptr, nullptr);
      if (connection < 0) {
        return;
      }
      std::array<char, 4096> request{};
      (void)::recv(connection, request.data(), request.size(), 0);
      constexpr std::string_view response =
          "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n"
          "Connection: close\r\n\r\nunix";
      (void)::send(connection, response.data(), response.size(), 0);
      ::close(connection);
    });
    return true;
  }

  auto stop() -> void {
    if (listener_ >= 0) {
      ::shutdown(listener_, SHUT_RDWR);
      ::close(listener_);
      listener_ = -1;
    }
    if (worker_.joinable()) {
      worker_.join();
    }
    ::unlink(path_.c_str());
  }

private:
  std::string path_;
  int listener_{-1};
  std::jthread worker_;
};

} // namespace

class HttpClientTest : public ::testing::Test {
protected:
  void SetUp() override {
    runtime_ = std::make_unique<Runtime>();
    ASSERT_TRUE(runtime_->start().has_value());
  }

  void TearDown() override {
    if (runtime_) {
      runtime_->stop();
    }
  }

  std::unique_ptr<Runtime> runtime_;
};

TEST_F(HttpClientTest, ConnectUnixFailsForNonExistentSocket) {
  std::atomic<bool> completed{false};
  std::unique_ptr<HttpClient> result_client;

  auto test_task = [&]() -> spawn_task {
    auto client_res = co_await HttpClient::connect_unix(
        current_io_context(), "/tmp/nonexistent_socket_12345.sock");

    if (client_res) {
      result_client = std::move(*client_res);
    }
    completed = true;
  };

  auto t = test_task();
  runtime_->spawn_external(std::move(t));

  dagforge::test::busy_wait_for(std::chrono::milliseconds(100));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed.load() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_TRUE(completed.load());
  EXPECT_EQ(result_client, nullptr);
}

TEST_F(HttpClientTest, ConnectUnixOwnsSocketPathAcrossDeferredStart) {
  auto completed = std::make_shared<std::atomic_bool>(false);
  auto failed = std::make_shared<std::atomic_bool>(false);
  runtime_->spawn_external(connect_unix_after_yield(completed, failed));

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!completed->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(completed->load(std::memory_order_acquire));
  EXPECT_TRUE(failed->load(std::memory_order_acquire));
}

TEST_F(HttpClientTest, HttpClientConfigDefaults) {
  HttpClientConfig config;

  EXPECT_EQ(config.dns_timeout, std::chrono::milliseconds(5000));
  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.tls_handshake_timeout,
            std::chrono::milliseconds(30000));
  EXPECT_EQ(config.write_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.first_byte_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(30000));
  EXPECT_EQ(config.max_response_size, 10 * 1024 * 1024);
  EXPECT_TRUE(config.keep_alive);
}

TEST_F(HttpClientTest, HttpClientConfigCustomValues) {
  HttpClientConfig config{
      .dns_timeout = std::chrono::milliseconds(4000),
      .connect_timeout = std::chrono::milliseconds(5000),
      .tls_handshake_timeout = std::chrono::milliseconds(6000),
      .write_timeout = std::chrono::milliseconds(7000),
      .first_byte_timeout = std::chrono::milliseconds(8000),
      .read_timeout = std::chrono::milliseconds(10000),
      .max_response_size = 1024,
      .keep_alive = false,
  };

  EXPECT_EQ(config.dns_timeout, std::chrono::milliseconds(4000));
  EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(5000));
  EXPECT_EQ(config.tls_handshake_timeout,
            std::chrono::milliseconds(6000));
  EXPECT_EQ(config.write_timeout, std::chrono::milliseconds(7000));
  EXPECT_EQ(config.first_byte_timeout, std::chrono::milliseconds(8000));
  EXPECT_EQ(config.read_timeout, std::chrono::milliseconds(10000));
  EXPECT_EQ(config.max_response_size, 1024U);
  EXPECT_FALSE(config.keep_alive);
}

TEST_F(HttpClientTest, RejectsResolvedEndpointsBeforeConnect) {
  HttpClientConfig config;
  config.endpoint_allowed = [](const boost::asio::ip::address &) {
    return false;
  };
  auto attempt = [config = std::move(config)]() mutable
      -> task<Result<std::unique_ptr<HttpClient>>> {
    co_return co_await HttpClient::connect_tcp(
        current_io_context(), "localhost", 9, std::move(config));
  };
  auto connected = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(connected.has_value());
  EXPECT_EQ(connected.error(), make_error_code(Error::Unauthorized));
}

TEST_F(HttpClientTest, RejectsIncompleteMutualTlsIdentity) {
  HttpClientConfig config;
  config.tls_client_cert_file = "/tmp/client.pem";
  auto attempt = [config = std::move(config)]() mutable
      -> task<Result<std::unique_ptr<HttpClient>>> {
    co_return co_await HttpClient::connect_tls(
        current_io_context(), "localhost", 443, std::move(config));
  };
  auto connected = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(connected.has_value());
  EXPECT_EQ(connected.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(HttpClientTest, ReportsConnectStageFailure) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  auto attempt = [port]() -> task<Result<std::unique_ptr<HttpClient>>> {
    co_return co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port,
        HttpClientConfig{.connect_timeout = std::chrono::milliseconds(250)});
  };
  auto connected = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(connected.has_value());
  EXPECT_EQ(connected.error(),
            make_error_code(HttpClientError::ConnectFailure));
}

TEST_F(HttpClientTest, ReportsFirstByteTimeout) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  StallingHttpServer server(port);
  ASSERT_TRUE(server.start());

  auto attempt = [port]() -> task<Result<HttpResponse>> {
    HttpClientConfig config{
        .dns_timeout = std::chrono::milliseconds(250),
        .connect_timeout = std::chrono::milliseconds(250),
        .write_timeout = std::chrono::milliseconds(250),
        .first_byte_timeout = std::chrono::milliseconds(50),
        .read_timeout = std::chrono::milliseconds(250),
        .keep_alive = false,
    };
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    HttpRequest request;
    request.method = HttpMethod::GET;
    request.path = "/silent";
    co_return co_await (*connected)->request(std::move(request));
  };

  auto response = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(response.has_value());
  EXPECT_EQ(response.error(),
            make_error_code(HttpClientError::FirstByteTimeout));
  EXPECT_EQ(response.error(), std::errc::timed_out);
  server.stop();
}

TEST_F(HttpClientTest, ReportsResponseReadTimeout) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  StallingHttpServer server(
      port,
      "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: "
      "keep-alive\r\n\r\npart");
  ASSERT_TRUE(server.start());

  auto attempt = [port]() -> task<Result<HttpResponse>> {
    HttpClientConfig config{
        .dns_timeout = std::chrono::milliseconds(250),
        .connect_timeout = std::chrono::milliseconds(250),
        .write_timeout = std::chrono::milliseconds(250),
        .first_byte_timeout = std::chrono::milliseconds(250),
        .read_timeout = std::chrono::milliseconds(50),
        .keep_alive = false,
    };
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    HttpRequest request;
    request.method = HttpMethod::GET;
    request.path = "/partial";
    co_return co_await (*connected)->request(std::move(request));
  };

  auto response = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(response.has_value());
  EXPECT_EQ(response.error(), make_error_code(HttpClientError::ReadTimeout));
  EXPECT_EQ(response.error(), std::errc::timed_out);
  server.stop();
}

TEST_F(HttpClientTest, EnforcesResponseHeaderAndBodyLimits) {
  const auto body_port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(body_port, 0);
  StallingHttpServer body_server(
      body_port,
      "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\n"
      "12345678");
  ASSERT_TRUE(body_server.start());

  auto body_attempt = [body_port]() -> task<Result<HttpResponse>> {
    HttpClientConfig config;
    config.max_response_size = 4;
    config.keep_alive = false;
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", body_port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    co_return co_await (*connected)->get("/oversized");
  };
  auto oversized = sync_wait_on_runtime(*runtime_, body_attempt());
  ASSERT_FALSE(oversized.has_value());
  EXPECT_EQ(oversized.error(), make_error_code(Error::ResourceExhausted));
  body_server.stop();

  const auto header_port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(header_port, 0);
  StallingHttpServer header_server(
      header_port,
      "HTTP/1.1 200 OK\r\nX-One: 1\r\nX-Two: 2\r\nContent-Length: 0\r\n"
      "Connection: close\r\n\r\n");
  ASSERT_TRUE(header_server.start());
  auto header_attempt = [header_port]() -> task<Result<HttpResponse>> {
    HttpClientConfig config;
    config.max_response_headers = 1;
    config.keep_alive = false;
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", header_port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    co_return co_await (*connected)->get("/too-many-headers");
  };
  auto too_many_headers =
      sync_wait_on_runtime(*runtime_, header_attempt());
  ASSERT_FALSE(too_many_headers.has_value());
  EXPECT_EQ(too_many_headers.error(),
            make_error_code(Error::ResourceExhausted));
  header_server.stop();
}

TEST_F(HttpClientTest, ReportsMalformedResponseAsProtocolError) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  StallingHttpServer server(port, "NOT HTTP\r\n\r\n");
  ASSERT_TRUE(server.start());

  auto attempt = [port]() -> task<Result<HttpResponse>> {
    HttpClientConfig config;
    config.first_byte_timeout = std::chrono::milliseconds(250);
    config.keep_alive = false;
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    co_return co_await (*connected)->get("/malformed");
  };
  auto response = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(response.has_value());
  EXPECT_EQ(response.error(), make_error_code(Error::ProtocolError));
  server.stop();
}

TEST_F(HttpClientTest, CancelsInFlightRequestThroughCancellationSlot) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  StallingHttpServer server(port);
  ASSERT_TRUE(server.start());

  auto signal = std::make_shared<boost::asio::cancellation_signal>();
  std::jthread canceller([signal] {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    signal->emit(boost::asio::cancellation_type::all);
  });
  auto attempt = [port, signal]() -> task<Result<HttpResponse>> {
    HttpClientConfig config;
    config.first_byte_timeout = std::chrono::seconds(2);
    config.keep_alive = false;
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port, config);
    if (!connected) {
      co_return fail(connected.error());
    }
    HttpRequest request;
    request.method = HttpMethod::GET;
    request.path = "/cancelled";
    co_return co_await (*connected)->request(std::move(request), signal->slot());
  };
  auto response = sync_wait_on_runtime(*runtime_, attempt());
  ASSERT_FALSE(response.has_value());
  EXPECT_EQ(response.error(), std::errc::operation_canceled);
  server.stop();
}

TEST_F(HttpClientTest, SupportsMoveConstructionAndAssignment) {
  const auto first_port = dagforge::test::pick_unused_tcp_port_or_zero();
  const auto second_port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(first_port, 0);
  ASSERT_NE(second_port, 0);
  HttpServer first_server(*runtime_);
  HttpServer second_server(*runtime_);
  first_server.router().get("/first", [](HttpRequest) -> task<HttpResponse> {
    co_return HttpResponse::ok().set_body("first");
  });
  second_server.router().get("/second", [](HttpRequest) -> task<HttpResponse> {
    co_return HttpResponse::ok().set_body("second");
  });
  ASSERT_TRUE(first_server.start("127.0.0.1", first_port).has_value());
  ASSERT_TRUE(second_server.start("127.0.0.1", second_port).has_value());

  auto scenario = [first_port, second_port]() -> task<Result<void>> {
    auto first = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", first_port);
    auto second = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", second_port);
    if (!first || !second) {
      co_return fail(first ? second.error() : first.error());
    }
    HttpClient moved{std::move(**first)};
    auto first_response = co_await moved.get("/first");
    if (!first_response || response_text(*first_response) != "first") {
      co_return fail(first_response ? Error::ProtocolError
                                    : first_response.error());
    }
    moved = std::move(**second);
    auto second_response = co_await moved.get("/second");
    if (!second_response || response_text(*second_response) != "second") {
      co_return fail(second_response ? Error::ProtocolError
                                     : second_response.error());
    }
    co_return ok();
  };
  auto result = sync_wait_on_runtime(*runtime_, scenario());
  EXPECT_TRUE(result.has_value()) << result.error().message();
  first_server.stop();
  second_server.stop();
}

TEST_F(HttpClientTest, ReusesConnectionAcrossSupportedMethods) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);

  HttpServer server(*runtime_);
  auto echo = [](HttpRequest request) -> task<HttpResponse> {
    HttpResponse response;
    response.status = HttpStatus::Ok;
    response.set_header("X-Method", std::string{http_method_name(request.method)});
    response.set_body(std::string{request.body_as_string()});
    co_return response;
  };
  server.router().get("/get", echo);
  server.router().post("/post", echo);
  server.router().put("/put", echo);
  server.router().del("/delete", echo);
  server.router().add_route(HttpMethod::PATCH, "/patch", echo);
  server.router().add_route(HttpMethod::OPTIONS, "/options", echo);
  server.router().add_route(HttpMethod::HEAD, "/head", echo);
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  auto scenario = [port]() -> task<Result<void>> {
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port,
        HttpClientConfig{.keep_alive = true});
    if (!connected) {
      co_return fail(connected.error());
    }
    auto &client = **connected;

    auto get = co_await client.get("/get?mode=test");
    if (!get || get->status != HttpStatus::Ok || !client.is_reusable()) {
      co_return fail(get ? Error::ProtocolError : get.error());
    }
    auto post = co_await client.post("/post", {'p', 'o', 's', 't'});
    if (!post || response_text(*post) != "post") {
      co_return fail(post ? Error::ProtocolError : post.error());
    }
    auto json = co_await client.post_json("/post", R"({"ok":true})");
    if (!json || response_text(*json) != R"({"ok":true})") {
      co_return fail(json ? Error::ProtocolError : json.error());
    }
    auto put = co_await client.put("/put", {'p', 'u', 't'});
    if (!put || response_text(*put) != "put") {
      co_return fail(put ? Error::ProtocolError : put.error());
    }
    auto deleted = co_await client.delete_("/delete");
    if (!deleted) {
      co_return fail(deleted.error());
    }

    for (const auto [method, path] : {
             std::pair{HttpMethod::PATCH, "/patch"},
             std::pair{HttpMethod::OPTIONS, "/options"},
             std::pair{HttpMethod::HEAD, "/head"},
         }) {
      HttpRequest request;
      request.method = method;
      request.path = path;
      auto response = co_await client.request(std::move(request));
      if (!response || response->status != HttpStatus::Ok) {
        co_return fail(response ? Error::ProtocolError : response.error());
      }
    }

    client.close();
    if (client.is_connected() || client.is_reusable()) {
      co_return fail(Error::InvalidState);
    }
    auto closed = co_await client.get("/get");
    if (closed || closed.error() != make_error_code(Error::InvalidState)) {
      co_return fail(Error::ProtocolError);
    }
    co_return ok();
  };

  auto result = sync_wait_on_runtime(*runtime_, scenario());
  EXPECT_TRUE(result.has_value()) << result.error().message();
  server.stop();
}

TEST_F(HttpClientTest, RejectsMalformedTargetsAndFramingHeaders) {
  const auto port = dagforge::test::pick_unused_tcp_port_or_zero();
  ASSERT_NE(port, 0);
  HttpServer server(*runtime_);
  server.router().get("/ok", [](HttpRequest) -> task<HttpResponse> {
    co_return HttpResponse::ok();
  });
  ASSERT_TRUE(server.start("127.0.0.1", port, false).has_value());

  auto scenario = [port]() -> task<Result<void>> {
    auto connected = co_await HttpClient::connect_tcp(
        current_io_context(), "127.0.0.1", port);
    if (!connected) {
      co_return fail(connected.error());
    }
    for (auto request : {
             HttpRequest{.method = HttpMethod::GET, .path = "relative"},
             HttpRequest{.method = HttpMethod::GET, .path = "/bad\r\npath"},
         }) {
      auto response = co_await (*connected)->request(std::move(request));
      if (response || response.error() != make_error_code(Error::InvalidArgument)) {
        co_return fail(Error::ProtocolError);
      }
    }

    for (std::string_view header : {"Content-Length", "Transfer-Encoding"}) {
      HttpRequest request;
      request.method = HttpMethod::GET;
      request.path = "/ok";
      request.headers.add(std::string{header}, "1");
      auto response = co_await (*connected)->request(std::move(request));
      if (response || response.error() != make_error_code(Error::InvalidArgument)) {
        co_return fail(Error::ProtocolError);
      }
    }
    co_return ok();
  };

  auto result = sync_wait_on_runtime(*runtime_, scenario());
  EXPECT_TRUE(result.has_value()) << result.error().message();
  server.stop();
}

TEST_F(HttpClientTest, SendsRequestOverUnixSocket) {
  const auto path = dagforge::test::make_temp_path("dagforge_http_unix_");
  ASSERT_FALSE(path.empty());
  ::unlink(path.c_str());
  UnixResponseServer server(path);
  ASSERT_TRUE(server.start());

  auto scenario = [path]() -> task<Result<HttpResponse>> {
    auto connected = co_await HttpClient::connect_unix(
        current_io_context(), path, HttpClientConfig{.keep_alive = false});
    if (!connected) {
      co_return fail(connected.error());
    }
    co_return co_await (*connected)->get("/unix");
  };
  auto response = sync_wait_on_runtime(*runtime_, scenario());
  ASSERT_TRUE(response.has_value()) << response.error().message();
  EXPECT_EQ(response_text(*response), "unix");
  server.stop();
}

TEST_F(HttpClientTest, ReportsDnsAndTlsConfigurationFailures) {
  auto dns = []() -> task<Result<std::unique_ptr<HttpClient>>> {
    co_return co_await HttpClient::connect_tcp(
        current_io_context(), "does-not-exist.invalid", 80,
        HttpClientConfig{.dns_timeout = std::chrono::seconds(2)});
  };
  auto dns_result = sync_wait_on_runtime(*runtime_, dns());
  ASSERT_FALSE(dns_result.has_value());
  EXPECT_EQ(dns_result.error(), make_error_code(HttpClientError::DnsFailure));

  for (auto config : {
           HttpClientConfig{.tls_min_version = "1.1"},
           HttpClientConfig{.tls_ca_file = "/definitely/missing/ca.pem"},
           HttpClientConfig{
               .tls_client_cert_file = "/definitely/missing/client.pem",
               .tls_client_key_file = "/definitely/missing/client.key"},
       }) {
    auto attempt = [config = std::move(config)]() mutable
        -> task<Result<std::unique_ptr<HttpClient>>> {
      co_return co_await HttpClient::connect_tls(
          current_io_context(), "localhost", 443, std::move(config));
    };
    auto result = sync_wait_on_runtime(*runtime_, attempt());
    ASSERT_FALSE(result.has_value());
  }
}

} // namespace dagforge::http::test
