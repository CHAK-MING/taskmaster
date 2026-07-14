#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/http/http_types.hpp"
#include "dagforge/io/context.hpp"

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>

#include <array>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>
#endif

namespace dagforge::http {

enum class HttpClientError : std::uint8_t {
  Success,
  DnsFailure,
  DnsTimeout,
  ConnectFailure,
  ConnectTimeout,
  TlsHandshakeFailure,
  TlsHandshakeTimeout,
  WriteFailure,
  WriteTimeout,
  FirstByteFailure,
  FirstByteTimeout,
  ReadFailure,
  ReadTimeout,
};

class HttpClientErrorCategory final : public std::error_category {
public:
  [[nodiscard]] auto name() const noexcept -> const char * override {
    return "dagforge.http.client";
  }

  [[nodiscard]] auto message(int value) const -> std::string override {
    static constexpr std::array<std::string_view, 13> messages{
        "success",
        "HTTP DNS resolution failed",
        "HTTP DNS resolution timed out",
        "HTTP connection failed",
        "HTTP connection timed out",
        "HTTP TLS handshake failed",
        "HTTP TLS handshake timed out",
        "HTTP request write failed",
        "HTTP request write timed out",
        "HTTP response first byte failed",
        "HTTP response first byte timed out",
        "HTTP response read failed",
        "HTTP response read timed out",
    };
    const auto index = static_cast<std::size_t>(value);
    return index < messages.size() ? std::string{messages[index]}
                                   : std::string{"unknown HTTP client error"};
  }

  using std::error_category::equivalent;

  [[nodiscard]] auto
  equivalent(int code, const std::error_condition &condition) const noexcept
      -> bool override {
    if (condition.category() != std::generic_category() ||
        condition.value() != static_cast<int>(std::errc::timed_out)) {
      return false;
    }
    switch (static_cast<HttpClientError>(code)) {
    case HttpClientError::DnsTimeout:
    case HttpClientError::ConnectTimeout:
    case HttpClientError::TlsHandshakeTimeout:
    case HttpClientError::WriteTimeout:
    case HttpClientError::FirstByteTimeout:
    case HttpClientError::ReadTimeout:
      return true;
    default:
      return false;
    }
  }
};

[[nodiscard]] inline auto http_client_error_category() noexcept
    -> const HttpClientErrorCategory & {
  static const HttpClientErrorCategory category;
  return category;
}

[[nodiscard]] inline auto make_error_code(HttpClientError error) noexcept
    -> std::error_code {
  return {std::to_underlying(error), http_client_error_category()};
}

struct HttpClientConfig {
  std::chrono::milliseconds dns_timeout{5000};
  std::chrono::milliseconds connect_timeout{30000};
  std::chrono::milliseconds tls_handshake_timeout{30000};
  std::chrono::milliseconds write_timeout{30000};
  std::chrono::milliseconds first_byte_timeout{30000};
  std::chrono::milliseconds read_timeout{30000};
  std::size_t max_response_headers{128};
  std::size_t max_response_header_size{64UL * 1024UL};
  std::size_t max_response_size{10UL * 1024UL * 1024UL};
  bool keep_alive{true};
  std::string tls_min_version{"1.2"};
  std::string tls_ca_file;
  std::string tls_client_cert_file;
  std::string tls_client_key_file;
  std::function<bool(const boost::asio::ip::address &)> endpoint_allowed;
};

template <typename T>
concept HttpConnector = requires(T t, HttpRequest req) {
  { t.request(std::move(req)) } -> std::same_as<task<Result<HttpResponse>>>;
  { t.is_connected() } -> std::same_as<bool>;
};

class HttpClient {
public:
  using TlsStream = boost::asio::ssl::stream<boost::asio::ip::tcp::socket>;
  using SocketVariant =
      std::variant<boost::asio::ip::tcp::socket,
                   boost::asio::local::stream_protocol::socket, TlsStream>;

  HttpClient(SocketVariant socket, HttpClientConfig config = {});
  ~HttpClient();

  HttpClient(const HttpClient &) = delete;
  auto operator=(const HttpClient &) -> HttpClient & = delete;
  HttpClient(HttpClient &&) noexcept;
  auto operator=(HttpClient &&) noexcept -> HttpClient &;

  static auto connect_tcp(io::IoContext &ctx, std::string host, uint16_t port,
                          HttpClientConfig config = {},
                          boost::asio::cancellation_slot cancellation = {})
      -> task<Result<std::unique_ptr<HttpClient>>>;

  static auto connect_tls(io::IoContext &ctx, std::string host, uint16_t port,
                          HttpClientConfig config = {},
                          boost::asio::cancellation_slot cancellation = {})
      -> task<Result<std::unique_ptr<HttpClient>>>;

  static auto connect_unix(io::IoContext &ctx, std::string socket_path,
                           HttpClientConfig config = {},
                           boost::asio::cancellation_slot cancellation = {})
      -> task<Result<std::unique_ptr<HttpClient>>>;

  auto request(HttpRequest req,
               boost::asio::cancellation_slot cancellation = {})
      -> task<Result<HttpResponse>>;

  auto get(std::string_view path, const HttpHeaders &headers = {})
      -> task<Result<HttpResponse>>;

  auto post(std::string_view path, std::vector<uint8_t> body,
            const HttpHeaders &headers = {}) -> task<Result<HttpResponse>>;

  auto post_json(std::string_view path, std::string_view json,
                 const HttpHeaders &headers = {}) -> task<Result<HttpResponse>>;

  auto delete_(std::string_view path, const HttpHeaders &headers = {})
      -> task<Result<HttpResponse>>;

  auto put(std::string_view path, std::vector<uint8_t> body,
           const HttpHeaders &headers = {}) -> task<Result<HttpResponse>>;

  [[nodiscard]] auto is_connected() const noexcept -> bool;
  [[nodiscard]] auto is_reusable() const noexcept -> bool;
  auto close() -> void;

private:
  HttpClient(SocketVariant socket, HttpClientConfig config,
             std::shared_ptr<boost::asio::ssl::context> tls_context);

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge::http

template <>
struct std::is_error_code_enum<dagforge::http::HttpClientError>
    : std::true_type {};
