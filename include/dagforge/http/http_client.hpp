#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/http/http_types.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/io/context.hpp"

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>
#endif


namespace dagforge::http {

struct HttpClientConfig {
  std::chrono::milliseconds connect_timeout{30000};
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
  using TlsStream =
      boost::asio::ssl::stream<boost::asio::ip::tcp::socket>;
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
  auto close() -> void;

private:
  HttpClient(SocketVariant socket, HttpClientConfig config,
             std::shared_ptr<boost::asio::ssl::context> tls_context);

  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge::http
