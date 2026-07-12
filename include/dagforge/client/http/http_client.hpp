#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/coroutine.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/io/context.hpp"

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/local/stream_protocol.hpp>

#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
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
  std::size_t max_response_size{10UL * 1024UL * 1024UL};
  bool keep_alive{true};
};

template <typename T>
concept HttpConnector = requires(T t, HttpRequest req) {
  { t.request(std::move(req)) } -> std::same_as<task<Result<HttpResponse>>>;
  { t.is_connected() } -> std::same_as<bool>;
};

class HttpClient {
public:
  using SocketVariant =
      std::variant<boost::asio::ip::tcp::socket,
                   boost::asio::local::stream_protocol::socket>;

  HttpClient(SocketVariant socket, HttpClientConfig config = {});
  ~HttpClient();

  HttpClient(const HttpClient &) = delete;
  auto operator=(const HttpClient &) -> HttpClient & = delete;
  HttpClient(HttpClient &&) noexcept;
  auto operator=(HttpClient &&) noexcept -> HttpClient &;

  static auto connect_tcp(io::IoContext &ctx, std::string host, uint16_t port,
                          HttpClientConfig config = {})
      -> task<Result<std::unique_ptr<HttpClient>>>;

  static auto connect_unix(io::IoContext &ctx, std::string socket_path,
                           HttpClientConfig config = {})
      -> task<Result<std::unique_ptr<HttpClient>>>;

  auto request(HttpRequest req) -> task<Result<HttpResponse>>;

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
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge::http
