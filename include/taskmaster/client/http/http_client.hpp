#pragma once

#include "taskmaster/client/http/http_types.hpp"
#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/io/context.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string_view>

namespace taskmaster::http {

struct HttpClientConfig {
  std::chrono::milliseconds connect_timeout{30000};
  std::chrono::milliseconds read_timeout{30000};
  std::size_t max_response_size{10 * 1024 * 1024};  // 10MB
  bool keep_alive{true};
};

class HttpClient {
public:
  HttpClient(io::IoContext& ctx, int fd, HttpClientConfig config = {});
  ~HttpClient();

  HttpClient(const HttpClient&) = delete;
  auto operator=(const HttpClient&) -> HttpClient& = delete;
  HttpClient(HttpClient&&) noexcept;
  auto operator=(HttpClient&&) noexcept -> HttpClient&;

  static auto connect_tcp(io::IoContext& ctx, std::string_view host,
                          uint16_t port, HttpClientConfig config = {})
      -> task<std::unique_ptr<HttpClient>>;

  static auto connect_unix(io::IoContext& ctx, std::string_view socket_path,
                           HttpClientConfig config = {})
      -> task<std::unique_ptr<HttpClient>>;

  auto request(HttpRequest req) -> task<HttpResponse>;

  auto get(std::string_view path, const HttpHeaders& headers = {})
      -> task<HttpResponse>;

  auto post(std::string_view path, std::vector<uint8_t> body,
            const HttpHeaders& headers = {}) -> task<HttpResponse>;

  auto post_json(std::string_view path, std::string_view json,
                 const HttpHeaders& headers = {}) -> task<HttpResponse>;

  auto delete_(std::string_view path, const HttpHeaders& headers = {})
      -> task<HttpResponse>;

  auto put(std::string_view path, std::vector<uint8_t> body,
           const HttpHeaders& headers = {}) -> task<HttpResponse>;

  [[nodiscard]] auto is_connected() const noexcept -> bool;
  auto close() -> void;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::http
