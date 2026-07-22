#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/http/http_types.hpp"

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace dagforge {
class Runtime;
}

namespace dagforge::http {

class Router;

struct HttpServerConfig {
  std::uint64_t max_request_header_bytes{64ULL * 1024ULL};
  std::uint64_t max_request_body_bytes{1024ULL * 1024ULL};
  std::chrono::milliseconds connection_idle_timeout{std::chrono::seconds(30)};
  std::size_t max_connections{1024};
  std::size_t max_requests_per_connection{100};
};

class HttpServer {
public:
  explicit HttpServer(Runtime &runtime);
  ~HttpServer();

  HttpServer(const HttpServer &) = delete;
  auto operator=(const HttpServer &) -> HttpServer & = delete;

  auto router() -> Router &;
  [[nodiscard]] auto set_tls_credentials(std::string cert_chain_file,
                                         std::string private_key_file,
                                         std::string minimum_version = "1.2")
      -> Result<void>;
  auto configure(HttpServerConfig config) -> Result<void>;
  auto set_request_body_limit(std::uint64_t bytes) -> Result<void>;

  auto start(std::string_view host, uint16_t port) -> Result<void>;
  auto start(std::string_view host, uint16_t port, bool reuse_port)
      -> Result<void>;
  auto stop() -> void;

  [[nodiscard]] auto is_running() const -> bool;

private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
};

} // namespace dagforge::http
