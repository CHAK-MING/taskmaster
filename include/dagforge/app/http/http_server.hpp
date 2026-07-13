#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/error.hpp"
#endif

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>


namespace dagforge {
class Runtime;
}

namespace dagforge::http {

class Router;

class HttpServer {
public:
  explicit HttpServer(Runtime &runtime);
  ~HttpServer();

  HttpServer(const HttpServer &) = delete;
  auto operator=(const HttpServer &) -> HttpServer & = delete;

  auto router() -> Router &;
  [[nodiscard]] auto set_tls_credentials(std::string cert_chain_file,
                                         std::string private_key_file)
      -> Result<void>;

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
