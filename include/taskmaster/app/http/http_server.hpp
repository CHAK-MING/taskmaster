#pragma once

#include "taskmaster/app/http/router.hpp"
#include "taskmaster/core/runtime.hpp"

#include <cstdint>
#include <memory>
#include <string_view>

namespace taskmaster::http {

class HttpServer {
public:
  explicit HttpServer(Runtime& runtime);
  ~HttpServer();

  HttpServer(const HttpServer&) = delete;
  auto operator=(const HttpServer&) -> HttpServer& = delete;

  auto router() -> Router&;

  auto start(std::string_view host, uint16_t port) -> task<void>;
  auto stop() -> void;

  [[nodiscard]] auto is_running() const -> bool;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster::http
