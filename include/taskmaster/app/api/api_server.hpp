#pragma once

#include "taskmaster/app/api/websocket_hub.hpp"

#include <cstdint>
#include <memory>
#include <string>

namespace taskmaster {

class Application;

class ApiServer {
public:
  ApiServer(Application& app, uint16_t port = 8080,
            const std::string& host = "127.0.0.1");
  ~ApiServer();

  ApiServer(const ApiServer&) = delete;
  auto operator=(const ApiServer&) -> ApiServer& = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  [[nodiscard]] auto hub() -> WebSocketHub&;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster
