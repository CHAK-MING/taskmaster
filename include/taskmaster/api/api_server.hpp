#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "taskmaster/api/websocket_hub.hpp"

namespace taskmaster {

class Application;

class ApiServer {
public:
  ApiServer(Application &app, uint16_t port = 8080,
            const std::string &host = "127.0.0.1");
  ~ApiServer();

  ApiServer(const ApiServer &) = delete;
  auto operator=(const ApiServer &) -> ApiServer & = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  [[nodiscard]] auto hub() -> WebSocketHub & { return hub_; }

private:
  auto setup_routes() -> void;
  auto setup_websocket() -> void;

  Application &app_;
  uint16_t port_;
  std::string host_;
  WebSocketHub hub_;

  std::unique_ptr<crow::SimpleApp> crow_app_;
  std::thread server_thread_;
  std::atomic<bool> running_{false};
};

} // namespace taskmaster
