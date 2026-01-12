#pragma once

#include "taskmaster/app/http/websocket.hpp"
#include <memory>

namespace taskmaster {

class Application;

namespace http {
class HttpServer;
class WebSocketHub;
}  // namespace http

class ApiServer {
public:
  explicit ApiServer(Application& app);
  ~ApiServer();

  void start();
  void stop();
  bool is_running() const;
  http::WebSocketHub& websocket_hub();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace taskmaster
