#pragma once

#include "taskmaster/app/api/websocket_hub.hpp"

#include <string>
#include <string_view>

namespace taskmaster {

class ApiServer;

// Handles WebSocket event broadcasting
class EventService {
public:
  EventService() = default;
  ~EventService() = default;

  EventService(const EventService&) = delete;
  auto operator=(const EventService&) -> EventService& = delete;

  auto set_api_server(ApiServer* api) -> void;

  auto emit_task_status(std::string_view run_id, std::string_view task,
                        std::string_view status) -> void;
  auto emit_run_status(std::string_view run_id, std::string_view status)
      -> void;
  auto emit_log(std::string_view run_id, std::string_view task,
                std::string_view stream, std::string msg) -> void;

private:
  ApiServer* api_{nullptr};
};

}  // namespace taskmaster
