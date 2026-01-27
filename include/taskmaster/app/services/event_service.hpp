#pragma once

#include <string>
#include <string_view>
#include "taskmaster/util/id.hpp"

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

  auto emit_task_status(const DAGRunId& dag_run_id, const TaskId& task,
                        std::string_view status) -> void;
  auto emit_run_status(const DAGRunId& dag_run_id, std::string_view status)
      -> void;
  auto emit_log(const DAGRunId& dag_run_id, const TaskId& task,
                std::string_view stream, std::string msg) -> void;

private:
  ApiServer* api_{nullptr};
};

}  // namespace taskmaster
