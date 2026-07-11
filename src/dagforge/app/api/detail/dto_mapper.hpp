#pragma once

#include "dagforge/app/api/api_dto.hpp"
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/storage/database_service.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/util.hpp"

#include <chrono>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>

namespace dagforge::api_detail {

using json = JsonValue;

inline auto status_from_error(const std::error_code &ec) -> http::HttpStatus {
  if (ec.category() == io::io_error_category()) {
    switch (static_cast<io::IoError>(ec.value())) {
    case io::IoError::TimedOut:
    case io::IoError::Cancelled:
      return http::HttpStatus::ServiceUnavailable;
    case io::IoError::InvalidArgument:
      return http::HttpStatus::BadRequest;
    case io::IoError::EndOfFile:
      return http::HttpStatus::NotFound;
    case io::IoError::ConnectionReset:
    case io::IoError::BrokenPipe:
      return http::HttpStatus::ServiceUnavailable;
    default:
      break;
    }
  }

  if (ec.category() == error_category()) {
    switch (static_cast<Error>(ec.value())) {
    case Error::NotFound:
    case Error::FileNotFound:
      return http::HttpStatus::NotFound;
    case Error::InvalidArgument:
    case Error::ParseError:
    case Error::InvalidUrl:
      return http::HttpStatus::BadRequest;
    case Error::Timeout:
    case Error::Cancelled:
      return http::HttpStatus::ServiceUnavailable;
    case Error::AlreadyExists:
    case Error::HasActiveRuns:
    case Error::InvalidState:
      return http::HttpStatus::Conflict;
    case Error::ReadOnly:
      return http::HttpStatus::Forbidden;
    case Error::ResourceExhausted:
    case Error::QueueFull:
      return http::HttpStatus::ServiceUnavailable;
    case Error::SystemNotRunning:
    case Error::DatabaseError:
      return http::HttpStatus::InternalServerError;
    default:
      break;
    }
  }

  if (ec == std::make_error_code(std::errc::resource_unavailable_try_again)) {
    return http::HttpStatus::ServiceUnavailable;
  }
  return http::HttpStatus::InternalServerError;
}

inline auto text_response(std::string body, http::HttpStatus status,
                          std::string_view content_type) -> http::HttpResponse {
  http::HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", std::string(content_type));
  resp.body.assign(body.begin(), body.end());
  return resp;
}

inline auto error_response(int code, std::string_view message)
    -> http::HttpResponse {
  http::HttpResponse resp{
      .status = static_cast<http::HttpStatus>(code), .headers = {}, .body = {}};
  resp.set_header("Content-Type", "application/json");
  resp.set_body(dump_json(json{{"error", std::string(message)}}));
  return resp;
}

inline auto json_response(const json &j,
                          http::HttpStatus status = http::HttpStatus::Ok)
    -> http::HttpResponse {
  http::HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", "application/json");
  auto s = dump_json(j);
  resp.body.assign(s.begin(), s.end());
  return resp;
}

template <typename T>
inline auto json_response_glz(const T &value,
                              http::HttpStatus status = http::HttpStatus::Ok)
    -> http::HttpResponse {
  http::HttpResponse resp;
  resp.status = status;
  resp.set_header("Content-Type", "application/json");

  std::string buffer;
  if (auto ec = glz::write_json(value, buffer); !ec) {
    resp.body.assign(buffer.begin(), buffer.end());
  } else {
    log::error("JSON serialization failed for API response");
    resp.status = http::HttpStatus::InternalServerError;
    resp.body.clear();
    constexpr std::string_view fallback =
        R"({"error":"JSON serialization failed"})";
    resp.body.assign(fallback.begin(), fallback.end());
  }
  return resp;
}

inline auto to_result_response(const std::error_code &ec)
    -> Result<http::HttpResponse> {
  return error_response(static_cast<int>(status_from_error(ec)), ec.message());
}

inline auto parse_execution_date_arg(std::string_view value)
    -> std::optional<std::chrono::system_clock::time_point> {
  if (value.empty() || value == "now") {
    return std::chrono::system_clock::now();
  }

  std::chrono::system_clock::time_point tp;
  std::istringstream ss{std::string(value)};

  if (value.size() == 10) {
    std::chrono::year_month_day ymd;
    ss >> std::chrono::parse("%Y-%m-%d", ymd);
    if (ss.fail()) {
      return std::nullopt;
    }
    tp = std::chrono::sys_days{ymd};
  } else {
    ss >> std::chrono::parse("%Y-%m-%dT%H:%M:%SZ", tp);
    if (ss.fail()) {
      return std::nullopt;
    }
  }
  return tp;
}

inline auto to_dto(const DAGInfo &dag_info) -> api_dto::DagInfoDto {
  api_dto::DagInfoDto dto{
      .dag_id = dag_info.dag_id.str(),
      .name = dag_info.name,
      .description = dag_info.description,
      .cron = dag_info.cron,
      .max_concurrent_runs = dag_info.max_concurrent_runs,
      .is_paused = dag_info.is_paused,
      .tasks = {},
  };
  dto.tasks.reserve(dag_info.tasks.size());
  for (const auto &t : dag_info.tasks) {
    dto.tasks.emplace_back(t.task_id.str());
  }
  return dto;
}

inline auto to_run_history_entry(const DAGRun &run, const DAGId &dag_id)
    -> DatabaseService::RunHistoryEntry {
  return DatabaseService::RunHistoryEntry{
      .dag_run_id = run.id().clone(),
      .dag_id = dag_id.clone(),
      .dag_rowid = run.dag_rowid(),
      .run_rowid = run.run_rowid(),
      .dag_version = run.dag_version(),
      .state = run.state(),
      .trigger_type = run.trigger_type(),
      .scheduled_at = run.scheduled_at(),
      .started_at = run.started_at(),
      .finished_at = run.finished_at(),
      .execution_date = run.execution_date(),
  };
}

inline auto to_dto(const DatabaseService::RunHistoryEntry &entry,
                   DAGRunState resolved_state)
    -> api_dto::RunHistoryEntryDto {
  return api_dto::RunHistoryEntryDto{
      .dag_run_id = entry.dag_run_id.str(),
      .dag_id = entry.dag_id.str(),
      .state = enum_to_string(resolved_state),
      .trigger_type = enum_to_string(entry.trigger_type),
      .started_at = util::format_iso8601(entry.started_at),
      .finished_at = util::format_iso8601(entry.finished_at),
      .execution_date = util::format_iso8601(entry.execution_date),
  };
}

inline auto to_dto(const TaskConfig &task) -> api_dto::TaskDetailDto {
  api_dto::TaskDetailDto dto{
      .task_id = task.task_id.str(),
      .name = task.name,
      .command = task.command,
      .executor = std::string(to_string_view(task.executor)),
      .sensor_type = {},
      .sensor_target = {},
      .execution_timeout_sec = static_cast<int>(task.execution_timeout.count()),
      .retry_interval_sec = static_cast<int>(task.retry_interval.count()),
      .max_retries = task.max_retries,
      .trigger_rule = std::string(to_string_view(task.trigger_rule)),
      .is_branch = task.is_branch,
      .depends_on_past = task.depends_on_past,
      .xcom_push_count = task.xcom_push.size(),
      .xcom_pull_count = task.xcom_pull.size(),
      .dependencies = {},
  };
  if (const auto *sensor = task.executor_config.as<SensorExecutorConfig>();
      sensor != nullptr) {
    dto.sensor_type = std::string(to_string_view(sensor->type));
    dto.sensor_target = sensor->target;
  }
  dto.dependencies.reserve(task.dependencies.size());
  for (const auto &dep : task.dependencies) {
    dto.dependencies.emplace_back(api_dto::TaskDependencyDto{
        .task = dep.task_id.str(), .label = dep.label});
  }
  return dto;
}

inline auto xcom_entries_to_json_object(const std::vector<XComEntry> &xcoms)
    -> json {
  json xcom = json::object_t{};
  for (const auto &entry : xcoms) {
    if (auto parsed = parse_json(entry.value); parsed) {
      xcom[entry.key] = std::move(*parsed);
    } else {
      xcom[entry.key] = entry.value;
    }
  }
  return xcom;
}

} // namespace dagforge::api_detail
