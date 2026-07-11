#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <glaze/json.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <vector>
#endif

namespace dagforge::api_dto {

struct DagInfoDto {
  std::string dag_id;
  std::string name;
  std::string description;
  std::string cron;
  int max_concurrent_runs{1};
  bool is_paused{false};
  std::vector<std::string> tasks;
};

struct DagsResponseDto {
  std::vector<DagInfoDto> dags;
};

struct RunHistoryEntryDto {
  std::string dag_run_id;
  std::string dag_id;
  std::string state;
  std::string trigger_type;
  std::string started_at;
  std::string finished_at;
  std::string execution_date;
};

struct HistoryResponseDto {
  std::vector<RunHistoryEntryDto> runs;
};

struct TaskIdsResponseDto {
  std::vector<std::string> tasks;
};

struct TaskDependencyDto {
  std::string task;
  std::string label;
};

struct TaskDetailDto {
  std::string task_id;
  std::string name;
  std::string command;
  std::string executor;
  std::string sensor_type;
  std::string sensor_target;
  int execution_timeout_sec{0};
  int retry_interval_sec{0};
  int max_retries{0};
  std::string trigger_rule;
  bool is_branch{false};
  bool depends_on_past{false};
  std::size_t xcom_push_count{0};
  std::size_t xcom_pull_count{0};
  std::vector<TaskDependencyDto> dependencies;
};

struct DagRunSummaryDto {
  std::string dag_run_id;
  std::string state;
  std::string started_at;
  std::string finished_at;
};

struct DagRunHistoryResponseDto {
  std::vector<DagRunSummaryDto> runs;
};

struct TaskInstanceDto {
  std::string task_id;
  std::string state;
  int attempt{0};
  int exit_code{0};
  std::int64_t duration_ms{0};
  std::string started_at;
  std::string finished_at;
  std::string error;
};

struct RunTasksResponseDto {
  std::string dag_run_id;
  std::vector<TaskInstanceDto> tasks;
};

struct TaskLogEntryDto {
  std::string task_id;
  int attempt{1};
  std::string stream;
  std::string logged_at;
  std::string content;
};

struct RunLogsResponseDto {
  std::string dag_run_id;
  std::vector<TaskLogEntryDto> logs;
};

struct TaskLogsResponseDto {
  std::string dag_run_id;
  std::string task_id;
  std::vector<TaskLogEntryDto> logs;
};

struct TriggerRequestDto {
  std::optional<std::string_view> execution_date;
};

} // namespace dagforge::api_dto

namespace glz {

template <> struct meta<dagforge::api_dto::DagInfoDto> {
  using T = dagforge::api_dto::DagInfoDto;
  static constexpr auto value = object(
      "dag_id", &T::dag_id, "name", &T::name, "description", &T::description,
      "cron", &T::cron, "max_concurrent_runs", &T::max_concurrent_runs,
      "is_paused", &T::is_paused, "tasks", &T::tasks);
};

template <> struct meta<dagforge::api_dto::DagsResponseDto> {
  using T = dagforge::api_dto::DagsResponseDto;
  static constexpr auto value = object("dags", &T::dags);
};

template <> struct meta<dagforge::api_dto::RunHistoryEntryDto> {
  using T = dagforge::api_dto::RunHistoryEntryDto;
  static constexpr auto value = object(
      "dag_run_id", &T::dag_run_id, "dag_id", &T::dag_id, "state", &T::state,
      "trigger_type", &T::trigger_type, "started_at", &T::started_at,
      "finished_at", &T::finished_at, "execution_date", &T::execution_date);
};

template <> struct meta<dagforge::api_dto::HistoryResponseDto> {
  using T = dagforge::api_dto::HistoryResponseDto;
  static constexpr auto value = object("runs", &T::runs);
};

template <> struct meta<dagforge::api_dto::TaskIdsResponseDto> {
  using T = dagforge::api_dto::TaskIdsResponseDto;
  static constexpr auto value = object("tasks", &T::tasks);
};

template <> struct meta<dagforge::api_dto::TaskDependencyDto> {
  using T = dagforge::api_dto::TaskDependencyDto;
  static constexpr auto value = object("task", &T::task, "label", &T::label);
};

template <> struct meta<dagforge::api_dto::TaskDetailDto> {
  using T = dagforge::api_dto::TaskDetailDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "name", &T::name, "command", &T::command,
             "executor", &T::executor, "sensor_type", &T::sensor_type,
             "sensor_target", &T::sensor_target, "execution_timeout_sec",
             &T::execution_timeout_sec, "retry_interval_sec",
             &T::retry_interval_sec, "max_retries", &T::max_retries,
             "trigger_rule", &T::trigger_rule, "is_branch", &T::is_branch,
             "depends_on_past", &T::depends_on_past, "xcom_push_count",
             &T::xcom_push_count, "xcom_pull_count", &T::xcom_pull_count,
             "dependencies", &T::dependencies);
};

template <> struct meta<dagforge::api_dto::DagRunSummaryDto> {
  using T = dagforge::api_dto::DagRunSummaryDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "state", &T::state, "started_at",
             &T::started_at, "finished_at", &T::finished_at);
};

template <> struct meta<dagforge::api_dto::DagRunHistoryResponseDto> {
  using T = dagforge::api_dto::DagRunHistoryResponseDto;
  static constexpr auto value = object("runs", &T::runs);
};

template <> struct meta<dagforge::api_dto::TaskLogEntryDto> {
  using T = dagforge::api_dto::TaskLogEntryDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "attempt", &T::attempt, "stream",
             &T::stream, "logged_at", &T::logged_at, "content", &T::content);
};

template <> struct meta<dagforge::api_dto::RunLogsResponseDto> {
  using T = dagforge::api_dto::RunLogsResponseDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "logs", &T::logs);
};

template <> struct meta<dagforge::api_dto::TaskLogsResponseDto> {
  using T = dagforge::api_dto::TaskLogsResponseDto;
  static constexpr auto value = object("dag_run_id", &T::dag_run_id, "task_id",
                                       &T::task_id, "logs", &T::logs);
};

template <> struct meta<dagforge::api_dto::TriggerRequestDto> {
  using T = dagforge::api_dto::TriggerRequestDto;
  static constexpr auto value = object("execution_date", &T::execution_date);
};

template <> struct meta<dagforge::api_dto::TaskInstanceDto> {
  using T = dagforge::api_dto::TaskInstanceDto;
  static constexpr auto value =
      object("task_id", &T::task_id, "state", &T::state, "attempt", &T::attempt,
             "exit_code", &T::exit_code, "duration_ms", &T::duration_ms,
             "started_at", &T::started_at, "finished_at", &T::finished_at,
             "error", &T::error);
};

template <> struct meta<dagforge::api_dto::RunTasksResponseDto> {
  using T = dagforge::api_dto::RunTasksResponseDto;
  static constexpr auto value =
      object("dag_run_id", &T::dag_run_id, "tasks", &T::tasks);
};

} // namespace glz
