#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <optional>
#include <string>
#endif

namespace dagforge::cli {

struct ServeStartOptions {
  std::string config_file;
  std::optional<std::string> pid_file;
  std::optional<std::string> log_file;
  std::optional<std::string> log_level;
  bool no_api{false};
  bool daemon{false};
  std::optional<int> shards;
};

struct ServeStopOptions {
  std::string config_file;
  std::optional<std::string> pid_file;
  int timeout_sec{10};
  bool force{false};
};

struct ServeStatusOptions {
  std::string config_file;
  std::optional<std::string> pid_file;
  bool json{false};
};

struct TriggerOptions {
  std::string config_file;
  std::string dag_id;
  std::string execution_date;
  bool no_api{false};
  bool wait{false};
  bool json{false};
};

struct ListDagsOptions {
  std::string config_file;
  std::string output{"table"};
  bool json{false};
  bool include_stale{false};
  std::size_t limit{50};
};

struct ListRunsOptions {
  std::string config_file;
  std::string dag_id;
  std::string state;
  std::string output{"table"};
  bool json{false};
  std::size_t limit{50};
};

struct ListTasksOptions {
  std::string config_file;
  std::string dag_id;
  std::string output{"table"};
  bool json{false};
};

struct ClearOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  std::optional<std::string> task;
  bool failed_only{false};
  bool all_tasks{false};
  bool downstream{false};
  bool dry_run{false};
  bool json{false};
};

struct PauseOptions {
  std::string config_file;
  std::string dag_id;
  bool json{false};
};

struct UnpauseOptions {
  std::string config_file;
  std::string dag_id;
  bool json{false};
};

struct InspectOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  bool latest{false};
  bool details{false};
  bool json{false};
};

struct LogsOptions {
  std::string config_file;
  std::string dag_id;
  std::string run_id;
  std::string task_id;
  int attempt{1};
  bool latest{false};
  bool follow{false};
  bool short_time{false};
  bool json{false};
};

struct DbOptions {
  std::string config_file;
  bool dry_run{false};
};

struct ValidateOptions {
  std::string config_file;
  std::optional<std::string> file;
  bool json{false};
};

struct TestTaskOptions {
  std::string config_file;
  std::string dag_id;
  std::string task_id;
  bool json{false};
};

} // namespace dagforge::cli
