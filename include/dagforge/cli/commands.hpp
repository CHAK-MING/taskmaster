#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/cli/command_types.hpp"
#endif

namespace dagforge::cli {

[[nodiscard]] auto cmd_serve_start(const ServeStartOptions &opts) -> int;
[[nodiscard]] auto cmd_serve_stop(const ServeStopOptions &opts) -> int;
[[nodiscard]] auto cmd_serve_status(const ServeStatusOptions &opts) -> int;
[[nodiscard]] auto cmd_trigger(const TriggerOptions &opts) -> int;
[[nodiscard]] auto cmd_list_dags(const ListDagsOptions &opts) -> int;
[[nodiscard]] auto cmd_list_runs(const ListRunsOptions &opts) -> int;
[[nodiscard]] auto cmd_list_tasks(const ListTasksOptions &opts) -> int;
[[nodiscard]] auto cmd_clear(const ClearOptions &opts) -> int;
[[nodiscard]] auto cmd_pause(const PauseOptions &opts) -> int;
[[nodiscard]] auto cmd_unpause(const UnpauseOptions &opts) -> int;
[[nodiscard]] auto cmd_inspect(const InspectOptions &opts) -> int;
[[nodiscard]] auto cmd_validate(const ValidateOptions &opts) -> int;
[[nodiscard]] auto cmd_db_init(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_db_migrate(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_db_prune_stale(const DbOptions &opts) -> int;
[[nodiscard]] auto cmd_logs(const LogsOptions &opts) -> int;
[[nodiscard]] auto cmd_test_task(const TestTaskOptions &opts) -> int;

} // namespace dagforge::cli
