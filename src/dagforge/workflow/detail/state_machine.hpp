#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <chrono>
#include <cstddef>
#include <span>
#include <vector>

namespace dagforge::workflow::detail {

struct RestartPreparation {
  std::vector<std::size_t> finalized_attempts;
  std::vector<std::size_t> failed_tasks;
};

[[nodiscard]] auto can_transition(RunState from, RunState to) noexcept -> bool;
[[nodiscard]] auto can_transition(TaskState from, TaskState to) noexcept -> bool;
[[nodiscard]] auto can_transition(AttemptState from, AttemptState to) noexcept
    -> bool;

auto transition(RunSnapshot &run, RunState state,
                std::chrono::system_clock::time_point now) -> Result<void>;
auto transition(TaskSnapshot &task, TaskState state,
                std::chrono::system_clock::time_point now) -> Result<void>;
auto transition(AttemptSnapshot &attempt, AttemptState state,
                std::chrono::system_clock::time_point now) -> Result<void>;

// A terminal state is not authoritative until its persistence commit
// succeeds. This operation reclassifies a staged terminal outcome when that
// commit fails; ordinary runtime transitions must continue to use transition().
auto apply_persistence_failure(RunSnapshot &run, ExecutionFailure failure)
    -> void;

[[nodiscard]] auto attempt_snapshot_is_valid(
    const AttemptSnapshot &attempt) noexcept -> bool;
[[nodiscard]] auto task_snapshot_is_valid(const TaskSnapshot &task) noexcept
    -> bool;
[[nodiscard]] auto run_snapshot_is_valid(const RunSnapshot &run) noexcept
    -> bool;
[[nodiscard]] auto runtime_projection_is_valid(
    const RunSnapshot &published, std::span<const TaskSnapshot> tasks,
    std::size_t active_attempts) noexcept -> bool;

[[nodiscard]] auto rehydrate_for_restart(
    RunSnapshot &snapshot, std::chrono::system_clock::time_point now)
    -> RestartPreparation;

} // namespace dagforge::workflow::detail
