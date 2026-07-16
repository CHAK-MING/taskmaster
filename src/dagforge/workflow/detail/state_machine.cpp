#include "state_machine.hpp"

#include "attempt_outcome.hpp"
#include "dagforge/workflow/execution_failure.hpp"

#include <algorithm>
#include <memory>
#include <ranges>
#include <unordered_set>

namespace dagforge::workflow::detail {
namespace {

[[nodiscard]] auto active_attempt(const TaskSnapshot &task) noexcept
    -> const AttemptSnapshot * {
  if (!task.active_attempt_id || task.attempts.empty()) {
    return nullptr;
  }
  const auto &candidate = task.attempts.back();
  return candidate.attempt_id == *task.active_attempt_id &&
                 !is_terminal(candidate.state)
             ? std::addressof(candidate)
             : nullptr;
}

auto finalize_active_attempt(TaskSnapshot &task, AttemptState state,
                             TerminationReason termination_reason,
                             const ExecutionFailure &failure,
                             std::chrono::system_clock::time_point now)
    -> bool {
  if (!task.active_attempt_id || task.attempts.empty()) {
    return false;
  }
  auto &attempt = task.attempts.back();
  if (attempt.attempt_id != *task.active_attempt_id ||
      is_terminal(attempt.state) || !can_transition(attempt.state, state)) {
    return false;
  }
  attempt.state = state;
  attempt.termination_reason = termination_reason;
  attempt.failure = failure;
  attempt.finished_at = now;
  return true;
}

} // namespace

auto can_transition(RunState from, RunState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case RunState::Running:
    return to == RunState::Pausing || to == RunState::Stopping ||
           to == RunState::Succeeded || to == RunState::Failed;
  case RunState::Pausing:
    return to == RunState::Paused || to == RunState::Stopping ||
           to == RunState::Succeeded || to == RunState::Failed;
  case RunState::Paused:
    return to == RunState::Running || to == RunState::Stopping;
  case RunState::Stopping:
    return to == RunState::Succeeded || to == RunState::Failed ||
           to == RunState::Cancelled;
  case RunState::Succeeded:
  case RunState::Failed:
  case RunState::Cancelled:
    return false;
  }
  return false;
}

auto can_transition(TaskState from, TaskState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case TaskState::Pending:
    return to == TaskState::Ready || to == TaskState::Skipped ||
           to == TaskState::Cancelled;
  case TaskState::Ready:
    return to == TaskState::Running || to == TaskState::Skipped ||
           to == TaskState::Cancelled;
  case TaskState::Running:
    return to == TaskState::RetryWaiting || to == TaskState::Succeeded ||
           to == TaskState::Failed || to == TaskState::Cancelled;
  case TaskState::RetryWaiting:
    return to == TaskState::Ready || to == TaskState::Cancelled;
  case TaskState::Succeeded:
  case TaskState::Failed:
  case TaskState::Skipped:
  case TaskState::Cancelled:
    return false;
  }
  return false;
}

auto can_transition(AttemptState from, AttemptState to) noexcept -> bool {
  if (from == to) {
    return true;
  }
  switch (from) {
  case AttemptState::Starting:
    return to == AttemptState::Running || to == AttemptState::Failed ||
           to == AttemptState::Terminating || to == AttemptState::TimedOut ||
           to == AttemptState::Cancelled;
  case AttemptState::Running:
    return to == AttemptState::Terminating ||
           to == AttemptState::Succeeded || to == AttemptState::Failed ||
           to == AttemptState::TimedOut || to == AttemptState::Cancelled;
  case AttemptState::Terminating:
    return to == AttemptState::TimedOut || to == AttemptState::Cancelled ||
           to == AttemptState::Failed;
  case AttemptState::Succeeded:
  case AttemptState::Failed:
  case AttemptState::TimedOut:
  case AttemptState::Cancelled:
    return false;
  }
  return false;
}

auto transition(RunSnapshot &run, RunState state,
                std::chrono::system_clock::time_point now) -> Result<void> {
  if (!can_transition(run.state, state)) {
    return fail(Error::InvalidState);
  }
  run.state = state;
  if (is_terminal(state)) {
    run.finished_at = now;
  }
  return ok();
}

auto transition(TaskSnapshot &task, TaskState state,
                std::chrono::system_clock::time_point now) -> Result<void> {
  if (!can_transition(task.state, state)) {
    return fail(Error::InvalidState);
  }
  task.state = state;
  if (state == TaskState::Running &&
      task.started_at == std::chrono::system_clock::time_point{}) {
    task.started_at = now;
  }
  if (state == TaskState::Ready) {
    task.next_attempt_at.reset();
  }
  if (is_terminal(state)) {
    task.active_attempt_id.reset();
    task.next_attempt_at.reset();
    task.finished_at = now;
  }
  return ok();
}

auto transition(AttemptSnapshot &attempt, AttemptState state,
                std::chrono::system_clock::time_point now) -> Result<void> {
  if (!can_transition(attempt.state, state)) {
    return fail(Error::InvalidState);
  }
  attempt.state = state;
  if (state == AttemptState::Running &&
      attempt.started_at == std::chrono::system_clock::time_point{}) {
    attempt.started_at = now;
  }
  if (is_terminal(state)) {
    attempt.finished_at = now;
  }
  return ok();
}

auto apply_persistence_failure(RunSnapshot &run, ExecutionFailure failure)
    -> void {
  run.stop_intent = StopIntent::Fail;
  run.stop_reason = failure.message;
  run.failure = std::move(failure);
  if (is_terminal(run.state)) {
    run.state = RunState::Failed;
  }
}

auto attempt_snapshot_is_valid(const AttemptSnapshot &attempt) noexcept
    -> bool {
  return !attempt.attempt_id.empty() && attempt.number > 0 &&
         attempt_outcome_is_valid(attempt);
}

auto task_snapshot_is_valid(const TaskSnapshot &task) noexcept -> bool {
  if (task.node_id.empty() || task.attempt_count != task.attempts.size()) {
    return false;
  }
  std::unordered_set<std::string> attempt_ids;
  for (std::size_t index = 0; index < task.attempts.size(); ++index) {
    const auto &attempt = task.attempts[index];
    if (attempt.number != index + 1 || !attempt_snapshot_is_valid(attempt) ||
        !attempt_ids.emplace(attempt.attempt_id.str()).second) {
      return false;
    }
  }

  const auto *active = active_attempt(task);
  if ((task.state == TaskState::Running) != (active != nullptr)) {
    return false;
  }
  if (task.active_attempt_id && active == nullptr) {
    return false;
  }
  if (task.state == TaskState::RetryWaiting) {
    if (task.attempts.empty() ||
        !is_terminal(task.attempts.back().state) || !task.next_attempt_at) {
      return false;
    }
  } else if (task.next_attempt_at) {
    return false;
  }
  if (is_terminal(task.state) && task.active_attempt_id) {
    return false;
  }
  if (task.reused_from_run_id &&
      (task.state != TaskState::Succeeded || !task.attempts.empty() ||
       task.attempt_count != 0)) {
    return false;
  }
  if (task.state == TaskState::Failed && !task.failure) {
    return false;
  }
  if (task.state == TaskState::Succeeded && task.failure) {
    return false;
  }
  return true;
}

auto run_snapshot_is_valid(const RunSnapshot &run) noexcept -> bool {
  if (run.run_id.empty() || run.workflow_id.empty() || run.plan_id.empty() ||
      run.parent_run_id.has_value() != run.parent_plan_id.has_value() ||
      (run.parent_run_id ? run.repair_revision == 0
                         : run.repair_revision != 0)) {
    return false;
  }
  std::unordered_set<std::string> node_ids;
  for (const auto &task : run.tasks) {
    if (!task_snapshot_is_valid(task) ||
        !node_ids.emplace(task.node_id.str()).second) {
      return false;
    }
  }
  if (is_terminal(run.state) &&
      !std::ranges::all_of(run.tasks, [](const auto &task) {
        return is_terminal(task.state);
      })) {
    return false;
  }
  if (run.state == RunState::Failed && !run.failure) {
    return false;
  }
  if (run.state == RunState::Succeeded && run.failure) {
    return false;
  }
  return true;
}

auto runtime_projection_is_valid(const RunSnapshot &published,
                                 std::span<const TaskSnapshot> tasks,
                                 std::size_t active_attempts) noexcept -> bool {
  if (published.tasks.size() != tasks.size()) {
    return false;
  }
  std::size_t observed_active = 0;
  for (std::size_t index = 0; index < tasks.size(); ++index) {
    const auto &task = tasks[index];
    const auto &copy = published.tasks[index];
    if (copy.node_id != task.node_id || copy.state != task.state ||
        copy.attempt_count != task.attempt_count ||
        copy.active_attempt_id != task.active_attempt_id ||
        !task_snapshot_is_valid(task)) {
      return false;
    }
    observed_active += task.active_attempt_id.has_value() ? 1U : 0U;
  }
  return observed_active == active_attempts && run_snapshot_is_valid(published);
}

auto rehydrate_for_restart(RunSnapshot &snapshot,
                           std::chrono::system_clock::time_point now)
    -> RestartPreparation {
  RestartPreparation preparation;
  const auto restart_failure = make_execution_failure(
      Error::SystemNotRunning, "runtime_restarted",
      "The previous executor process was not attachable after restart");
  const bool restoring_stop = snapshot.state == RunState::Stopping;
  if (snapshot.state == RunState::Pausing) {
    snapshot.state = RunState::Paused;
  }

  for (std::size_t index = 0; index < snapshot.tasks.size(); ++index) {
    auto &task = snapshot.tasks[index];
    const bool interrupted = task.state == TaskState::Running ||
                             task.active_attempt_id.has_value();
    if (restoring_stop && !is_terminal(task.state)) {
      const auto stop_failure = snapshot.failure.value_or(
          make_execution_failure(
              snapshot.stop_intent == StopIntent::Cancel ? Error::Cancelled
                                                         : Error::SystemNotRunning,
              snapshot.stop_intent == StopIntent::Cancel ? "run_cancelled"
                                                         : "runtime_restarted",
              snapshot.stop_reason.empty()
                  ? "Workflow stopped during restart recovery"
                  : snapshot.stop_reason));
      if (finalize_active_attempt(
              task,
              snapshot.stop_intent == StopIntent::Cancel
                  ? AttemptState::Cancelled
                  : AttemptState::Failed,
              snapshot.stop_intent == StopIntent::Cancel
                  ? TerminationReason::RunCancelled
                  : TerminationReason::RunFailed,
              stop_failure, now)) {
        preparation.finalized_attempts.push_back(index);
      }
      task.active_attempt_id.reset();
      task.next_attempt_at.reset();
      task.failure = stop_failure;
      task.state = snapshot.stop_intent == StopIntent::Cancel
                       ? TaskState::Cancelled
                       : TaskState::Failed;
      task.finished_at = now;
      if (task.state == TaskState::Failed) {
        preparation.failed_tasks.push_back(index);
      }
      continue;
    }
    if (interrupted) {
      if (finalize_active_attempt(task, AttemptState::Failed,
                                  TerminationReason::RunFailed,
                                  restart_failure, now)) {
        preparation.finalized_attempts.push_back(index);
      }
      task.active_attempt_id.reset();
      task.next_attempt_at.reset();
      task.failure = restart_failure;
      task.state = TaskState::Ready;
      task.finished_at = {};
      continue;
    }
    if (task.state == TaskState::RetryWaiting &&
        (!task.next_attempt_at || *task.next_attempt_at <= now)) {
      task.state = TaskState::Ready;
      task.next_attempt_at.reset();
    }
  }
  return preparation;
}

} // namespace dagforge::workflow::detail
