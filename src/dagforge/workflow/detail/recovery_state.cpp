#include "recovery_state.hpp"

#include "dagforge/workflow/execution_failure.hpp"

namespace dagforge::workflow::detail {
namespace {

auto finalize_active_attempt(TaskSnapshot &task, AttemptState state,
                             FailureClass failure_class,
                             TerminationReason termination_reason,
                             const ExecutionFailure &failure,
                             std::chrono::system_clock::time_point now)
    -> bool {
  if (!task.active_attempt_id || task.attempts.empty()) {
    return false;
  }
  auto &attempt = task.attempts.back();
  if (attempt.attempt_id != *task.active_attempt_id ||
      is_terminal(attempt.state)) {
    return false;
  }
  attempt.state = state;
  attempt.failure_class = failure_class;
  attempt.termination_reason = termination_reason;
  attempt.failure = failure;
  attempt.finished_at = now;
  return true;
}

} // namespace

auto prepare_restart_snapshot(
    RunSnapshot &snapshot, std::chrono::system_clock::time_point now)
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
                  ? FailureClass::Cancelled
                  : FailureClass::Infrastructure,
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
                                  FailureClass::Infrastructure,
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
