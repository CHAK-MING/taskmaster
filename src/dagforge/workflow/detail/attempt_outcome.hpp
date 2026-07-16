#pragma once

#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <optional>

namespace dagforge::workflow::detail {

[[nodiscard]] constexpr auto attempt_outcome_is_valid(
    AttemptState state, std::optional<TerminationReason> termination_reason,
    std::optional<Error> failure_kind) noexcept -> bool {
  switch (state) {
  case AttemptState::Starting:
  case AttemptState::Running:
    return !termination_reason && !failure_kind;
  case AttemptState::Terminating:
    return termination_reason.has_value() && !failure_kind;
  case AttemptState::Succeeded:
    return !termination_reason && !failure_kind;
  case AttemptState::TimedOut:
    return !termination_reason && failure_kind == Error::Timeout;
  case AttemptState::Cancelled:
    return failure_kind == Error::Cancelled;
  case AttemptState::Failed:
    return failure_kind && failure_kind != Error::Cancelled &&
           failure_kind != Error::Timeout;
  }
  return false;
}

[[nodiscard]] inline auto attempt_outcome_is_valid(
    const AttemptSnapshot &attempt) noexcept -> bool {
  return attempt_outcome_is_valid(
      attempt.state, attempt.termination_reason,
      attempt.failure ? std::optional{attempt.failure->kind} : std::nullopt);
}

} // namespace dagforge::workflow::detail
