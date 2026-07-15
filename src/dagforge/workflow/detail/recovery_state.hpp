#pragma once

#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <chrono>
#include <cstddef>
#include <vector>

namespace dagforge::workflow::detail {

struct RestartPreparation {
  std::vector<std::size_t> finalized_attempts;
  std::vector<std::size_t> failed_tasks;
};

auto prepare_restart_snapshot(
    RunSnapshot &snapshot, std::chrono::system_clock::time_point now)
    -> RestartPreparation;

} // namespace dagforge::workflow::detail
