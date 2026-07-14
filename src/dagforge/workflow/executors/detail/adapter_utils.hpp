#pragma once

#include "dagforge/workflow/executor_registry.hpp"

#include <ranges>
#include <span>
#include <string_view>
#include <utility>

namespace dagforge::workflow::executor_detail {

[[nodiscard]] inline auto input_exists(ExecutorCompileContext context,
                                       std::string_view input) -> bool {
  return std::ranges::any_of(
      context.inputs,
      [&](const InputBinding &binding) { return binding.input == input; });
}

[[nodiscard]] inline auto output_requested(
    std::span<const WorkflowPortId> outputs, std::string_view output) -> bool {
  return std::ranges::any_of(outputs, [&](const WorkflowPortId &candidate) {
    return candidate == output;
  });
}

inline auto add_output(ExecutorOutputs &outputs,
                       std::span<const WorkflowPortId> requested,
                       std::string_view preferred, WorkflowValue value)
    -> void {
  if (output_requested(requested, preferred)) {
    outputs.emplace_back(WorkflowPortId{preferred}, std::move(value));
    return;
  }
  if (preferred == "result" && requested.size() == 1 && outputs.empty()) {
    outputs.emplace_back(requested.front().clone(), std::move(value));
  }
}

} // namespace dagforge::workflow::executor_detail
