#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <chrono>
#include <cstdint>
#include <string_view>
#endif

namespace dagforge {

enum class TaskState : std::uint8_t {
  Pending,
  Running,
  Success,
  Failed,
  UpstreamFailed,
  Retrying,
  Skipped,
  Ready,
};

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::TaskState> {
  static constexpr auto value = glz::enumerate(
      "pending", dagforge::TaskState::Pending, "running",
      dagforge::TaskState::Running, "success", dagforge::TaskState::Success,
      "failed", dagforge::TaskState::Failed, "upstream_failed",
      dagforge::TaskState::UpstreamFailed, "retrying",
      dagforge::TaskState::Retrying, "skipped", dagforge::TaskState::Skipped,
      "ready", dagforge::TaskState::Ready);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(TaskState value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<TaskState>(std::string_view s) noexcept
    -> TaskState {
  return ::dagforge::util::parse_enum(s, TaskState::Pending);
}

[[nodiscard]] constexpr bool is_terminal(TaskState s) noexcept {
  return s == TaskState::Success || s == TaskState::Failed ||
         s == TaskState::UpstreamFailed || s == TaskState::Skipped;
}

struct RetryPolicy {
  int max_retries{3};
  std::chrono::seconds retry_interval{std::chrono::seconds(0)};
};

} // namespace dagforge
