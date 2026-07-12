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

namespace task_defaults {
inline constexpr std::chrono::seconds kExecutionTimeout{3600};
inline constexpr std::chrono::seconds kRetryInterval{60};
inline constexpr int kMaxRetries{3};
} // namespace task_defaults

enum class TriggerRule : std::uint8_t {
  AllSuccess,
  AllFailed,
  AllDone,
  OneSuccess,
  OneFailed,
  NoneFailed,
  NoneSkipped,
  AllDoneMinOneSuccess,
  AllSkipped,
  OneDone,
  NoneFailedMinOneSuccess,
  Always,
};

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::TriggerRule> {
  static constexpr auto value = glz::enumerate(
      "all_success", dagforge::TriggerRule::AllSuccess, "all_failed",
      dagforge::TriggerRule::AllFailed, "all_done",
      dagforge::TriggerRule::AllDone, "one_success",
      dagforge::TriggerRule::OneSuccess, "one_failed",
      dagforge::TriggerRule::OneFailed, "none_failed",
      dagforge::TriggerRule::NoneFailed, "none_skipped",
      dagforge::TriggerRule::NoneSkipped, "all_done_min_one_success",
      dagforge::TriggerRule::AllDoneMinOneSuccess, "all_skipped",
      dagforge::TriggerRule::AllSkipped, "one_done",
      dagforge::TriggerRule::OneDone, "none_failed_min_one_success",
      dagforge::TriggerRule::NoneFailedMinOneSuccess, "always",
      dagforge::TriggerRule::Always);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(TriggerRule value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<TriggerRule>(std::string_view s) noexcept
    -> TriggerRule {
  return ::dagforge::util::parse_enum(s, TriggerRule::AllSuccess);
}

} // namespace dagforge
