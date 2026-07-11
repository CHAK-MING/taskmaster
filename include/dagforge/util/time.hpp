#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <array>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <string>
#include <string_view>
#include <utility>
#endif

namespace dagforge::util {

namespace detail {

[[nodiscard]] inline auto format_tm(const std::tm &tm,
                                    const char *pattern) -> std::string {
  std::array<char, 64> buffer{};
  auto written = std::strftime(buffer.data(), buffer.size(), pattern, &tm);
  return written == 0 ? std::string{} : std::string(buffer.data(), written);
}

template <typename FormatFn>
[[nodiscard]] inline auto format_time_or(
    std::chrono::system_clock::time_point tp, std::string_view empty_value,
    FormatFn &&format_fn) -> std::string {
  if (tp == std::chrono::system_clock::time_point{}) {
    return std::string{empty_value};
  }
  return std::forward<FormatFn>(format_fn)(tp);
}

} // namespace detail

[[nodiscard]] inline auto
format_iso8601(std::chrono::system_clock::time_point tp) -> std::string {
  return detail::format_time_or(
      tp, "", [](std::chrono::system_clock::time_point value) {
        auto t = std::chrono::system_clock::to_time_t(value);
        std::tm tm{};
        gmtime_r(&t, &tm);
        return detail::format_tm(tm, "%Y-%m-%dT%H:%M:%SZ");
      });
}

[[nodiscard]] inline auto format_timestamp() -> std::string {
  return format_iso8601(std::chrono::system_clock::now());
}

[[nodiscard]] inline auto
format_local_timestamp(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        auto t = std::chrono::system_clock::to_time_t(value);
        std::tm tm{};
        localtime_r(&t, &tm);
        return detail::format_tm(tm, "%Y-%m-%d %H:%M:%S");
      });
}

[[nodiscard]] inline auto
format_local_timestamp_short(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        auto t = std::chrono::system_clock::to_time_t(value);
        std::tm tm{};
        localtime_r(&t, &tm);
        return detail::format_tm(tm, "%Y-%m-%d %H:%M");
      });
}

[[nodiscard]] inline auto format_iso8601(std::int64_t millis) -> std::string {
  if (millis <= 0) {
    return {};
  }
  return format_iso8601(
      std::chrono::system_clock::time_point{std::chrono::milliseconds{millis}});
}

[[nodiscard]] inline auto to_utc(std::chrono::system_clock::time_point tp)
    -> std::tm {
  auto t = std::chrono::system_clock::to_time_t(tp);
  std::tm tm{};
  gmtime_r(&t, &tm);
  return tm;
}

[[nodiscard]] inline auto
to_unix_millis(std::chrono::system_clock::time_point tp) -> std::int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

} // namespace dagforge::util
