#pragma once

#include <chrono>
#include <format>
#include <string>

namespace taskmaster {

inline auto format_timestamp() -> std::string {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  return std::format("{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}Z",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                     tm.tm_min, tm.tm_sec);
}

}  // namespace taskmaster
