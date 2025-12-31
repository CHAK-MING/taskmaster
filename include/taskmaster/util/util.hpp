#pragma once

#include <chrono>
#include <cstdint>
#include <format>
#include <random>
#include <string>

namespace taskmaster {

inline auto generate_uuid() -> std::string {
  thread_local std::random_device rd;
  thread_local std::mt19937_64 gen(rd());
  thread_local std::uniform_int_distribution<std::uint64_t> dis;

  std::uint64_t a = dis(gen);
  std::uint64_t b = dis(gen);

  return std::format(
      "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
      static_cast<std::uint32_t>(a >> 32), static_cast<std::uint16_t>(a >> 16),
      static_cast<std::uint16_t>(a), static_cast<std::uint16_t>(b >> 48),
      b & 0xFFFFFFFFFFFFULL);
}

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
