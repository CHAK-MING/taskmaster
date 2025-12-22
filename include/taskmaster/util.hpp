#pragma once

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>

namespace taskmaster {

inline auto generate_uuid() -> std::string {
  thread_local std::random_device rd;
  thread_local std::mt19937_64 gen(rd());
  thread_local std::uniform_int_distribution<std::uint64_t> dis;

  std::uint64_t a = dis(gen);
  std::uint64_t b = dis(gen);

  char buf[37];
  std::snprintf(
      buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012llx",
      static_cast<std::uint32_t>(a >> 32), static_cast<std::uint16_t>(a >> 16),
      static_cast<std::uint16_t>(a), static_cast<std::uint16_t>(b >> 48),
      static_cast<unsigned long long>(b & 0xFFFFFFFFFFFFULL));
  return buf;
}

inline auto format_timestamp() -> std::string {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buf;
}

} // namespace taskmaster
