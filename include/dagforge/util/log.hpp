#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/time.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <format>
#include <functional>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <thread>
#endif

namespace dagforge::log {

enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error };

inline constexpr std::array<std::string_view, 5> level_names = {
    "trace", "debug", "info", "warn", "error"};

inline constexpr std::array<std::string_view, 5> level_colors = {
    "\o{33}[90m", // trace: gray
    "\o{33}[36m", // debug: cyan
    "\o{33}[32m", // info: green
    "\o{33}[33m", // warn: yellow
    "\o{33}[31m"  // error: red
};

[[nodiscard]] inline auto level_name(Level level) -> std::string_view {
  return level_names.at(std::to_underlying(level));
}

[[nodiscard]] inline auto level_color(Level level) -> std::string_view {
  return level_colors.at(std::to_underlying(level));
}

class Logger {
  struct Impl;
  std::unique_ptr<Impl> impl_;

  auto enqueue(std::string message) -> void;
  [[nodiscard]] auto should_log(Level level) const noexcept -> bool;

public:
  Logger();
  ~Logger();

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) noexcept;
  auto operator=(Logger &&) noexcept -> Logger &;

  auto start() -> void;
  auto stop() -> void;
  auto set_level(Level level) noexcept -> void;
  auto set_output_stderr() noexcept -> void;
  auto set_output_file(std::string_view path) -> bool;
  [[nodiscard]] auto level() const noexcept -> Level;

  template <typename... Args>
  auto log(Level level, std::format_string<Args...> fmt, Args &&...args)
      -> void {
    if (!should_log(level))
      return;

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::floor<std::chrono::milliseconds>(now);
    auto tid =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;
    enqueue(std::format("[{}] [{}{}{}] [{}] {}\n",
                        util::format_local_timestamp(time), level_color(level),
                        level_name(level), "\o{33}[0m", tid,
                        std::format(fmt, std::forward<Args>(args)...)));
  }
};

Logger &logger();

inline auto set_level(Level level) noexcept -> void {
  logger().set_level(level);
}

inline auto set_output_file(std::string_view path) -> bool {
  return logger().set_output_file(path);
}

inline auto set_output_stderr() noexcept -> void {
  logger().set_output_stderr();
}

inline auto set_level(std::string_view name) noexcept -> void {
  const auto *it = std::ranges::find(level_names, name);
  auto level = (it != level_names.end())
                   ? static_cast<Level>(std::distance(level_names.begin(), it))
                   : Level::Info;
  logger().set_level(level);
}

inline auto start() -> void { logger().start(); }
template <typename T> inline auto start(T &) -> void { logger().start(); }
inline auto stop() -> void { logger().stop(); }

template <typename... Args>
auto trace(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Trace, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto debug(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Debug, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto info(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Info, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto warn(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Warn, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto error(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Error, fmt, std::forward<Args>(args)...);
}

} // namespace dagforge::log
