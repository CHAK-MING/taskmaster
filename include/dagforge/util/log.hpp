#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/util/time.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <iterator>
#include <memory>
#include <source_location>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

namespace dagforge::log {

enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error };
enum class ColorPolicy : std::uint8_t { Auto, Always, Never };
enum class OverflowPolicy : std::uint8_t { DropNewest, Block };

inline constexpr std::array<std::string_view, 5> level_names = {
    "trace", "debug", "info", "warn", "error"};

inline constexpr std::array<std::string_view, 5> level_colors = {
    "\o{33}[90m", // trace: gray
    "\o{33}[36m", // debug: cyan
    "\o{33}[32m", // info: green
    "\o{33}[33m", // warn: yellow
    "\o{33}[31m"  // error: red
};

[[nodiscard]] inline auto level_name(Level level) noexcept -> std::string_view {
  const auto index = static_cast<std::size_t>(std::to_underlying(level));
  return index < level_names.size() ? level_names[index] : "unknown";
}

[[nodiscard]] inline auto level_color(Level level) noexcept
    -> std::string_view {
  const auto index = static_cast<std::size_t>(std::to_underlying(level));
  return index < level_colors.size() ? level_colors[index] : std::string_view{};
}

struct LoggerOptions {
  std::size_t queue_capacity{8192};
  std::size_t batch_size{64};
  ColorPolicy color_policy{ColorPolicy::Auto};
  OverflowPolicy overflow_policy{OverflowPolicy::DropNewest};
};

struct Record {
  std::chrono::system_clock::time_point timestamp;
  Level level{Level::Info};
  std::uint64_t thread_id{};
  std::string message;
  std::source_location origin;
};

class Sink {
public:
  virtual ~Sink() = default;

  [[nodiscard]] virtual auto supports_color() const noexcept -> bool = 0;
  [[nodiscard]] virtual auto write(const Record &record,
                                   std::string_view rendered)
      -> Result<void> = 0;
  [[nodiscard]] virtual auto flush() -> Result<void> = 0;
};

template <typename... Args> struct FormatWithLocation {
  std::format_string<Args...> format;
  std::source_location origin;

  template <typename T>
  consteval FormatWithLocation(
      const T &value,
      std::source_location location = std::source_location::current())
      : format(value), origin(location) {}
};

class Logger {
  struct Impl;
  std::unique_ptr<Impl> impl_;

  auto enqueue(Record record) -> void;
  [[nodiscard]] auto should_log(Level level) const noexcept -> bool;

public:
  explicit Logger(LoggerOptions options = {});
  ~Logger();

  Logger(const Logger &) = delete;
  auto operator=(const Logger &) -> Logger & = delete;
  Logger(Logger &&) = delete;
  auto operator=(Logger &&) -> Logger & = delete;

  auto start() -> void;
  auto stop() -> void;
  auto set_level(Level level) noexcept -> void;
  auto set_color_policy(ColorPolicy policy) noexcept -> void;
  auto set_overflow_policy(OverflowPolicy policy) noexcept -> void;
  auto set_output_stderr() -> void;
  [[nodiscard]] auto set_sink(std::shared_ptr<Sink> sink) -> Result<void>;
  [[nodiscard]] auto set_output_file(std::string_view path) -> Result<void>;
  [[nodiscard]] auto flush() -> Result<void>;
  [[nodiscard]] auto level() const noexcept -> Level;
  [[nodiscard]] auto color_policy() const noexcept -> ColorPolicy;
  [[nodiscard]] auto overflow_policy() const noexcept -> OverflowPolicy;
  [[nodiscard]] auto dropped_messages() const noexcept -> std::uint64_t;

  template <typename... Args>
  auto log(Level level,
           FormatWithLocation<std::type_identity_t<Args>...> format,
           Args &&...args) -> void {
    if (!should_log(level)) {
      return;
    }

    const auto now = std::chrono::system_clock::now();
    const auto thread_id =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1'000'000;
    enqueue(Record{
        .timestamp = std::chrono::floor<std::chrono::milliseconds>(now),
        .level = level,
        .thread_id = thread_id,
        .message = std::format(format.format, std::forward<Args>(args)...),
        .origin = format.origin,
    });
  }
};

[[nodiscard]] auto logger() -> Logger &;

inline auto set_level(Level level) noexcept -> void {
  logger().set_level(level);
}

inline auto set_color_policy(ColorPolicy policy) noexcept -> void {
  logger().set_color_policy(policy);
}

inline auto set_overflow_policy(OverflowPolicy policy) noexcept -> void {
  logger().set_overflow_policy(policy);
}

[[nodiscard]] inline auto set_output_file(std::string_view path)
    -> Result<void> {
  return logger().set_output_file(path);
}

inline auto set_output_stderr() -> void { logger().set_output_stderr(); }

[[nodiscard]] inline auto set_sink(std::shared_ptr<Sink> sink) -> Result<void> {
  return logger().set_sink(std::move(sink));
}

inline auto set_level(std::string_view name) noexcept -> void {
  const auto *it = std::ranges::find(level_names, name);
  const auto level =
      it != level_names.end()
          ? static_cast<Level>(std::distance(level_names.begin(), it))
          : Level::Info;
  logger().set_level(level);
}

inline auto start() -> void { logger().start(); }
inline auto stop() -> void { logger().stop(); }
[[nodiscard]] inline auto flush() -> Result<void> { return logger().flush(); }
[[nodiscard]] inline auto dropped_messages() noexcept -> std::uint64_t {
  return logger().dropped_messages();
}

template <typename... Args>
auto trace(FormatWithLocation<std::type_identity_t<Args>...> format,
           Args &&...args) -> void {
  logger().log(Level::Trace, format, std::forward<Args>(args)...);
}

template <typename... Args>
auto debug(FormatWithLocation<std::type_identity_t<Args>...> format,
           Args &&...args) -> void {
  logger().log(Level::Debug, format, std::forward<Args>(args)...);
}

template <typename... Args>
auto info(FormatWithLocation<std::type_identity_t<Args>...> format,
          Args &&...args) -> void {
  logger().log(Level::Info, format, std::forward<Args>(args)...);
}

template <typename... Args>
auto warn(FormatWithLocation<std::type_identity_t<Args>...> format,
          Args &&...args) -> void {
  logger().log(Level::Warn, format, std::forward<Args>(args)...);
}

template <typename... Args>
auto error(FormatWithLocation<std::type_identity_t<Args>...> format,
           Args &&...args) -> void {
  logger().log(Level::Error, format, std::forward<Args>(args)...);
}

} // namespace dagforge::log
