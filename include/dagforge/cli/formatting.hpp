#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/time.hpp"
#endif

#include <chrono>
#include <cstdio>
#include <format>
#include <print>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>


namespace dagforge::cli::fmt {

namespace ansi {

inline auto is_tty() noexcept -> bool {
  static const bool tty = ::isatty(::fileno(stdout));
  return tty;
}

inline constexpr std::string_view kReset = "\033[0m";
inline constexpr std::string_view kBold = "\033[1m";
inline constexpr std::string_view kDim = "\033[2m";

inline constexpr std::string_view kGreen = "\033[32m";
inline constexpr std::string_view kRed = "\033[31m";
inline constexpr std::string_view kYellow = "\033[33m";
inline constexpr std::string_view kBlue = "\033[34m";
inline constexpr std::string_view kCyan = "\033[36m";
inline constexpr std::string_view kWhite = "\033[37m";

inline auto colorize(std::string_view text, std::string_view color)
    -> std::string {
  if (!is_tty()) {
    return std::string(text);
  }
  return std::format("{}{}{}", color, text, kReset);
}

inline auto bold(std::string_view text) -> std::string {
  return colorize(text, kBold);
}

inline auto green(std::string_view text) -> std::string {
  return colorize(text, kGreen);
}

inline auto red(std::string_view text) -> std::string {
  return colorize(text, kRed);
}

inline auto yellow(std::string_view text) -> std::string {
  return colorize(text, kYellow);
}

inline auto blue(std::string_view text) -> std::string {
  return colorize(text, kBlue);
}

inline auto cyan(std::string_view text) -> std::string {
  return colorize(text, kCyan);
}

inline auto dim(std::string_view text) -> std::string {
  return colorize(text, kDim);
}

inline auto ansi_visible_width(std::string_view s) -> std::size_t {
  std::size_t width = 0;
  bool in_escape = false;
  for (char c : s) {
    if (in_escape) {
      if (c == 'm')
        in_escape = false;
    } else if (c == '\033') {
      in_escape = true;
    } else {
      ++width;
    }
  }
  return width;
}

} // namespace ansi

inline auto colorize_dag_run_state(std::string_view state) -> std::string {
  if (state == "success")
    return ansi::green(state);
  if (state == "failed")
    return ansi::red(state);
  if (state == "running")
    return ansi::yellow(state);
  return std::string(state);
}

inline auto colorize_task_state(std::string_view state) -> std::string {
  if (state == "success")
    return ansi::green(state);
  if (state == "failed")
    return ansi::red(state);
  if (state == "running")
    return ansi::blue(state);
  if (state == "pending")
    return ansi::dim(state);
  if (state == "skipped")
    return ansi::cyan(state);
  if (state == "upstream_failed")
    return ansi::red(state);
  if (state == "retrying")
    return ansi::yellow(state);
  return std::string(state);
}

inline auto colorize_active_state(bool is_active) -> std::string {
  return is_active ? ansi::green("ACTIVE") : ansi::yellow("PAUSED");
}

class Table {
public:
  struct Column {
    std::string header;
    std::size_t width;
    bool right_align{false};
  };

  explicit Table(std::vector<Column> columns) : columns_(std::move(columns)) {}

  auto print_header() const -> void {
    for (std::size_t i = 0; i < columns_.size(); ++i) {
      if (i > 0)
        std::print(" ");
      const auto &col = columns_[i];
      auto visible = ansi::ansi_visible_width(col.header);
      auto pad = visible < col.width ? col.width - visible : 0;
      if (col.right_align) {
        std::print("{}{}", std::string(pad, ' '), col.header);
      } else {
        std::print("{}{}", col.header, std::string(pad, ' '));
      }
    }
    std::println("");

    std::size_t total_width = 0;
    for (const auto &col : columns_)
      total_width += col.width;
    total_width += columns_.size() - 1;
    std::println("{}", std::string(total_width, '-'));
  }

  auto print_row(const std::vector<std::string> &values) const -> void {
    for (std::size_t i = 0; i < columns_.size() && i < values.size(); ++i) {
      if (i > 0)
        std::print(" ");
      const auto &col = columns_[i];
      const auto &val = values[i];
      auto visible = ansi::ansi_visible_width(val);
      auto pad = visible < col.width ? col.width - visible : 0;
      if (col.right_align) {
        std::print("{}{}", std::string(pad, ' '), val);
      } else {
        std::print("{}{}", val, std::string(pad, ' '));
      }
    }
    std::println("");
  }

private:
  std::vector<Column> columns_;
};

inline auto format_timestamp(std::int64_t millis) -> std::string {
  if (millis <= 0)
    return "-";
  return util::format_local_timestamp(
      std::chrono::system_clock::time_point{std::chrono::milliseconds{millis}});
}

inline auto format_timestamp(std::chrono::system_clock::time_point tp)
    -> std::string {
  return util::format_local_timestamp(tp);
}

inline auto format_timestamp_short(std::int64_t millis) -> std::string {
  if (millis <= 0)
    return "-";
  return util::format_local_timestamp_short(
      std::chrono::system_clock::time_point{std::chrono::milliseconds{millis}});
}

inline auto format_timestamp_short(std::chrono::system_clock::time_point tp)
    -> std::string {
  return util::format_local_timestamp_short(tp);
}

inline auto format_duration_seconds(std::int64_t start_millis,
                                    std::int64_t end_millis) -> std::string {
  if (start_millis <= 0 || end_millis <= 0)
    return "-";
  auto dur = (end_millis - start_millis) / 1000;
  if (dur < 60)
    return std::format("{}s", dur);
  if (dur < 3600)
    return std::format("{}m {}s", dur / 60, dur % 60);
  return std::format("{}h {}m", dur / 3600, (dur % 3600) / 60);
}

inline auto format_duration_seconds(std::chrono::system_clock::time_point start,
                                    std::chrono::system_clock::time_point end)
    -> std::string {
  if (start == std::chrono::system_clock::time_point{} ||
      end == std::chrono::system_clock::time_point{}) {
    return "-";
  }
  auto dur =
      std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
  if (dur < 60)
    return std::format("{}s", dur);
  if (dur < 3600)
    return std::format("{}m {}s", dur / 60, dur % 60);
  return std::format("{}h {}m", dur / 3600, (dur % 3600) / 60);
}

inline auto ascii_bar(double fraction, std::size_t width = 30) -> std::string {
  auto filled = static_cast<std::size_t>(fraction * static_cast<double>(width));
  if (filled > width)
    filled = width;
  return std::format("[{}{}]", std::string(filled, '#'),
                     std::string(width - filled, ' '));
}

} // namespace dagforge::cli::fmt
