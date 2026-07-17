#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/parse.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <format>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#endif

namespace dagforge::util {

namespace detail {

template <typename FormatFn>
[[nodiscard]] inline auto
format_time_or(std::chrono::system_clock::time_point tp,
               std::string_view empty_value, FormatFn &&format_fn)
    -> std::string {
  if (tp == std::chrono::system_clock::time_point{}) {
    return std::string{empty_value};
  }
  return std::forward<FormatFn>(format_fn)(tp);
}

[[nodiscard]] constexpr auto
rfc3339_error_offset(std::string_view input) noexcept -> std::size_t {
  constexpr std::array<std::pair<std::size_t, char>, 6> kPunctuation{{
      {4, '-'},
      {7, '-'},
      {10, 'T'},
      {13, ':'},
      {16, ':'},
      {19, 'Z'},
  }};
  for (const auto &[offset, expected] : kPunctuation) {
    if (offset >= input.size()) {
      return input.size();
    }
    if (input[offset] != expected) {
      return offset;
    }
  }
  for (std::size_t offset = 0; offset < 19; ++offset) {
    const bool punctuation = offset == 4 || offset == 7 || offset == 10 ||
                             offset == 13 || offset == 16;
    if (!punctuation && (input[offset] < '0' || input[offset] > '9')) {
      return offset;
    }
  }
  return 0;
}

} // namespace detail

[[nodiscard]] inline auto
format_rfc3339_utc(std::chrono::system_clock::time_point tp) -> std::string {
  return std::format("{:%FT%TZ}", std::chrono::floor<std::chrono::seconds>(tp));
}

[[nodiscard]] inline auto
format_in_zone(std::chrono::system_clock::time_point tp,
               const std::chrono::time_zone &zone,
               std::string_view format = "{:%F %T}") -> std::string {
  const std::chrono::zoned_time zoned{
      &zone, std::chrono::floor<std::chrono::seconds>(tp)};
  return std::vformat(format, std::make_format_args(zoned));
}

[[nodiscard]] inline auto parse_rfc3339_utc(std::string_view input)
    -> ParseResult<std::chrono::system_clock::time_point> {
  if (input.empty()) {
    return parse_failure(
        make_parse_error(ParseErrorKind::EmptyInput, input, 0));
  }
  if (input.size() < 20) {
    return parse_failure(
        make_parse_error(ParseErrorKind::IncompleteInput, input, input.size()));
  }

  std::chrono::sys_seconds parsed{};
  std::istringstream stream{std::string{input.substr(0, 20)}};
  stream >> std::chrono::parse("%FT%TZ", parsed);
  if (stream.fail()) {
    return parse_failure(
        make_parse_error(ParseErrorKind::InvalidSyntax, input,
                         detail::rfc3339_error_offset(input.substr(0, 20))));
  }
  if (input.size() != 20) {
    return parse_failure(
        make_parse_error(ParseErrorKind::TrailingCharacters, input, 20));
  }
  return std::chrono::system_clock::time_point{parsed.time_since_epoch()};
}

// Compatibility wrappers retain the historical sentinel behavior used by the
// CLI and wire projections. New code should prefer the exact APIs above.
[[nodiscard]] inline auto
format_iso8601(std::chrono::system_clock::time_point tp) -> std::string {
  return detail::format_time_or(tp, "", format_rfc3339_utc);
}

[[nodiscard]] inline auto format_timestamp() -> std::string {
  return format_rfc3339_utc(std::chrono::system_clock::now());
}

[[nodiscard]] inline auto
format_local_timestamp(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        try {
          return format_in_zone(value, *std::chrono::current_zone());
        } catch (const std::runtime_error &) {
          return std::string{"-"};
        }
      });
}

[[nodiscard]] inline auto
format_local_timestamp_short(std::chrono::system_clock::time_point tp)
    -> std::string {
  return detail::format_time_or(
      tp, "-", [](std::chrono::system_clock::time_point value) {
        try {
          return format_in_zone(value, *std::chrono::current_zone(),
                                "{:%F %R}");
        } catch (const std::runtime_error &) {
          return std::string{"-"};
        }
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
  const auto seconds = std::chrono::floor<std::chrono::seconds>(tp);
  const auto day = std::chrono::floor<std::chrono::days>(seconds);
  const std::chrono::year_month_day date{day};
  const std::chrono::hh_mm_ss time{seconds - day};
  const auto first_day =
      std::chrono::sys_days{date.year() / std::chrono::January / 1};

  std::tm result{};
  result.tm_sec = static_cast<int>(time.seconds().count());
  result.tm_min = static_cast<int>(time.minutes().count());
  result.tm_hour = static_cast<int>(time.hours().count());
  result.tm_mday = static_cast<int>(static_cast<unsigned>(date.day()));
  result.tm_mon = static_cast<int>(static_cast<unsigned>(date.month())) - 1;
  result.tm_year = static_cast<int>(date.year()) - 1900;
  result.tm_wday = static_cast<int>(std::chrono::weekday{day}.c_encoding());
  result.tm_yday = static_cast<int>((day - first_day).count());
  result.tm_isdst = 0;
  return result;
}

[[nodiscard]] inline auto
to_unix_millis(std::chrono::system_clock::time_point tp) -> std::int64_t {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             tp.time_since_epoch())
      .count();
}

[[nodiscard]] inline auto from_unix_millis(std::int64_t millis)
    -> std::chrono::system_clock::time_point {
  return std::chrono::system_clock::time_point{
      std::chrono::milliseconds{millis}};
}

} // namespace dagforge::util
