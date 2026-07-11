#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/util/log.hpp"

#include <glaze/toml.hpp>

#include <charconv>
#include <chrono>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#endif


namespace dagforge::toml_util {

/// Read entire file into a string.
[[nodiscard]] inline auto read_file(std::string_view path)
    -> Result<std::string> {
  std::ifstream in(std::string(path), std::ios::binary);
  if (!in) {
    return fail(Error::FileNotFound);
  }
  return ok(std::string((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>()));
}

/// Parse TOML text into a glaze-compatible struct T.
template <typename T>
[[nodiscard]] auto parse_toml(std::string_view text,
                              std::string *diagnostic = nullptr) -> Result<T> {
  if (text.find('\0') != std::string_view::npos) {
    if (diagnostic) {
      *diagnostic = "embedded NUL byte";
    }
    return fail(Error::ParseError);
  }

  T raw{};
  constexpr auto kOpts =
      glz::opts{.format = glz::TOML,
                .null_terminated = false,
                .error_on_unknown_keys = false};
  if (auto ec = glz::read<kOpts>(raw, text); ec) {
    auto detail = glz::format_error(ec, text);
    log::error("TOML parse error: {}", detail);
    if (diagnostic) {
      *diagnostic = std::move(detail);
    }
    return fail(Error::ParseError);
  }
  return ok(std::move(raw));
}

[[nodiscard]] inline auto parse_date_yyyy_mm_dd(std::string_view text)
    -> std::optional<std::chrono::system_clock::time_point> {
  if (text.empty()) {
    return std::nullopt;
  }

  int year_value = 0;
  unsigned month_value = 0;
  unsigned day_value = 0;

  const char *begin = text.data();
  const char *end = begin + text.size();

  auto [year_end, year_ec] = std::from_chars(begin, end, year_value);
  if (year_ec != std::errc{} || year_end >= end || *year_end != '-') {
    return std::nullopt;
  }

  auto [month_end, month_ec] = std::from_chars(year_end + 1, end, month_value);
  if (month_ec != std::errc{} || month_end >= end || *month_end != '-') {
    return std::nullopt;
  }

  auto [day_end, day_ec] = std::from_chars(month_end + 1, end, day_value);
  if (day_ec != std::errc{} || day_end != end) {
    return std::nullopt;
  }

  using namespace std::chrono;
  const year_month_day ymd{year{year_value}, month{month_value},
                           day{day_value}};
  if (!ymd.ok()) {
    return std::nullopt;
  }
  return sys_days{ymd};
}

[[nodiscard]] inline auto format_date_yyyy_mm_dd(
    std::chrono::system_clock::time_point value) -> std::string {
  using namespace std::chrono;
  const auto days = floor<sys_days::duration>(value);
  const year_month_day ymd{sys_days{days}};
  return std::format("{:04}-{:02}-{:02}", int(ymd.year()),
                     unsigned(ymd.month()), unsigned(ymd.day()));
}

} // namespace dagforge::toml_util
