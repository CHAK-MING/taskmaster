#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#if !defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                \
    !DAGFORGE_CONSUME_NAMED_MODULES
#include "dagforge/core/error.hpp"
#endif

#include <glaze/json.hpp>

#include <string>
#include <string_view>
#include <utility>
#endif

namespace dagforge {

using JsonValue = glz::generic_json<glz::num_mode::i64>;

namespace detail {

struct StrictJsonOpts : glz::opts {
  bool validate_skipped = true;
  bool validate_trailing_whitespace = true;
};

inline constexpr auto kStrictJsonOpts = [] {
  StrictJsonOpts opts{};
  opts.null_terminated = false;
  return opts;
}();

inline constexpr auto kAllowUnknownJsonOpts = [] {
  auto opts = kStrictJsonOpts;
  opts.error_on_unknown_keys = false;
  return opts;
}();

[[nodiscard]] inline auto validate_json_input(std::string_view input) -> bool {
  glz::context context{};
  glz::skip value{};
  return !static_cast<bool>(
      glz::read<kStrictJsonOpts>(value, input, context));
}

template <typename T, auto Opts>
[[nodiscard]] inline auto parse_json_as_with_options(std::string_view input)
    -> Result<T> {
  if (!validate_json_input(input)) {
    return fail(Error::ParseError);
  }

  T value{};
  if (auto ec = glz::read<Opts>(value, input); ec) {
    return fail(Error::ParseError);
  }
  return ok(std::move(value));
}

} // namespace detail

template <typename T>
[[nodiscard]] inline auto serialize_json(const T &value) -> Result<std::string> {
  auto out = glz::write_json(value);
  if (!out) {
    return fail(Error::ProtocolError);
  }
  return ok(std::move(*out));
}

[[nodiscard]] inline auto dump_json(const JsonValue &value) -> std::string {
  auto out = serialize_json(value);
  return out ? std::move(*out) : "null";
}

template <typename T>
[[nodiscard]] inline auto parse_json_as(std::string_view input) -> Result<T> {
  return detail::parse_json_as_with_options<T, detail::kStrictJsonOpts>(input);
}

template <typename T>
[[nodiscard]] inline auto parse_json_as_allow_unknown(std::string_view input)
    -> Result<T> {
  return detail::parse_json_as_with_options<T, detail::kAllowUnknownJsonOpts>(
      input);
}

[[nodiscard]] inline auto parse_json(std::string_view input)
    -> Result<JsonValue> {
  return parse_json_as<JsonValue>(input);
}

[[nodiscard]] inline auto is_valid_json(std::string_view input) -> bool {
  return detail::validate_json_input(input);
}

} // namespace dagforge
