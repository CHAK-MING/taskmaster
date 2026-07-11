#pragma once

#if !defined(DAGFORGE_BUILDING_MODULE_INTERFACE) &&                              \
    (!defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                \
     !DAGFORGE_CONSUME_NAMED_MODULES)
#include "dagforge/core/error.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <glaze/json.hpp>

#include <string>
#include <string_view>
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

[[nodiscard]] inline auto validate_json_input(std::string_view input) -> bool {
  glz::context context{};
  glz::skip value{};
  return !static_cast<bool>(
      glz::read<kStrictJsonOpts>(value, input, context));
}

} // namespace detail

[[nodiscard]] inline auto dump_json(const JsonValue &value) -> std::string {
  auto out = glz::write_json(value);
  return out ? *out : "null";
}

template <typename T>
[[nodiscard]] inline auto parse_json_as(std::string_view input) -> Result<T> {
  if (!detail::validate_json_input(input)) {
    return fail(Error::ParseError);
  }

  T value{};
  if (auto ec = glz::read<detail::kStrictJsonOpts>(value, input); ec) {
    return fail(Error::ParseError);
  }
  return ok(std::move(value));
}

[[nodiscard]] inline auto parse_json(std::string_view input)
    -> Result<JsonValue> {
  return parse_json_as<JsonValue>(input);
}

[[nodiscard]] inline auto is_valid_json(std::string_view input) -> bool {
  return detail::validate_json_input(input);
}

} // namespace dagforge
