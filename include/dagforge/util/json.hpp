#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#if !defined(DAGFORGE_CONSUME_NAMED_MODULES) ||                                \
    !DAGFORGE_CONSUME_NAMED_MODULES
#include "dagforge/core/error.hpp"
#endif
#include "dagforge/util/id.hpp"

#include <glaze/json.hpp>

#include <string>
#include <string_view>
#include <utility>
#endif

namespace glz {

template <typename Tag> struct meta<dagforge::TypedId<Tag>> {
  using Id = dagforge::TypedId<Tag>;

  static constexpr auto read = [](Id &id, std::string value) {
    id = Id{std::move(value)};
  };
  static constexpr auto write = [](const Id &id) -> std::string_view {
    return id.value();
  };
  static constexpr auto value = custom<read, write>;
};

} // namespace glz

namespace dagforge {

using JsonValue = glz::generic_json<glz::num_mode::i64>;

enum class JsonInputState : std::uint8_t {
  Valid,
  Incomplete,
  Invalid,
};

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

[[nodiscard]] inline auto classify_json_input_impl(std::string_view input)
    -> JsonInputState {
  glz::context context{};
  glz::skip value{};
  if (!static_cast<bool>(
          glz::read<kStrictJsonOpts>(value, input, context))) {
    return JsonInputState::Valid;
  }
  return context.error == glz::error_code::unexpected_end
             ? JsonInputState::Incomplete
             : JsonInputState::Invalid;
}

[[nodiscard]] inline auto validate_json_input(std::string_view input) -> bool {
  return classify_json_input_impl(input) == JsonInputState::Valid;
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

[[nodiscard]] inline auto classify_json_input(std::string_view input)
    -> JsonInputState {
  return detail::classify_json_input_impl(input);
}

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

class JsonPayload {
public:
  JsonPayload() = default;

  [[nodiscard]] static auto from_serialized(std::string encoded)
      -> Result<JsonPayload> {
    JsonPayload payload;
    payload.encoded_ = std::move(encoded);
    if (!payload.valid()) {
      return fail(Error::ParseError);
    }
    return ok(std::move(payload));
  }

  template <typename T>
  [[nodiscard]] static auto from(const T &value) -> Result<JsonPayload> {
    auto encoded = serialize_json(value);
    if (!encoded) {
      return fail(encoded.error());
    }
    return from_serialized(std::move(*encoded));
  }

  [[nodiscard]] auto encoded() const noexcept -> std::string_view {
    return encoded_;
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return encoded_.size();
  }

  [[nodiscard]] auto valid() const -> bool {
    return is_valid_json(encoded_);
  }

  [[nodiscard]] auto is_object() const -> bool {
    const auto first = encoded_.find_first_not_of(" \t\r\n");
    return first != std::string::npos && encoded_[first] == '{';
  }

  [[nodiscard]] auto is_null() const -> bool {
    const auto first = encoded_.find_first_not_of(" \t\r\n");
    const auto last = encoded_.find_last_not_of(" \t\r\n");
    return first != std::string::npos && last >= first &&
           std::string_view{encoded_}.substr(first, last - first + 1) ==
               "null";
  }

  [[nodiscard]] auto materialize() const -> Result<JsonValue> {
    return parse_json(encoded_);
  }

  auto operator==(const JsonPayload &) const -> bool = default;

private:
  std::string encoded_{"{}"};

  friend struct glz::meta<JsonPayload>;
};

} // namespace dagforge

namespace glz {

template <> struct meta<dagforge::JsonPayload> {
  using T = dagforge::JsonPayload;

  static constexpr auto read = [](T &value, raw_json encoded) {
    value.encoded_ = std::move(encoded.str);
  };
  static constexpr auto write = [](const T &value) -> raw_json_view {
    return raw_json_view{value.encoded_};
  };
  static constexpr auto value = custom<read, write>;
};

} // namespace glz
