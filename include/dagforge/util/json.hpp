#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#if !defined(DAGFORGE_CONSUME_NAMED_MODULES) || !DAGFORGE_CONSUME_NAMED_MODULES
#include "dagforge/core/error.hpp"
#endif
#include "dagforge/util/id.hpp"
#include "dagforge/util/parse.hpp"

#include <glaze/json.hpp>
#include <glaze/json/schema.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#endif

namespace glz {

template <typename Tag> struct meta<dagforge::TypedId<Tag>> {
  using Id = dagforge::TypedId<Tag>;

  static constexpr auto read = [](Id &id, std::string value,
                                  glz::context &context) {
    auto parsed = Id::parse(std::move(value));
    if (!parsed) {
      context.error = glz::error_code::constraint_violated;
      context.custom_error_message = "invalid DAGForge typed ID";
      return;
    }
    id = std::move(*parsed);
  };
  static constexpr auto write = [](const Id &id,
                                   glz::context &context) -> std::string_view {
    if (!id.valid()) {
      context.error = glz::error_code::constraint_violated;
      context.custom_error_message = "invalid DAGForge typed ID";
    }
    return id.value();
  };
  static constexpr auto value = custom<read, write>;
};

} // namespace glz

namespace dagforge {

using JsonValue = glz::generic_json<glz::num_mode::i64>;
class JsonPayload;

enum class JsonInputState : std::uint8_t {
  Valid,
  Incomplete,
  Invalid,
};

namespace detail {

enum class JsonRootKind : std::uint8_t { Other, Object, Null };

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
  glz::skip value{};
  const auto error = glz::read<kStrictJsonOpts>(value, input);
  if (!error) {
    return JsonInputState::Valid;
  }
  return error.ec == glz::error_code::unexpected_end ||
                 error.ec == glz::error_code::end_reached
             ? JsonInputState::Incomplete
             : JsonInputState::Invalid;
}

[[nodiscard]] inline auto json_parse_error(std::string_view input,
                                           glz::error_ctx error,
                                           bool schema_error)
    -> util::ParseError {
  auto kind = util::ParseErrorKind::InvalidSyntax;
  if (input.empty()) {
    kind = util::ParseErrorKind::EmptyInput;
  } else if (error.ec == glz::error_code::unexpected_end ||
             error.ec == glz::error_code::end_reached) {
    kind = util::ParseErrorKind::IncompleteInput;
  } else if (schema_error) {
    kind = util::ParseErrorKind::SchemaMismatch;
  }
  return util::make_parse_error(kind, input, error.count);
}

[[nodiscard]] inline auto validate_json_input_detailed(std::string_view input)
    -> util::ParseResult<void> {
  glz::skip value{};
  if (const auto error = glz::read<kStrictJsonOpts>(value, input); error) {
    return std::unexpected{json_parse_error(input, error, false)};
  }
  return {};
}

[[nodiscard]] inline auto validate_json_input(std::string_view input) -> bool {
  return validate_json_input_detailed(input).has_value();
}

[[nodiscard]] inline auto classify_json_root(std::string_view input) noexcept
    -> JsonRootKind {
  const auto first = input.find_first_not_of(" \t\r\n");
  if (first == std::string_view::npos) {
    return JsonRootKind::Other;
  }
  if (input[first] == '{') {
    return JsonRootKind::Object;
  }
  const auto last = input.find_last_not_of(" \t\r\n");
  if (last >= first && input.substr(first, last - first + 1) == "null") {
    return JsonRootKind::Null;
  }
  return JsonRootKind::Other;
}

template <typename T, auto Opts>
[[nodiscard]] inline auto
parse_json_as_with_options_detailed(std::string_view input)
    -> util::ParseResult<T> {
  if (auto syntax = validate_json_input_detailed(input); !syntax) {
    return std::unexpected{syntax.error()};
  }

  T value{};
  if (const auto error = glz::read<Opts>(value, input); error) {
    return std::unexpected{json_parse_error(input, error, true)};
  }
  return value;
}

template <typename T, auto Opts>
[[nodiscard]] inline auto parse_json_as_with_options(std::string_view input)
    -> Result<T> {
  auto value = parse_json_as_with_options_detailed<T, Opts>(input);
  if (!value) {
    return fail(Error::ParseError);
  }
  return ok(std::move(*value));
}

} // namespace detail

[[nodiscard]] inline auto classify_json_input(std::string_view input)
    -> JsonInputState {
  return detail::classify_json_input_impl(input);
}

template <typename T>
[[nodiscard]] inline auto serialize_json(const T &value)
    -> Result<std::string> {
  auto out = glz::write_json(value);
  if (!out) {
    return fail(Error::ProtocolError);
  }
  return ok(std::move(*out));
}

template <typename T>
[[nodiscard]] inline auto json_schema_payload() -> Result<JsonPayload>;

template <typename T>
[[nodiscard]] inline auto parse_json_as_detailed(std::string_view input)
    -> util::ParseResult<T> {
  return detail::parse_json_as_with_options_detailed<T,
                                                     detail::kStrictJsonOpts>(
      input);
}

template <typename T>
[[nodiscard]] inline auto parse_json_as(std::string_view input) -> Result<T> {
  return detail::parse_json_as_with_options<T, detail::kStrictJsonOpts>(input);
}

template <typename T>
[[nodiscard]] inline auto
parse_json_as_allow_unknown_detailed(std::string_view input)
    -> util::ParseResult<T> {
  return detail::parse_json_as_with_options_detailed<
      T, detail::kAllowUnknownJsonOpts>(input);
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

[[nodiscard]] inline auto parse_json_detailed(std::string_view input)
    -> util::ParseResult<JsonValue> {
  return parse_json_as_detailed<JsonValue>(input);
}

[[nodiscard]] inline auto is_valid_json(std::string_view input) -> bool {
  return detail::validate_json_input(input);
}

class JsonPayload {
public:
  JsonPayload() = default;

  [[nodiscard]] static auto from_serialized_detailed(std::string encoded)
      -> util::ParseResult<JsonPayload> {
    if (auto validated = detail::validate_json_input_detailed(encoded);
        !validated) {
      return std::unexpected{validated.error()};
    }
    JsonPayload payload;
    payload.encoded_ = std::move(encoded);
    payload.root_kind_ = detail::classify_json_root(payload.encoded_);
    return payload;
  }

  [[nodiscard]] static auto from_serialized(std::string encoded)
      -> Result<JsonPayload> {
    auto payload = from_serialized_detailed(std::move(encoded));
    if (!payload) {
      return fail(Error::ParseError);
    }
    return ok(std::move(*payload));
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

  [[nodiscard]] auto valid() const noexcept -> bool { return true; }

  [[nodiscard]] auto is_object() const noexcept -> bool {
    return root_kind_ == detail::JsonRootKind::Object;
  }

  [[nodiscard]] auto is_null() const noexcept -> bool {
    return root_kind_ == detail::JsonRootKind::Null;
  }

  [[nodiscard]] auto materialize_detailed() const
      -> util::ParseResult<JsonValue> {
    return parse_json_detailed(encoded_);
  }

  [[nodiscard]] auto materialize() const -> Result<JsonValue> {
    return parse_json(encoded_);
  }

  [[nodiscard]] auto operator==(const JsonPayload &other) const noexcept
      -> bool {
    return encoded_ == other.encoded_;
  }

private:
  std::string encoded_{"{}"};
  detail::JsonRootKind root_kind_{detail::JsonRootKind::Object};

  friend struct glz::meta<JsonPayload>;
};

template <typename T>
[[nodiscard]] inline auto json_schema_payload() -> Result<JsonPayload> {
  auto schema = glz::write_json_schema<T>();
  if (!schema) {
    return fail(Error::ProtocolError);
  }
  return JsonPayload::from_serialized(std::move(*schema));
}

} // namespace dagforge

namespace glz {

template <> struct meta<dagforge::JsonPayload> {
  using T = dagforge::JsonPayload;

  static constexpr auto read = [](T &value, raw_json encoded) {
    value.encoded_ = std::move(encoded.str);
    value.root_kind_ = dagforge::detail::classify_json_root(value.encoded_);
  };
  static constexpr auto write = [](const T &value) -> raw_json_view {
    return raw_json_view{value.encoded_};
  };
  static constexpr auto value = custom<read, write>;
};

} // namespace glz
