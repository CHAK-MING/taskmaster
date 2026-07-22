#pragma once

#include <algorithm>
#include <charconv>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string_view>
#include <system_error>

namespace dagforge::util {

enum class ParseErrorKind : std::uint8_t {
  EmptyInput,
  InvalidSyntax,
  IncompleteInput,
  OutOfRange,
  TrailingCharacters,
  SchemaMismatch,
  InvalidBase,
};

struct ParseError {
  ParseErrorKind kind{ParseErrorKind::InvalidSyntax};
  std::size_t offset{};
  std::size_t line{1};
  std::size_t column{1};

  [[nodiscard]] constexpr auto message() const noexcept -> std::string_view {
    switch (kind) {
    case ParseErrorKind::EmptyInput:
      return "empty input";
    case ParseErrorKind::InvalidSyntax:
      return "invalid syntax";
    case ParseErrorKind::IncompleteInput:
      return "incomplete input";
    case ParseErrorKind::OutOfRange:
      return "value out of range";
    case ParseErrorKind::TrailingCharacters:
      return "trailing characters";
    case ParseErrorKind::SchemaMismatch:
      return "input does not match the expected schema";
    case ParseErrorKind::InvalidBase:
      return "numeric base must be between 2 and 36";
    }
    return "parse error";
  }

  auto operator==(const ParseError &) const -> bool = default;
};

template <typename T> using ParseResult = std::expected<T, ParseError>;

[[nodiscard]] constexpr auto parse_failure(ParseError error) noexcept
    -> std::unexpected<ParseError> {
  return std::unexpected{error};
}

[[nodiscard]] constexpr auto make_parse_error(ParseErrorKind kind,
                                              std::string_view input,
                                              std::size_t offset) noexcept
    -> ParseError {
  offset = std::min(offset, input.size());
  std::size_t line = 1;
  std::size_t column = 1;
  for (std::size_t index = 0; index < offset; ++index) {
    if (input[index] == '\n') {
      ++line;
      column = 1;
    } else {
      ++column;
    }
  }
  return ParseError{
      .kind = kind, .offset = offset, .line = line, .column = column};
}

template <std::integral T>
  requires(!std::same_as<T, bool>)
[[nodiscard]] auto parse_integer(std::string_view input, int base = 10)
    -> ParseResult<T> {
  if (base < 2 || base > 36) {
    return parse_failure(
        make_parse_error(ParseErrorKind::InvalidBase, input, 0));
  }
  if (input.empty()) {
    return parse_failure(
        make_parse_error(ParseErrorKind::EmptyInput, input, 0));
  }

  T value{};
  const char *const begin = input.data();
  const char *const end = begin + input.size();
  const auto [position, error] = std::from_chars(begin, end, value, base);
  const auto offset = static_cast<std::size_t>(position - begin);
  if (error == std::errc::invalid_argument) {
    return parse_failure(
        make_parse_error(ParseErrorKind::InvalidSyntax, input, offset));
  }
  if (error == std::errc::result_out_of_range) {
    return parse_failure(
        make_parse_error(ParseErrorKind::OutOfRange, input, offset));
  }
  if (position != end) {
    return parse_failure(
        make_parse_error(ParseErrorKind::TrailingCharacters, input, offset));
  }
  return value;
}

} // namespace dagforge::util
