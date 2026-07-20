#include "lexer.hpp"

#include "unicode.hpp"

#include <array>
#include <charconv>
#include <cctype>
#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>

namespace dagforge::jsonata::detail {

namespace {

[[nodiscard]] auto is_operator_character(char value) -> bool {
  constexpr std::string_view kOperators = ".[]{}(),:;?+-*/%=<>|^@#&!~";
  return kOperators.find(value) != std::string_view::npos;
}

[[nodiscard]] auto hex_value(char value) -> std::optional<std::uint32_t> {
  if (value >= '0' && value <= '9') {
    return static_cast<std::uint32_t>(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return static_cast<std::uint32_t>(value - 'a' + 10);
  }
  if (value >= 'A' && value <= 'F') {
    return static_cast<std::uint32_t>(value - 'A' + 10);
  }
  return std::nullopt;
}

} // namespace

Lexer::Lexer(std::string_view source, CompileLimits limits,
             const CompileInterrupt *interrupt)
    : source_(source), limits_(limits), interrupt_(interrupt) {}

auto Lexer::check_interrupt() const -> Result<void> {
  if (interrupt_ == nullptr) {
    return {};
  }
  if (interrupt_->stop_token.stop_requested()) {
    return std::unexpected(host_failure("H1001", "JSONata evaluation cancelled",
                                        interrupt_->diagnostic_source,
                                        interrupt_->diagnostic_byte_offset));
  }
  if (interrupt_->deadline &&
      std::chrono::steady_clock::now() > *interrupt_->deadline) {
    return std::unexpected(dynamic_failure(
        "D1012", "JSONata evaluation timeout exceeded",
        interrupt_->diagnostic_source, interrupt_->diagnostic_byte_offset));
  }
  return {};
}

auto Lexer::next(bool prefix) -> Result<Token> {
  auto interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  while (true) {
    skip_whitespace();
    if (!starts_with("/*")) {
      break;
    }
    const auto comment_start = offset_;
    offset_ += 2;
    while (offset_ + 1 < source_.size() && !starts_with("*/")) {
      ++offset_;
    }
    if (offset_ + 1 >= source_.size()) {
      return std::unexpected(syntax_failure(
          "S0106", "Comment has no closing tag", source_, comment_start));
    }
    offset_ += 2;
  }

  interrupted = check_interrupt();
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }

  if (++token_count_ > limits_.max_tokens) {
    return std::unexpected(host_failure("H1102", "JSONata token limit exceeded",
                                        source_, offset_));
  }
  if (offset_ >= source_.size()) {
    return Token{.kind = TokenKind::End,
                 .span = ByteSpan{.begin = offset_, .end = offset_}};
  }

  const auto start = offset_;
  const auto current = source_[offset_];
  if (current == '"' || current == '\'') {
    return scan_string(current, start);
  }
  if (current == '`') {
    return scan_quoted_name(start);
  }
  if (current == '$') {
    return scan_variable(start);
  }
  if (prefix && current == '/') {
    return scan_regex(start);
  }
  if (current >= '0' && current <= '9') {
    return scan_number(start);
  }
  if (is_operator_character(current)) {
    return scan_operator(start);
  }
  return scan_name(start);
}

auto Lexer::skip_whitespace() -> void {
  while (offset_ < source_.size()) {
    const auto value = source_[offset_];
    if (value != ' ' && value != '\t' && value != '\n' && value != '\r' &&
        value != '\v') {
      return;
    }
    ++offset_;
  }
}

auto Lexer::starts_with(std::string_view value) const -> bool {
  return source_.substr(offset_).starts_with(value);
}

auto Lexer::scan_string(char quote, std::size_t start) -> Result<Token> {
  ++offset_;
  std::string value;
  while (offset_ < source_.size()) {
    auto current = source_[offset_++];
    if (current == quote) {
      return Token{.kind = TokenKind::String,
                   .text = std::move(value),
                   .span = ByteSpan{.begin = start, .end = offset_}};
    }
    if (current != '\\') {
      value.push_back(current);
      continue;
    }
    if (offset_ >= source_.size()) {
      break;
    }
    current = source_[offset_++];
    switch (current) {
    case '"':
    case '\'':
    case '\\':
    case '/':
      value.push_back(current);
      break;
    case 'b':
      value.push_back('\b');
      break;
    case 'f':
      value.push_back('\f');
      break;
    case 'n':
      value.push_back('\n');
      break;
    case 'r':
      value.push_back('\r');
      break;
    case 't':
      value.push_back('\t');
      break;
    case 'u': {
      const auto escape_offset = offset_ - 1;
      auto first = scan_unicode_escape();
      if (!first) {
        return std::unexpected(
            syntax_failure("S0104",
                           "The escape sequence \\u must be followed by 4 hex "
                           "digits",
                           source_, escape_offset));
      }
      std::uint32_t codepoint = *first;
      if (codepoint >= 0xD800U && codepoint <= 0xDBFFU &&
          offset_ + 5 < source_.size() && source_[offset_] == '\\' &&
          source_[offset_ + 1] == 'u') {
        const auto saved = offset_;
        offset_ += 2;
        auto second = scan_unicode_escape();
        if (second && *second >= 0xDC00U && *second <= 0xDFFFU) {
          codepoint =
              0x10000U + ((codepoint - 0xD800U) << 10U) + (*second - 0xDC00U);
        } else {
          offset_ = saved;
        }
      }
      append_utf8(value, codepoint);
      break;
    }
    default:
      return std::unexpected(
          syntax_failure("S0103", "Unsupported escape sequence", source_,
                         offset_ - 1, std::string(1, current)));
    }
  }
  return std::unexpected(syntax_failure(
      "S0101", "String literal must be terminated by a matching quote", source_,
      source_.size()));
}

auto Lexer::scan_unicode_escape() -> std::optional<std::uint32_t> {
  if (offset_ + 4 > source_.size()) {
    return std::nullopt;
  }
  std::uint32_t value = 0;
  for (std::size_t index = 0; index < 4; ++index) {
    auto digit = hex_value(source_[offset_ + index]);
    if (!digit) {
      return std::nullopt;
    }
    value = (value << 4U) | *digit;
  }
  offset_ += 4;
  return value;
}

auto Lexer::scan_quoted_name(std::size_t start) -> Result<Token> {
  ++offset_;
  const auto end = source_.find('`', offset_);
  if (end == std::string_view::npos) {
    offset_ = source_.size();
    return std::unexpected(
        syntax_failure("S0105", "Quoted property name has no closing backtick",
                       source_, offset_));
  }
  std::string value{source_.substr(offset_, end - offset_)};
  offset_ = end + 1;
  return Token{.kind = TokenKind::Name,
               .text = std::move(value),
               .span = ByteSpan{.begin = start, .end = offset_}};
}

auto Lexer::scan_variable(std::size_t start) -> Result<Token> {
  ++offset_;
  const auto name_start = offset_;
  if (offset_ < source_.size() && source_[offset_] == '$') {
    ++offset_;
  } else {
    while (offset_ < source_.size() &&
           !std::isspace(static_cast<unsigned char>(source_[offset_])) &&
           !is_operator_character(source_[offset_])) {
      ++offset_;
    }
  }
  return Token{
      .kind = TokenKind::Variable,
      .text = std::string{source_.substr(name_start, offset_ - name_start)},
      .span = ByteSpan{.begin = start, .end = offset_}};
}

auto Lexer::scan_regex(std::size_t start) -> Result<Token> {
  ++offset_;
  const auto pattern_start = offset_;
  std::size_t depth = 0;
  bool escaped = false;
  while (offset_ < source_.size()) {
    const auto current = source_[offset_];
    if (!escaped) {
      if (current == '/' && depth == 0) {
        break;
      }
      if (current == '(' || current == '[' || current == '{') {
        ++depth;
      } else if ((current == ')' || current == ']' || current == '}') &&
                 depth > 0) {
        --depth;
      }
    }
    escaped = !escaped && current == '\\';
    if (current != '\\') {
      escaped = false;
    }
    ++offset_;
  }
  if (offset_ >= source_.size()) {
    return std::unexpected(syntax_failure(
        "S0302", "Regular expression has no closing slash", source_, offset_));
  }
  if (offset_ == pattern_start) {
    return std::unexpected(syntax_failure(
        "S0301", "Regular expression cannot be empty", source_, offset_));
  }
  std::string pattern{source_.substr(pattern_start, offset_ - pattern_start)};
  ++offset_;
  std::string flags;
  while (offset_ < source_.size() &&
         (source_[offset_] == 'i' || source_[offset_] == 'm')) {
    flags.push_back(source_[offset_++]);
  }
  return Token{.kind = TokenKind::Regex,
               .text = pattern + "\n" + flags,
               .span = ByteSpan{.begin = start, .end = offset_}};
}

auto Lexer::scan_number(std::size_t start) -> Result<Token> {
  if (source_.substr(offset_).starts_with("0x") ||
      source_.substr(offset_).starts_with("0X") ||
      source_.substr(offset_).starts_with("0o") ||
      source_.substr(offset_).starts_with("0O") ||
      source_.substr(offset_).starts_with("0b") ||
      source_.substr(offset_).starts_with("0B")) {
    const auto marker = source_[offset_ + 1];
    const int base = marker == 'x' || marker == 'X'   ? 16
                     : marker == 'o' || marker == 'O' ? 8
                                                      : 2;
    offset_ += 2;
    const auto digits_start = offset_;
    while (offset_ < source_.size() &&
           std::isalnum(static_cast<unsigned char>(source_[offset_]))) {
      ++offset_;
    }
    std::uint64_t integer = 0;
    const auto digits = source_.substr(digits_start, offset_ - digits_start);
    const auto [end, error] = std::from_chars(
        digits.data(), digits.data() + digits.size(), integer, base);
    if (digits.empty() || error != std::errc{} ||
        end != digits.data() + digits.size()) {
      return std::unexpected(
          syntax_failure("S0102", "Number is out of range", source_, start,
                         std::string{source_.substr(start, offset_ - start)}));
    }
    return Token{.kind = TokenKind::Number,
                 .number = static_cast<double>(integer),
                 .span = ByteSpan{.begin = start, .end = offset_}};
  }

  while (offset_ < source_.size() &&
         std::isdigit(static_cast<unsigned char>(source_[offset_]))) {
    ++offset_;
  }
  if (offset_ < source_.size() && source_[offset_] == '.' &&
      (offset_ + 1 >= source_.size() || source_[offset_ + 1] != '.')) {
    ++offset_;
    while (offset_ < source_.size() &&
           std::isdigit(static_cast<unsigned char>(source_[offset_]))) {
      ++offset_;
    }
  }
  if (offset_ < source_.size() &&
      (source_[offset_] == 'e' || source_[offset_] == 'E')) {
    ++offset_;
    if (offset_ < source_.size() &&
        (source_[offset_] == '+' || source_[offset_] == '-')) {
      ++offset_;
    }
    while (offset_ < source_.size() &&
           std::isdigit(static_cast<unsigned char>(source_[offset_]))) {
      ++offset_;
    }
  }
  const auto token = source_.substr(start, offset_ - start);
  double number = 0.0;
  const auto [end, error] =
      std::from_chars(token.data(), token.data() + token.size(), number,
                      std::chars_format::general);
  if (error != std::errc{} || end != token.data() + token.size() ||
      !std::isfinite(number)) {
    return std::unexpected(syntax_failure("S0102", "Number is out of range",
                                          source_, start, std::string{token}));
  }
  return Token{.kind = TokenKind::Number,
               .number = number,
               .span = ByteSpan{.begin = start, .end = offset_}};
}

auto Lexer::scan_operator(std::size_t start) -> Result<Token> {
  constexpr std::array<std::string_view, 9> kDoubleOperators{
      "..", ":=", "!=", ">=", "<=", "**", "~>", "?:", "??"};
  for (const auto candidate : kDoubleOperators) {
    if (source_.substr(offset_).starts_with(candidate)) {
      offset_ += candidate.size();
      return Token{.kind = TokenKind::Operator,
                   .text = std::string{candidate},
                   .span = ByteSpan{.begin = start, .end = offset_}};
    }
  }
  const auto value = source_[offset_++];
  return Token{.kind = TokenKind::Operator,
               .text = std::string(1, value),
               .span = ByteSpan{.begin = start, .end = offset_}};
}

auto Lexer::scan_name(std::size_t start) -> Result<Token> {
  while (offset_ < source_.size() &&
         !std::isspace(static_cast<unsigned char>(source_[offset_])) &&
         !is_operator_character(source_[offset_])) {
    ++offset_;
  }
  if (offset_ == start) {
    return std::unexpected(syntax_failure("S0204", "Unknown operator", source_,
                                          start,
                                          std::string(1, source_[offset_])));
  }
  std::string name{source_.substr(start, offset_ - start)};
  if (name == "and" || name == "or" || name == "in") {
    return Token{.kind = TokenKind::Operator,
                 .text = std::move(name),
                 .span = ByteSpan{.begin = start, .end = offset_}};
  }
  if (name == "true" || name == "false" || name == "null") {
    return Token{.kind = TokenKind::Value,
                 .text = std::move(name),
                 .span = ByteSpan{.begin = start, .end = offset_}};
  }
  return Token{.kind = TokenKind::Name,
               .text = std::move(name),
               .span = ByteSpan{.begin = start, .end = offset_}};
}

} // namespace dagforge::jsonata::detail
