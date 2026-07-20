#pragma once

#include "model.hpp"

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>

namespace dagforge::jsonata::detail {

enum class TokenKind : std::uint8_t {
  End,
  Number,
  String,
  Name,
  Variable,
  Regex,
  Operator,
  Value,
};

struct Token {
  TokenKind kind{TokenKind::End};
  std::string text;
  double number{};
  ByteSpan span;
};

struct CompileInterrupt {
  std::stop_token stop_token;
  std::optional<std::chrono::steady_clock::time_point> deadline;
  std::string_view diagnostic_source;
  std::size_t diagnostic_byte_offset{};
};

class Lexer {
public:
  Lexer(std::string_view source, CompileLimits limits,
        const CompileInterrupt *interrupt = nullptr);

  [[nodiscard]] auto next(bool prefix) -> Result<Token>;

private:
  [[nodiscard]] auto check_interrupt() const -> Result<void>;
  auto skip_whitespace() -> void;
  [[nodiscard]] auto starts_with(std::string_view value) const -> bool;
  [[nodiscard]] auto scan_string(char quote, std::size_t start)
      -> Result<Token>;
  [[nodiscard]] auto scan_unicode_escape() -> std::optional<std::uint32_t>;
  [[nodiscard]] auto scan_quoted_name(std::size_t start) -> Result<Token>;
  [[nodiscard]] auto scan_variable(std::size_t start) -> Result<Token>;
  [[nodiscard]] auto scan_regex(std::size_t start) -> Result<Token>;
  [[nodiscard]] auto scan_number(std::size_t start) -> Result<Token>;
  [[nodiscard]] auto scan_operator(std::size_t start) -> Result<Token>;
  [[nodiscard]] auto scan_name(std::size_t start) -> Result<Token>;

  std::string_view source_;
  CompileLimits limits_;
  const CompileInterrupt *interrupt_{};
  std::size_t offset_{};
  std::size_t token_count_{};
};

} // namespace dagforge::jsonata::detail
