#pragma once

#include <string>
#include <string_view>

namespace dagforge::util {

[[nodiscard]] constexpr auto ascii_is_digit(char value) noexcept -> bool {
  return value >= '0' && value <= '9';
}

[[nodiscard]] constexpr auto ascii_is_alpha(char value) noexcept -> bool {
  return (value >= 'A' && value <= 'Z') ||
         (value >= 'a' && value <= 'z');
}

[[nodiscard]] constexpr auto ascii_is_alnum(char value) noexcept -> bool {
  return ascii_is_alpha(value) || ascii_is_digit(value);
}

[[nodiscard]] constexpr auto ascii_lower(char value) noexcept -> char {
  return value >= 'A' && value <= 'Z'
             ? static_cast<char>(value + ('a' - 'A'))
             : value;
}

[[nodiscard]] constexpr auto ascii_upper(char value) noexcept -> char {
  return value >= 'a' && value <= 'z'
             ? static_cast<char>(value - ('a' - 'A'))
             : value;
}

[[nodiscard]] inline auto ascii_lowercase(std::string_view value)
    -> std::string {
  std::string out;
  out.reserve(value.size());
  for (const char character : value) {
    out.push_back(ascii_lower(character));
  }
  return out;
}

[[nodiscard]] inline auto ascii_uppercase(std::string_view value)
    -> std::string {
  std::string out;
  out.reserve(value.size());
  for (const char character : value) {
    out.push_back(ascii_upper(character));
  }
  return out;
}

} // namespace dagforge::util
