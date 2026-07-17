#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/ascii.hpp"

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#endif

namespace dagforge {

template <typename T>
[[nodiscard]] auto parse(std::string_view s) noexcept -> T;

template <typename E>
[[nodiscard]] inline auto enum_to_string(E value) -> std::string {
  return std::string{to_string_view(value)};
}

namespace util {

enum class EnumParsePolicy : std::uint8_t {
  Exact,
  CaseInsensitive,
  Relaxed,
};

template <typename E>
  requires std::is_enum_v<E>
struct EnumEntry {
  std::string_view name;
  E value;
};

template <typename E> struct EnumTraits;

template <typename E>
concept RegisteredEnum =
    std::is_enum_v<E> && requires { EnumTraits<E>::entries; };

[[nodiscard]] constexpr auto
enum_token_equal(std::string_view lhs, std::string_view rhs,
                 EnumParsePolicy policy = EnumParsePolicy::Exact) noexcept
    -> bool {
  if (policy == EnumParsePolicy::Exact) {
    return lhs == rhs;
  }
  if (policy == EnumParsePolicy::CaseInsensitive) {
    if (lhs.size() != rhs.size()) {
      return false;
    }
    for (std::size_t index = 0; index < lhs.size(); ++index) {
      if (ascii_lower(lhs[index]) != ascii_lower(rhs[index])) {
        return false;
      }
    }
    return true;
  }

  std::size_t lhs_index = 0;
  std::size_t rhs_index = 0;
  while (true) {
    while (lhs_index < lhs.size() && !ascii_is_alnum(lhs[lhs_index])) {
      ++lhs_index;
    }
    while (rhs_index < rhs.size() && !ascii_is_alnum(rhs[rhs_index])) {
      ++rhs_index;
    }
    const bool lhs_done = lhs_index == lhs.size();
    const bool rhs_done = rhs_index == rhs.size();
    if (lhs_done || rhs_done) {
      return lhs_done && rhs_done;
    }
    if (ascii_lower(lhs[lhs_index]) != ascii_lower(rhs[rhs_index])) {
      return false;
    }
    ++lhs_index;
    ++rhs_index;
  }
}

template <typename E, std::size_t Size>
  requires std::is_enum_v<E>
[[nodiscard]] consteval auto
enum_entries_are_valid(const std::array<EnumEntry<E>, Size> &entries) -> bool {
  for (std::size_t lhs = 0; lhs < entries.size(); ++lhs) {
    if (entries[lhs].name.empty()) {
      return false;
    }
    for (std::size_t rhs = lhs + 1; rhs < entries.size(); ++rhs) {
      if (entries[lhs].value == entries[rhs].value ||
          enum_token_equal(entries[lhs].name, entries[rhs].name,
                           EnumParsePolicy::Relaxed)) {
        return false;
      }
    }
  }
  return true;
}

template <RegisteredEnum E>
inline constexpr auto enum_entry_count = EnumTraits<E>::entries.size();

template <RegisteredEnum E>
[[nodiscard]] consteval auto enum_names()
    -> std::array<std::string_view, enum_entry_count<E>> {
  std::array<std::string_view, enum_entry_count<E>> names{};
  for (std::size_t index = 0; index < names.size(); ++index) {
    names[index] = EnumTraits<E>::entries[index].name;
  }
  return names;
}

template <RegisteredEnum E>
[[nodiscard]] consteval auto enum_values()
    -> std::array<E, enum_entry_count<E>> {
  std::array<E, enum_entry_count<E>> values{};
  for (std::size_t index = 0; index < values.size(); ++index) {
    values[index] = EnumTraits<E>::entries[index].value;
  }
  return values;
}

template <RegisteredEnum E>
[[nodiscard]] constexpr auto
enum_to_string_view(E value, std::string_view fallback = "unknown") noexcept
    -> std::string_view {
  for (const auto &entry : EnumTraits<E>::entries) {
    if (entry.value == value) {
      return entry.name;
    }
  }
  return fallback;
}

template <RegisteredEnum E>
[[nodiscard]] constexpr auto
try_parse_enum(std::string_view input,
               EnumParsePolicy policy = EnumParsePolicy::Exact) noexcept
    -> std::optional<E> {
  for (const auto &entry : EnumTraits<E>::entries) {
    if (enum_token_equal(entry.name, input, policy)) {
      return entry.value;
    }
  }
  return std::nullopt;
}

template <RegisteredEnum E>
[[nodiscard]] constexpr auto enum_to_code(E value) noexcept
    -> std::underlying_type_t<E> {
  return static_cast<std::underlying_type_t<E>>(value);
}

template <RegisteredEnum E, std::integral I>
[[nodiscard]] constexpr auto try_parse_enum_code(I code) noexcept
    -> std::optional<E> {
  using Underlying = std::underlying_type_t<E>;
  if (!std::in_range<Underlying>(code)) {
    return std::nullopt;
  }
  const auto raw = static_cast<Underlying>(code);
  for (const auto &entry : EnumTraits<E>::entries) {
    if (static_cast<Underlying>(entry.value) == raw) {
      return entry.value;
    }
  }
  return std::nullopt;
}

} // namespace util
} // namespace dagforge
