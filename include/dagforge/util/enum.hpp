#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/ascii.hpp"

#include <glaze/core/common.hpp>

#include <concepts>
#include <string>
#include <string_view>
#include <type_traits>
#endif

namespace dagforge {

template <typename T>
[[nodiscard]] auto parse(std::string_view s) noexcept -> T;

template <typename E>
[[nodiscard]] inline auto enum_to_string(E value) -> std::string {
  return std::string{to_string_view(value)};
}

namespace util {

[[nodiscard]] inline auto normalize_enum_token(std::string_view token)
    -> std::string {
  std::string out;
  out.reserve(token.size());
  for (char c : token) {
    if (ascii_is_alnum(c)) {
      out.push_back(ascii_lower(c));
    }
  }
  return out;
}

template <typename E>
  requires std::is_enum_v<E>
inline constexpr std::size_t enum_entry_count =
    glz::tuple_size_v<decltype(glz::meta<E>::value.value)> / 2;

template <std::size_t I, typename E>
  requires std::is_enum_v<E>
[[nodiscard]] constexpr auto enum_entry_name() noexcept -> std::string_view {
  return glz::get<I * 2>(glz::meta<E>::value.value);
}

template <std::size_t I, typename E>
  requires std::is_enum_v<E>
[[nodiscard]] constexpr auto enum_entry_value() noexcept -> E {
  return glz::get<I * 2 + 1>(glz::meta<E>::value.value);
}

template <typename E>
  requires std::is_enum_v<E>
[[nodiscard]] constexpr auto
enum_to_string_view(E value, std::string_view fallback = "unknown") noexcept
    -> std::string_view {
  std::string_view out = fallback;
  glz::for_each<enum_entry_count<E>>([&]<std::size_t I>() {
    if (enum_entry_value<I, E>() == value) {
      out = enum_entry_name<I, E>();
    }
  });
  return out;
}

template <typename E>
  requires std::is_enum_v<E>
[[nodiscard]] inline auto parse_enum(std::string_view input,
                                     E default_value) noexcept -> E {
  const auto normalized_input = normalize_enum_token(input);
  E out = default_value;
  glz::for_each<enum_entry_count<E>>([&]<std::size_t I>() {
    if (normalize_enum_token(enum_entry_name<I, E>()) == normalized_input) {
      out = enum_entry_value<I, E>();
    }
  });
  return out;
}

template <typename E>
  requires std::is_enum_v<E>
[[nodiscard]] constexpr auto enum_to_code(E value) noexcept
    -> std::underlying_type_t<E> {
  return static_cast<std::underlying_type_t<E>>(value);
}

template <typename E, typename I>
  requires(std::is_enum_v<E> && std::is_integral_v<I>)
[[nodiscard]] inline auto parse_enum_code(I code, E default_value) noexcept
    -> E {
  using U = std::underlying_type_t<E>;
  const auto raw = static_cast<U>(code);
  E out = default_value;
  glz::for_each<enum_entry_count<E>>([&]<std::size_t Index>() {
    const auto value = enum_entry_value<Index, E>();
    if (static_cast<U>(value) == raw) {
      out = value;
    }
  });
  return out;
}

} // namespace util
} // namespace dagforge
