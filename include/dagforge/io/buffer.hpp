#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <concepts>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#endif

namespace dagforge::io {

// Buffer Concepts - Compile-time constraints for buffer types

template <typename T>
concept MutableBufferSequence = requires(T &t) {
  { std::data(t) } -> std::convertible_to<void *>;
  { std::size(t) } -> std::convertible_to<std::size_t>;
};

template <typename T>
concept ConstBufferSequence = requires(const T &t) {
  { std::data(t) } -> std::convertible_to<const void *>;
  { std::size(t) } -> std::convertible_to<std::size_t>;
};

// Buffer Views - Type-safe buffer representations using C++23 std::span

using MutableBuffer = std::span<std::byte>;
using ConstBuffer = std::span<const std::byte>;

// Buffer Factory Functions

template <MutableBufferSequence T>
[[nodiscard]] constexpr auto buffer(T &container) noexcept -> MutableBuffer {
  return std::as_writable_bytes(std::span{container});
}

[[nodiscard]] constexpr auto buffer(void *data, std::size_t size) noexcept
    -> MutableBuffer {
  return {static_cast<std::byte *>(data), size};
}

template <ConstBufferSequence T>
[[nodiscard]] constexpr auto buffer(const T &container) noexcept
    -> ConstBuffer {
  return std::as_bytes(std::span{container});
}

[[nodiscard]] constexpr auto buffer(const void *data, std::size_t size) noexcept
    -> ConstBuffer {
  return {static_cast<const std::byte *>(data), size};
}

[[nodiscard]] constexpr auto buffer(std::string_view sv) noexcept
    -> ConstBuffer {
  return {reinterpret_cast<const std::byte *>(sv.data()), sv.size()};
}

} // namespace dagforge::io
