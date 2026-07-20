#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge::jsonata::detail {

[[nodiscard]] auto invalid_utf8_offset(std::string_view text) noexcept
    -> std::optional<std::size_t>;

[[nodiscard]] inline auto valid_utf8(std::string_view text) noexcept -> bool {
  return !invalid_utf8_offset(text).has_value();
}

[[nodiscard]] auto decode_utf8_codepoint(std::string_view character) noexcept
    -> std::uint32_t;

auto append_utf8(std::string &output, std::uint32_t codepoint) -> void;

[[nodiscard]] auto encode_utf8(std::uint32_t codepoint) -> std::string;

[[nodiscard]] auto utf16_units(std::string_view text,
                               std::size_t byte_offset) noexcept -> std::size_t;

[[nodiscard]] auto unicode_characters(std::string_view input)
    -> std::vector<std::string>;

} // namespace dagforge::jsonata::detail
