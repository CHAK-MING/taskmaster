#include "unicode.hpp"

#include <algorithm>

namespace dagforge::jsonata::detail {
namespace {

struct DecodedLead {
  std::size_t width{};
  std::uint32_t value{};
  std::uint32_t minimum{};
};

[[nodiscard]] auto decode_lead(unsigned char lead) noexcept
    -> std::optional<DecodedLead> {
  if (lead <= 0x7FU) {
    return DecodedLead{.width = 1, .value = lead, .minimum = 0};
  }
  if (lead >= 0xC2U && lead <= 0xDFU) {
    return DecodedLead{.width = 2,
                       .value = static_cast<std::uint32_t>(lead & 0x1FU),
                       .minimum = 0x80U};
  }
  if (lead >= 0xE0U && lead <= 0xEFU) {
    return DecodedLead{.width = 3,
                       .value = static_cast<std::uint32_t>(lead & 0x0FU),
                       .minimum = 0x800U};
  }
  if (lead >= 0xF0U && lead <= 0xF4U) {
    return DecodedLead{.width = 4,
                       .value = static_cast<std::uint32_t>(lead & 0x07U),
                       .minimum = 0x10000U};
  }
  return std::nullopt;
}

[[nodiscard]] auto codepoint_width(unsigned char lead) noexcept -> std::size_t {
  if (lead < 0x80U) {
    return 1;
  }
  if (lead < 0xE0U) {
    return 2;
  }
  if (lead < 0xF0U) {
    return 3;
  }
  return 4;
}

} // namespace

auto invalid_utf8_offset(std::string_view text) noexcept
    -> std::optional<std::size_t> {
  std::size_t index = 0;
  while (index < text.size()) {
    const auto decoded = decode_lead(static_cast<unsigned char>(text[index]));
    if (!decoded) {
      return index;
    }
    if (index + decoded->width > text.size()) {
      return index;
    }
    auto codepoint = decoded->value;
    for (std::size_t offset = 1; offset < decoded->width; ++offset) {
      const auto continuation =
          static_cast<unsigned char>(text[index + offset]);
      if ((continuation & 0xC0U) != 0x80U) {
        return index + offset;
      }
      codepoint = (codepoint << 6U) | (continuation & 0x3FU);
    }
    if (codepoint < decoded->minimum || codepoint > 0x10FFFFU ||
        (codepoint >= 0xD800U && codepoint <= 0xDFFFU)) {
      return index;
    }
    index += decoded->width;
  }
  return std::nullopt;
}

auto decode_utf8_codepoint(std::string_view character) noexcept
    -> std::uint32_t {
  if (character.empty()) {
    return 0;
  }
  const auto decoded =
      decode_lead(static_cast<unsigned char>(character.front()));
  if (!decoded || character.size() < decoded->width) {
    return static_cast<unsigned char>(character.front());
  }
  auto codepoint = decoded->value;
  for (std::size_t offset = 1; offset < decoded->width; ++offset) {
    codepoint = (codepoint << 6U) |
                (static_cast<unsigned char>(character[offset]) & 0x3FU);
  }
  return codepoint;
}

auto append_utf8(std::string &output, std::uint32_t codepoint) -> void {
  if (codepoint <= 0x7FU) {
    output.push_back(static_cast<char>(codepoint));
  } else if (codepoint <= 0x7FFU) {
    output.push_back(static_cast<char>(0xC0U | (codepoint >> 6U)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  } else if (codepoint <= 0xFFFFU) {
    output.push_back(static_cast<char>(0xE0U | (codepoint >> 12U)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  } else {
    output.push_back(static_cast<char>(0xF0U | (codepoint >> 18U)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 12U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | ((codepoint >> 6U) & 0x3FU)));
    output.push_back(static_cast<char>(0x80U | (codepoint & 0x3FU)));
  }
}

auto encode_utf8(std::uint32_t codepoint) -> std::string {
  std::string result;
  append_utf8(result, codepoint);
  return result;
}

auto utf16_units(std::string_view text, std::size_t byte_offset) noexcept
    -> std::size_t {
  byte_offset = std::min(byte_offset, text.size());
  std::size_t units = 0;
  std::size_t index = 0;
  while (index < byte_offset) {
    const auto width = codepoint_width(static_cast<unsigned char>(text[index]));
    const auto available = std::min(width, byte_offset - index);
    const auto codepoint = decode_utf8_codepoint(text.substr(index, available));
    units += codepoint > 0xFFFFU ? 2U : 1U;
    index += available;
  }
  return units;
}

auto unicode_characters(std::string_view input) -> std::vector<std::string> {
  std::vector<std::string> result;
  std::size_t index = 0;
  while (index < input.size()) {
    const auto width =
        codepoint_width(static_cast<unsigned char>(input[index]));
    const auto available = std::min(width, input.size() - index);
    result.emplace_back(input.substr(index, available));
    index += available;
  }
  return result;
}

} // namespace dagforge::jsonata::detail
