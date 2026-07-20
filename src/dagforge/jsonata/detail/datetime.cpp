#include "datetime.hpp"

#include "regex_adapter.hpp"
#include "unicode.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <numeric>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

constexpr std::array<std::string_view, 20> kFew{
    "Zero",    "One",     "Two",       "Three",    "Four",
    "Five",    "Six",     "Seven",     "Eight",    "Nine",
    "Ten",     "Eleven",  "Twelve",    "Thirteen", "Fourteen",
    "Fifteen", "Sixteen", "Seventeen", "Eighteen", "Nineteen"};
constexpr std::array<std::string_view, 20> kOrdinals{
    "Zeroth",    "First",     "Second",      "Third",      "Fourth",
    "Fifth",     "Sixth",     "Seventh",     "Eighth",     "Ninth",
    "Tenth",     "Eleventh",  "Twelfth",     "Thirteenth", "Fourteenth",
    "Fifteenth", "Sixteenth", "Seventeenth", "Eighteenth", "Nineteenth"};
constexpr std::array<std::string_view, 9> kDecades{
    "Twenty",  "Thirty", "Forty",  "Fifty",  "Sixty",
    "Seventy", "Eighty", "Ninety", "Hundred"};
constexpr std::array<std::string_view, 4> kMagnitudes{"Thousand", "Million",
                                                      "Billion", "Trillion"};
constexpr std::array<std::uint32_t, 37> kDecimalGroups{
    0x30,   0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6,
    0x0B66, 0x0BE6, 0x0C66, 0x0CE6, 0x0D66, 0x0DE6, 0x0E50, 0x0ED0,
    0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
    0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900,
    0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10};

enum class IntegerPrimary : std::uint8_t {
  Decimal,
  Letters,
  Roman,
  Words,
  Sequence,
};

enum class LetterCase : std::uint8_t { Upper, Lower, Title };

struct GroupingSeparator {
  std::size_t position{};
  std::string character;
};

struct IntegerPicture {
  IntegerPrimary primary{IntegerPrimary::Decimal};
  LetterCase letter_case{LetterCase::Lower};
  bool ordinal{false};
  std::uint32_t zero_code{0x30};
  std::size_t mandatory_digits{};
  std::size_t optional_digits{};
  bool regular{false};
  std::size_t regular_position{};
  std::string regular_character;
  std::vector<GroupingSeparator> separators;
  std::string token;
};

struct DecimalSymbols {
  std::string decimal_separator{"."};
  std::string grouping_separator{","};
  std::string exponent_separator{"e"};
  std::string infinity{"Infinity"};
  std::string minus_sign{"-"};
  std::string nan{"NaN"};
  std::string percent{"%"};
  std::string per_mille{"‰"};
  std::string zero_digit{"0"};
  std::string optional_digit{"#"};
  std::string pattern_separator{";"};
  std::uint32_t zero_code{0x30};
  std::vector<std::string> digit_family;
};

struct NumberSubpicture {
  std::string prefix;
  std::string suffix;
  std::string active;
  std::string mantissa;
  std::optional<std::string> exponent;
  std::string integer;
  std::string fractional;
  std::string picture;
  std::vector<std::size_t> integer_grouping_positions;
  std::size_t regular_grouping{};
  std::size_t minimum_integer_size{};
  std::size_t scaling_factor{};
  std::vector<std::size_t> fractional_grouping_positions;
  std::size_t minimum_fractional_size{};
  std::size_t maximum_fractional_size{};
  std::size_t minimum_exponent_size{};
};

[[nodiscard]] auto lowercase_ascii(std::string value) -> std::string {
  std::ranges::transform(value, value.begin(), [](unsigned char character) {
    return static_cast<char>(std::tolower(character));
  });
  return value;
}

[[nodiscard]] auto uppercase_ascii(std::string value) -> std::string {
  std::ranges::transform(value, value.begin(), [](unsigned char character) {
    return static_cast<char>(std::toupper(character));
  });
  return value;
}

[[nodiscard]] auto number_to_words(double value, bool ordinal) -> std::string {
  const auto lookup = [&](const auto &self, double number, bool previous,
                          bool use_ordinal) -> std::string {
    if (number <= 19) {
      const auto index = static_cast<std::size_t>(number);
      return std::string{previous ? " and " : ""} +
             std::string{use_ordinal ? kOrdinals[index] : kFew[index]};
    }
    if (number < 100) {
      const auto tens = static_cast<std::size_t>(std::floor(number / 10));
      const auto remainder = std::fmod(number, 10);
      std::string result{previous ? " and " : ""};
      result += kDecades[tens - 2];
      if (remainder > 0) {
        result += '-' + self(self, remainder, false, use_ordinal);
      } else if (use_ordinal) {
        result.resize(result.size() - 1);
        result += "ieth";
      }
      return result;
    }
    if (number < 1000) {
      const auto hundreds = static_cast<std::size_t>(std::floor(number / 100));
      const auto remainder = std::fmod(number, 100);
      std::string result{previous ? ", " : ""};
      result += kFew[hundreds];
      result += " Hundred";
      if (remainder > 0) {
        result += self(self, remainder, true, use_ordinal);
      } else if (use_ordinal) {
        result += "th";
      }
      return result;
    }
    auto magnitude = static_cast<int>(std::floor(std::log10(number) / 3));
    magnitude = std::min(magnitude, static_cast<int>(kMagnitudes.size()));
    const auto factor = std::pow(10.0, magnitude * 3);
    const auto mantissa = std::floor(number / factor);
    const auto remainder = number - mantissa * factor;
    std::string result{previous ? ", " : ""};
    result += self(self, mantissa, false, false);
    result += ' ';
    result += kMagnitudes[static_cast<std::size_t>(magnitude - 1)];
    if (remainder > 0) {
      result += self(self, remainder, true, use_ordinal);
    } else if (use_ordinal) {
      result += "th";
    }
    return result;
  };
  return lookup(lookup, value, false, ordinal);
}

[[nodiscard]] auto word_values()
    -> const std::unordered_map<std::string, double> & {
  static const auto values = [] {
    std::unordered_map<std::string, double> result;
    for (std::size_t index = 0; index < kFew.size(); ++index) {
      result.emplace(lowercase_ascii(std::string{kFew[index]}),
                     static_cast<double>(index));
      result.emplace(lowercase_ascii(std::string{kOrdinals[index]}),
                     static_cast<double>(index));
    }
    for (std::size_t index = 0; index < kDecades.size(); ++index) {
      auto word = lowercase_ascii(std::string{kDecades[index]});
      const auto value = static_cast<double>((index + 2) * 10);
      result.emplace(word, value);
      word.resize(word.size() - 1);
      result.emplace(word + "ieth", value);
    }
    result.emplace("hundredth", 100);
    for (std::size_t index = 0; index < kMagnitudes.size(); ++index) {
      auto word = lowercase_ascii(std::string{kMagnitudes[index]});
      const auto value = std::pow(10.0, static_cast<double>((index + 1) * 3));
      result.emplace(word, value);
      result.emplace(word + "th", value);
    }
    return result;
  }();
  return values;
}

[[nodiscard]] auto words_to_number(std::string text) -> double {
  text = lowercase_ascii(std::move(text));
  for (auto &character : text) {
    if (character == ',' || character == '-') {
      character = ' ';
    }
  }
  std::vector<double> segments{0};
  std::size_t begin = 0;
  while (begin < text.size()) {
    while (begin < text.size() && text[begin] == ' ') {
      ++begin;
    }
    if (begin >= text.size()) {
      break;
    }
    const auto end = text.find(' ', begin);
    const auto token = text.substr(begin, end - begin);
    begin = end == std::string::npos ? text.size() : end + 1;
    if (token == "and") {
      continue;
    }
    const auto found = word_values().find(token);
    if (found == word_values().end()) {
      continue;
    }
    const auto value = found->second;
    if (value < 100) {
      auto top = segments.back();
      segments.pop_back();
      if (top >= 1000) {
        segments.push_back(top);
        top = 0;
      }
      segments.push_back(top + value);
    } else {
      segments.back() *= value;
    }
  }
  return std::accumulate(segments.begin(), segments.end(), 0.0);
}

constexpr std::array<std::pair<int, std::string_view>, 13> kRomanNumerals{{
    {1000, "m"},
    {900, "cm"},
    {500, "d"},
    {400, "cd"},
    {100, "c"},
    {90, "xc"},
    {50, "l"},
    {40, "xl"},
    {10, "x"},
    {9, "ix"},
    {5, "v"},
    {4, "iv"},
    {1, "i"},
}};

[[nodiscard]] auto decimal_to_roman(double value) -> std::string {
  std::string result;
  for (const auto &[amount, numeral] : kRomanNumerals) {
    while (value >= amount) {
      result += numeral;
      value -= amount;
    }
  }
  return result;
}

[[nodiscard]] auto roman_to_decimal(std::string roman) -> double {
  roman = uppercase_ascii(std::move(roman));
  const auto roman_value = [](char character) {
    switch (character) {
    case 'M':
      return 1000;
    case 'D':
      return 500;
    case 'C':
      return 100;
    case 'L':
      return 50;
    case 'X':
      return 10;
    case 'V':
      return 5;
    case 'I':
      return 1;
    default:
      return 0;
    }
  };
  double result = 0;
  int maximum = 1;
  for (auto iterator = roman.rbegin(); iterator != roman.rend(); ++iterator) {
    const auto value = roman_value(*iterator);
    if (value < maximum) {
      result -= value;
    } else {
      maximum = value;
      result += value;
    }
  }
  return result;
}

[[nodiscard]] auto decimal_to_letters(double value, char first) -> std::string {
  std::string result;
  while (value > 0) {
    const auto remainder = static_cast<int>(std::fmod(value - 1, 26));
    result.insert(result.begin(), static_cast<char>(first + remainder));
    value = std::floor((value - 1) / 26);
  }
  return result;
}

[[nodiscard]] auto letters_to_decimal(std::string_view letters, char first)
    -> double {
  double result = 0;
  double factor = 1;
  for (auto iterator = letters.rbegin(); iterator != letters.rend();
       ++iterator) {
    result += (*iterator - first + 1) * factor;
    factor *= 26;
  }
  return result;
}

[[nodiscard]] auto analyse_integer_picture(std::string_view picture,
                                           std::string_view source,
                                           std::size_t byte_offset)
    -> Result<IntegerPicture> {
  IntegerPicture result;
  auto primary = picture;
  if (const auto semicolon = picture.rfind(';');
      semicolon != std::string_view::npos) {
    primary = picture.substr(0, semicolon);
    const auto modifier = picture.substr(semicolon + 1);
    result.ordinal = modifier.starts_with('o');
  }
  if (primary == "A" || primary == "a") {
    result.primary = IntegerPrimary::Letters;
    result.letter_case = primary == "A" ? LetterCase::Upper : LetterCase::Lower;
    return result;
  }
  if (primary == "I" || primary == "i") {
    result.primary = IntegerPrimary::Roman;
    result.letter_case = primary == "I" ? LetterCase::Upper : LetterCase::Lower;
    return result;
  }
  if (primary == "W" || primary == "Ww" || primary == "w") {
    result.primary = IntegerPrimary::Words;
    result.letter_case = primary == "W"    ? LetterCase::Upper
                         : primary == "Ww" ? LetterCase::Title
                                           : LetterCase::Lower;
    return result;
  }

  std::size_t separator_position = 0;
  std::optional<std::uint32_t> zero_code;
  auto characters = unicode_characters(primary);
  std::ranges::reverse(characters);
  for (const auto &character : characters) {
    const auto codepoint = decode_utf8_codepoint(character);
    bool digit = false;
    for (const auto group : kDecimalGroups) {
      if (codepoint >= group && codepoint <= group + 9) {
        digit = true;
        ++result.mandatory_digits;
        ++separator_position;
        if (!zero_code) {
          zero_code = group;
        } else if (*zero_code != group) {
          return std::unexpected(dynamic_failure(
              "D3131", "Integer picture mixes decimal digit families", source,
              byte_offset));
        }
        break;
      }
    }
    if (digit) {
      continue;
    }
    if (codepoint == 0x23) {
      ++separator_position;
      ++result.optional_digits;
      continue;
    }
    result.separators.push_back(GroupingSeparator{
        .position = separator_position,
        .character = character,
    });
  }

  if (result.mandatory_digits == 0) {
    result.primary = IntegerPrimary::Sequence;
    result.token = std::string{primary};
    return result;
  }
  result.primary = IntegerPrimary::Decimal;
  result.zero_code = *zero_code;
  if (!result.separators.empty()) {
    const auto character = result.separators.front().character;
    const bool same_character = std::ranges::all_of(
        result.separators, [&](const GroupingSeparator &separator) {
          return separator.character == character;
        });
    if (same_character) {
      auto factor = result.separators.front().position;
      for (const auto &separator : result.separators) {
        factor = std::gcd(factor, separator.position);
      }
      bool regular = factor != 0;
      for (std::size_t index = 1; index <= result.separators.size(); ++index) {
        regular &= std::ranges::any_of(
            result.separators, [&](const GroupingSeparator &separator) {
              return separator.position == index * factor;
            });
      }
      if (regular) {
        result.regular = true;
        result.regular_position = factor;
        result.regular_character = character;
      }
    }
  }
  return result;
}

[[nodiscard]] auto map_ascii_digits(std::string_view digits,
                                    std::uint32_t zero_code) -> std::string {
  if (zero_code == 0x30) {
    return std::string{digits};
  }
  std::string result;
  for (const auto digit : digits) {
    result += encode_utf8(zero_code + static_cast<std::uint32_t>(digit - '0'));
  }
  return result;
}

[[nodiscard]] auto format_integer(double value, const IntegerPicture &format,
                                  std::string_view source,
                                  std::size_t byte_offset)
    -> Result<std::string> {
  const bool negative = value < 0;
  value = std::abs(value);
  std::string result;
  switch (format.primary) {
  case IntegerPrimary::Letters:
    result = decimal_to_letters(
        value, format.letter_case == LetterCase::Upper ? 'A' : 'a');
    break;
  case IntegerPrimary::Roman:
    result = decimal_to_roman(value);
    if (format.letter_case == LetterCase::Upper) {
      result = uppercase_ascii(std::move(result));
    }
    break;
  case IntegerPrimary::Words:
    result = number_to_words(value, format.ordinal);
    if (format.letter_case == LetterCase::Upper) {
      result = uppercase_ascii(std::move(result));
    } else if (format.letter_case == LetterCase::Lower) {
      result = lowercase_ascii(std::move(result));
    }
    break;
  case IntegerPrimary::Decimal: {
    auto ascii = std::format("{:.0f}", value);
    if (ascii.size() < format.mandatory_digits) {
      ascii.insert(0, format.mandatory_digits - ascii.size(), '0');
    }
    auto characters =
        unicode_characters(map_ascii_digits(ascii, format.zero_code));
    if (format.regular) {
      const auto count = (characters.size() - 1) / format.regular_position;
      for (std::size_t index = count; index > 0; --index) {
        const auto position =
            characters.size() - index * format.regular_position;
        characters.insert(characters.begin() +
                              static_cast<std::ptrdiff_t>(position),
                          format.regular_character);
      }
    } else {
      auto separators = format.separators;
      std::ranges::reverse(separators);
      for (const auto &separator : separators) {
        const auto position = characters.size() > separator.position
                                  ? characters.size() - separator.position
                                  : 0;
        characters.insert(characters.begin() +
                              static_cast<std::ptrdiff_t>(position),
                          separator.character);
      }
    }
    for (const auto &character : characters) {
      result += character;
    }
    if (format.ordinal) {
      const auto integer = static_cast<std::uint64_t>(std::fmod(value, 100));
      const auto last = integer % 10;
      result += integer >= 10 && integer <= 19 ? "th"
                : last == 1                    ? "st"
                : last == 2                    ? "nd"
                : last == 3                    ? "rd"
                                               : "th";
    }
    break;
  }
  case IntegerPrimary::Sequence:
    return std::unexpected(
        dynamic_failure("D3130", "Numbering sequence is not supported", source,
                        byte_offset, format.token));
  }
  if (negative) {
    result.insert(result.begin(), '-');
  }
  return result;
}

[[nodiscard]] auto option_string(const Object *options, std::string_view key,
                                 std::string fallback) -> std::string {
  if (options == nullptr) {
    return fallback;
  }
  const auto value = object_lookup(*options, key);
  if (!value) {
    return fallback;
  }
  const auto normalized = normalize_sequence(*value);
  if (const auto *text = std::get_if<std::string>(&normalized.storage)) {
    return *text;
  }
  return fallback;
}

[[nodiscard]] auto decimal_symbols(const Object *options) -> DecimalSymbols {
  DecimalSymbols result;
  result.decimal_separator =
      option_string(options, "decimal-separator", result.decimal_separator);
  result.grouping_separator =
      option_string(options, "grouping-separator", result.grouping_separator);
  result.exponent_separator =
      option_string(options, "exponent-separator", result.exponent_separator);
  result.infinity = option_string(options, "infinity", result.infinity);
  result.minus_sign = option_string(options, "minus-sign", result.minus_sign);
  result.nan = option_string(options, "NaN", result.nan);
  result.percent = option_string(options, "percent", result.percent);
  result.per_mille = option_string(options, "per-mille", result.per_mille);
  result.zero_digit = option_string(options, "zero-digit", result.zero_digit);
  result.optional_digit =
      option_string(options, "digit", result.optional_digit);
  result.pattern_separator =
      option_string(options, "pattern-separator", result.pattern_separator);
  result.zero_code = decode_utf8_codepoint(result.zero_digit);
  for (std::uint32_t offset = 0; offset < 10; ++offset) {
    result.digit_family.push_back(encode_utf8(result.zero_code + offset));
  }
  return result;
}

[[nodiscard]] auto join_characters(const std::vector<std::string> &characters,
                                   std::size_t begin, std::size_t end)
    -> std::string {
  std::string result;
  for (std::size_t index = begin; index < end; ++index) {
    result += characters[index];
  }
  return result;
}

[[nodiscard]] auto split_exact(std::string_view input,
                               std::string_view separator)
    -> std::vector<std::string> {
  std::vector<std::string> result;
  if (separator.empty()) {
    result.emplace_back(input);
    return result;
  }
  std::size_t begin = 0;
  while (true) {
    const auto position = input.find(separator, begin);
    if (position == std::string_view::npos) {
      result.emplace_back(input.substr(begin));
      break;
    }
    result.emplace_back(input.substr(begin, position - begin));
    begin = position + separator.size();
  }
  return result;
}

[[nodiscard]] auto count_token(std::string_view input, std::string_view token)
    -> std::size_t {
  if (token.empty()) {
    return 0;
  }
  std::size_t count = 0;
  std::size_t begin = 0;
  while (true) {
    const auto position = input.find(token, begin);
    if (position == std::string_view::npos) {
      break;
    }
    ++count;
    begin = position + token.size();
  }
  return count;
}

[[nodiscard]] auto is_decimal_digit(std::string_view character,
                                    const DecimalSymbols &symbols) -> bool {
  return std::ranges::find(symbols.digit_family, character) !=
         symbols.digit_family.end();
}

[[nodiscard]] auto is_active_character(std::string_view character,
                                       const DecimalSymbols &symbols) -> bool {
  return is_decimal_digit(character, symbols) ||
         character == symbols.decimal_separator ||
         character == symbols.exponent_separator ||
         character == symbols.grouping_separator ||
         character == symbols.optional_digit ||
         character == symbols.pattern_separator;
}

[[nodiscard]] auto split_number_subpicture(std::string subpicture,
                                           const DecimalSymbols &symbols)
    -> NumberSubpicture {
  NumberSubpicture result;
  result.picture = subpicture;
  const auto characters = unicode_characters(subpicture);
  std::size_t first_active = 0;
  bool found_first = false;
  for (std::size_t index = 0; index < characters.size(); ++index) {
    if (is_active_character(characters[index], symbols) &&
        characters[index] != symbols.exponent_separator) {
      first_active = index;
      found_first = true;
      break;
    }
  }
  std::size_t last_active = characters.empty() ? 0 : characters.size() - 1;
  bool found_last = false;
  for (std::size_t index = characters.size(); index > 0; --index) {
    if (is_active_character(characters[index - 1], symbols) &&
        characters[index - 1] != symbols.exponent_separator) {
      last_active = index - 1;
      found_last = true;
      break;
    }
  }
  if (found_first && found_last) {
    result.prefix = join_characters(characters, 0, first_active);
    result.suffix =
        join_characters(characters, last_active + 1, characters.size());
    result.active = join_characters(characters, first_active, last_active + 1);
  } else {
    result.active = subpicture;
  }

  const auto exponent_position = result.active.find(symbols.exponent_separator);
  if (exponent_position == std::string::npos) {
    result.mantissa = result.active;
  } else {
    result.mantissa = result.active.substr(0, exponent_position);
    result.exponent = result.active.substr(exponent_position +
                                           symbols.exponent_separator.size());
  }
  const auto decimal_position = result.mantissa.find(symbols.decimal_separator);
  if (decimal_position == std::string::npos) {
    result.integer = result.mantissa;
    result.fractional = result.suffix;
  } else {
    result.integer = result.mantissa.substr(0, decimal_position);
    result.fractional = result.mantissa.substr(
        decimal_position + symbols.decimal_separator.size());
  }
  return result;
}

[[nodiscard]] auto validate_number_subpicture(const NumberSubpicture &parts,
                                              const DecimalSymbols &symbols,
                                              std::string_view source,
                                              std::size_t byte_offset)
    -> Result<void> {
  std::string error;
  if (count_token(parts.picture, symbols.decimal_separator) > 1) {
    error = "D3081";
  }
  if (count_token(parts.picture, symbols.percent) > 1) {
    error = "D3082";
  }
  if (count_token(parts.picture, symbols.per_mille) > 1) {
    error = "D3083";
  }
  if (parts.picture.contains(symbols.percent) &&
      parts.picture.contains(symbols.per_mille)) {
    error = "D3084";
  }
  const auto mantissa_characters = unicode_characters(parts.mantissa);
  if (!std::ranges::any_of(mantissa_characters, [&](const auto &character) {
        return is_decimal_digit(character, symbols) ||
               character == symbols.optional_digit;
      })) {
    error = "D3085";
  }
  if (std::ranges::any_of(unicode_characters(parts.active),
                          [&](const auto &character) {
                            return !is_active_character(character, symbols);
                          })) {
    error = "D3086";
  }
  const auto decimal_position = parts.picture.find(symbols.decimal_separator);
  if (decimal_position != std::string::npos) {
    if ((decimal_position >= symbols.grouping_separator.size() &&
         parts.picture.substr(decimal_position -
                                  symbols.grouping_separator.size(),
                              symbols.grouping_separator.size()) ==
             symbols.grouping_separator) ||
        parts.picture.substr(
            decimal_position + symbols.decimal_separator.size(),
            symbols.grouping_separator.size()) == symbols.grouping_separator) {
      error = "D3087";
    }
  } else if (parts.integer.ends_with(symbols.grouping_separator)) {
    error = "D3088";
  }
  if (parts.picture.contains(symbols.grouping_separator +
                             symbols.grouping_separator)) {
    error = "D3089";
  }

  const auto integer_characters = unicode_characters(parts.integer);
  const auto optional_integer =
      std::ranges::find(integer_characters, symbols.optional_digit);
  if (optional_integer != integer_characters.end() &&
      std::ranges::any_of(integer_characters.begin(), optional_integer,
                          [&](const auto &character) {
                            return is_decimal_digit(character, symbols);
                          })) {
    error = "D3090";
  }
  const auto fractional_characters = unicode_characters(parts.fractional);
  const auto optional_fractional =
      std::ranges::find_last(fractional_characters, symbols.optional_digit);
  if (!optional_fractional.empty() &&
      std::ranges::any_of(optional_fractional.begin(),
                          fractional_characters.end(),
                          [&](const auto &character) {
                            return is_decimal_digit(character, symbols);
                          })) {
    error = "D3091";
  }
  if (parts.exponent && !parts.exponent->empty() &&
      (parts.picture.contains(symbols.percent) ||
       parts.picture.contains(symbols.per_mille))) {
    error = "D3092";
  }
  if (parts.exponent) {
    const auto exponent_characters = unicode_characters(*parts.exponent);
    if (exponent_characters.empty() ||
        std::ranges::any_of(exponent_characters, [&](const auto &character) {
          return !is_decimal_digit(character, symbols);
        })) {
      error = "D3093";
    }
  }
  if (!error.empty()) {
    return std::unexpected(dynamic_failure(std::move(error),
                                           "Invalid format-number picture",
                                           source, byte_offset));
  }
  return {};
}

[[nodiscard]] auto grouping_positions(std::string_view part, bool left,
                                      const DecimalSymbols &symbols)
    -> std::vector<std::size_t> {
  const auto characters = unicode_characters(part);
  std::vector<std::size_t> result;
  for (std::size_t index = 0; index < characters.size(); ++index) {
    if (characters[index] != symbols.grouping_separator) {
      continue;
    }
    const auto begin = left ? std::size_t{0} : index;
    const auto end = left ? index : characters.size();
    result.push_back(static_cast<std::size_t>(std::ranges::count_if(
        characters.begin() + static_cast<std::ptrdiff_t>(begin),
        characters.begin() + static_cast<std::ptrdiff_t>(end),
        [&](const auto &character) {
          return is_decimal_digit(character, symbols) ||
                 character == symbols.optional_digit;
        })));
  }
  return result;
}

auto analyse_number_subpicture(NumberSubpicture &parts,
                               const DecimalSymbols &symbols) -> void {
  parts.integer_grouping_positions =
      grouping_positions(parts.integer, false, symbols);
  if (!parts.integer_grouping_positions.empty()) {
    auto factor = parts.integer_grouping_positions.front();
    for (const auto position : parts.integer_grouping_positions) {
      factor = std::gcd(factor, position);
    }
    bool regular = factor != 0;
    for (std::size_t index = 1;
         index <= parts.integer_grouping_positions.size(); ++index) {
      regular &=
          std::ranges::find(parts.integer_grouping_positions, index * factor) !=
          parts.integer_grouping_positions.end();
    }
    parts.regular_grouping = regular ? factor : 0;
  }
  parts.fractional_grouping_positions =
      grouping_positions(parts.fractional, true, symbols);
  const auto integer_characters = unicode_characters(parts.integer);
  parts.minimum_integer_size = static_cast<std::size_t>(
      std::ranges::count_if(integer_characters, [&](const auto &character) {
        return is_decimal_digit(character, symbols);
      }));
  parts.scaling_factor = parts.minimum_integer_size;
  const auto fractional_characters = unicode_characters(parts.fractional);
  parts.minimum_fractional_size = static_cast<std::size_t>(
      std::ranges::count_if(fractional_characters, [&](const auto &character) {
        return is_decimal_digit(character, symbols);
      }));
  parts.maximum_fractional_size = static_cast<std::size_t>(
      std::ranges::count_if(fractional_characters, [&](const auto &character) {
        return is_decimal_digit(character, symbols) ||
               character == symbols.optional_digit;
      }));
  if (parts.minimum_integer_size == 0 && parts.maximum_fractional_size == 0) {
    if (parts.exponent) {
      parts.minimum_fractional_size = 1;
      parts.maximum_fractional_size = 1;
    } else {
      parts.minimum_integer_size = 1;
    }
  }
  if (parts.exponent && parts.minimum_integer_size == 0 &&
      parts.integer.contains(symbols.optional_digit)) {
    parts.minimum_integer_size = 1;
  }
  if (parts.minimum_integer_size == 0 && parts.minimum_fractional_size == 0) {
    parts.minimum_fractional_size = 1;
  }
  if (parts.exponent) {
    parts.minimum_exponent_size =
        static_cast<std::size_t>(std::ranges::count_if(
            unicode_characters(*parts.exponent), [&](const auto &character) {
              return is_decimal_digit(character, symbols);
            }));
  }
}

[[nodiscard]] auto half_even_round(double value, std::size_t precision)
    -> double {
  const auto factor = std::pow(10.0, static_cast<double>(precision));
  return std::nearbyint(value * factor) / factor;
}

[[nodiscard]] auto fixed_decimal(double value, std::size_t precision)
    -> std::string {
  return std::format("{:.{}f}", std::abs(value), precision);
}

[[nodiscard]] auto map_number_digits(std::string_view input,
                                     const DecimalSymbols &symbols)
    -> std::vector<std::string> {
  std::vector<std::string> result;
  for (const auto &character : unicode_characters(input)) {
    if (character.size() == 1 && character[0] >= '0' && character[0] <= '9') {
      result.push_back(
          symbols.digit_family[static_cast<std::size_t>(character[0] - '0')]);
    } else if (character == ".") {
      result.push_back(symbols.decimal_separator);
    } else {
      result.push_back(character);
    }
  }
  return result;
}

[[nodiscard]] auto format_number(double value, std::string_view picture,
                                 const DecimalSymbols &symbols,
                                 std::string_view source,
                                 std::size_t byte_offset)
    -> Result<std::string> {
  auto subpictures = split_exact(picture, symbols.pattern_separator);
  if (subpictures.size() > 2) {
    return std::unexpected(dynamic_failure(
        "D3080", "Too many format-number subpictures", source, byte_offset));
  }
  std::vector<NumberSubpicture> analysed;
  for (auto &subpicture : subpictures) {
    auto parts = split_number_subpicture(std::move(subpicture), symbols);
    auto valid =
        validate_number_subpicture(parts, symbols, source, byte_offset);
    if (!valid) {
      return std::unexpected(valid.error());
    }
    analyse_number_subpicture(parts, symbols);
    analysed.push_back(std::move(parts));
  }
  if (analysed.size() == 1) {
    analysed.push_back(analysed.front());
    analysed.back().prefix = symbols.minus_sign + analysed.back().prefix;
  }
  auto &selected = value >= 0 ? analysed.front() : analysed.back();
  auto adjusted = value;
  if (selected.picture.contains(symbols.percent)) {
    adjusted *= 100;
  } else if (selected.picture.contains(symbols.per_mille)) {
    adjusted *= 1000;
  }

  double mantissa = adjusted;
  std::optional<int> exponent;
  if (selected.minimum_exponent_size > 0) {
    const auto scaling_factor = static_cast<double>(selected.scaling_factor);
    const auto maximum = std::pow(10.0, scaling_factor);
    const auto minimum = std::pow(10.0, scaling_factor - 1.0);
    exponent = 0;
    if (mantissa != 0) {
      while (std::abs(mantissa) < minimum) {
        mantissa *= 10;
        --*exponent;
      }
      while (std::abs(mantissa) > maximum) {
        mantissa /= 10;
        ++*exponent;
      }
    }
  }
  const auto rounded =
      half_even_round(mantissa, selected.maximum_fractional_size);
  auto characters = map_number_digits(
      fixed_decimal(rounded, selected.maximum_fractional_size), symbols);
  auto decimal = std::ranges::find(characters, symbols.decimal_separator);
  if (decimal == characters.end()) {
    characters.push_back(symbols.decimal_separator);
  }
  while (!characters.empty() && characters.front() == symbols.zero_digit) {
    characters.erase(characters.begin());
  }
  while (!characters.empty() && characters.back() == symbols.zero_digit) {
    characters.pop_back();
  }
  decimal = std::ranges::find(characters, symbols.decimal_separator);
  auto decimal_index =
      static_cast<std::size_t>(std::distance(characters.begin(), decimal));
  if (decimal == characters.end()) {
    characters.push_back(symbols.decimal_separator);
    decimal_index = characters.size() - 1;
  }
  if (decimal_index < selected.minimum_integer_size) {
    characters.insert(characters.begin(),
                      selected.minimum_integer_size - decimal_index,
                      symbols.zero_digit);
  }
  decimal = std::ranges::find(characters, symbols.decimal_separator);
  decimal_index =
      static_cast<std::size_t>(std::distance(characters.begin(), decimal));
  const auto existing_fractional = characters.size() - decimal_index - 1;
  if (existing_fractional < selected.minimum_fractional_size) {
    characters.insert(characters.end(),
                      selected.minimum_fractional_size - existing_fractional,
                      symbols.zero_digit);
  }

  decimal_index = static_cast<std::size_t>(
      std::distance(characters.begin(),
                    std::ranges::find(characters, symbols.decimal_separator)));
  if (selected.regular_grouping > 0) {
    const auto groups = decimal_index == 0
                            ? 0
                            : (decimal_index - 1) / selected.regular_grouping;
    for (std::size_t group = 1; group <= groups; ++group) {
      characters.insert(
          characters.begin() +
              static_cast<std::ptrdiff_t>(decimal_index -
                                          group * selected.regular_grouping),
          symbols.grouping_separator);
    }
  } else {
    for (const auto position : selected.integer_grouping_positions) {
      if (position <= decimal_index) {
        characters.insert(characters.begin() + static_cast<std::ptrdiff_t>(
                                                   decimal_index - position),
                          symbols.grouping_separator);
        ++decimal_index;
      }
    }
  }
  decimal_index = static_cast<std::size_t>(
      std::distance(characters.begin(),
                    std::ranges::find(characters, symbols.decimal_separator)));
  std::size_t inserted = 0;
  for (const auto position : selected.fractional_grouping_positions) {
    const auto target = decimal_index + 1 + position + inserted;
    if (target <= characters.size()) {
      characters.insert(characters.begin() +
                            static_cast<std::ptrdiff_t>(target),
                        symbols.grouping_separator);
      ++inserted;
    }
  }
  auto decimal_iterator =
      std::ranges::find(characters, symbols.decimal_separator);
  if (!selected.picture.contains(symbols.decimal_separator) ||
      decimal_iterator == characters.end() - 1) {
    if (decimal_iterator != characters.end()) {
      characters.erase(decimal_iterator);
    }
  }

  std::string result = selected.prefix;
  for (const auto &character : characters) {
    result += character;
  }
  if (exponent) {
    auto exponent_characters = map_number_digits(
        fixed_decimal(static_cast<double>(*exponent), 0), symbols);
    if (exponent_characters.size() < selected.minimum_exponent_size) {
      exponent_characters.insert(exponent_characters.begin(),
                                 selected.minimum_exponent_size -
                                     exponent_characters.size(),
                                 symbols.zero_digit);
    }
    result += symbols.exponent_separator;
    if (*exponent < 0) {
      result += symbols.minus_sign;
    }
    for (const auto &character : exponent_characters) {
      result += character;
    }
  }
  result += selected.suffix;
  return result;
}

} // namespace

auto format_integer_picture(double value, std::string_view picture,
                            std::string_view source, std::size_t byte_offset)
    -> Result<std::string> {
  auto format = analyse_integer_picture(picture, source, byte_offset);
  if (!format) {
    return std::unexpected(format.error());
  }
  return format_integer(std::floor(value), *format, source, byte_offset);
}

auto parse_integer_picture(std::string_view value, std::string_view picture,
                           std::string_view source, std::size_t byte_offset)
    -> Result<double> {
  auto format = analyse_integer_picture(picture, source, byte_offset);
  if (!format) {
    return std::unexpected(format.error());
  }
  switch (format->primary) {
  case IntegerPrimary::Letters:
    return letters_to_decimal(
        value, format->letter_case == LetterCase::Upper ? 'A' : 'a');
  case IntegerPrimary::Roman:
    return roman_to_decimal(std::string{value});
  case IntegerPrimary::Words:
    return words_to_number(std::string{value});
  case IntegerPrimary::Sequence:
    return std::unexpected(
        dynamic_failure("D3130", "Numbering sequence is not supported", source,
                        byte_offset, format->token));
  case IntegerPrimary::Decimal:
    break;
  }

  auto characters = unicode_characters(value);
  if (format->ordinal && characters.size() >= 2) {
    characters.resize(characters.size() - 2);
  }
  std::string digits;
  for (const auto &character : characters) {
    bool separator = false;
    for (const auto &item : format->separators) {
      separator |= character == item.character;
    }
    if (separator) {
      continue;
    }
    const auto codepoint = decode_utf8_codepoint(character);
    if (codepoint >= format->zero_code && codepoint <= format->zero_code + 9) {
      digits.push_back(static_cast<char>('0' + codepoint - format->zero_code));
    } else if (character == "-") {
      digits.push_back('-');
    }
  }
  double result = 0;
  const auto [end, error] =
      std::from_chars(digits.data(), digits.data() + digits.size(), result,
                      std::chars_format::general);
  if (digits.empty()) {
    return 0.0;
  }
  if (error != std::errc{} || end != digits.data() + digits.size()) {
    return std::unexpected(dynamic_failure("D3134", "Unable to parse integer",
                                           source, byte_offset));
  }
  return result;
}

auto format_number_picture(double value, std::string_view picture,
                           const Object *options, std::string_view source,
                           std::size_t byte_offset) -> Result<std::string> {
  return format_number(value, picture, decimal_symbols(options), source,
                       byte_offset);
}

namespace {

enum class NameCase : std::uint8_t { Lower, Upper, Title };

struct DateWidth {
  std::optional<std::size_t> minimum;
  std::optional<std::size_t> maximum;
};

struct DateMarker {
  char component{};
  std::string presentation;
  std::optional<char> presentation2;
  std::optional<NameCase> names;
  std::optional<DateWidth> width;
  std::optional<IntegerPicture> integer;
  std::optional<std::size_t> parse_width;
  int year_digits{-1};
};

struct DatePart {
  std::string literal;
  std::optional<DateMarker> marker;
};

struct DatePicture {
  std::vector<DatePart> parts;
};

constexpr std::array<std::string_view, 12> kMonths{
    "January", "February", "March",     "April",   "May",      "June",
    "July",    "August",   "September", "October", "November", "December"};
constexpr std::array<std::string_view, 8> kDays{
    "",         "Monday", "Tuesday",  "Wednesday",
    "Thursday", "Friday", "Saturday", "Sunday"};

[[nodiscard]] auto default_presentation(char component)
    -> std::optional<std::string_view> {
  switch (component) {
  case 'Y':
  case 'M':
  case 'D':
  case 'd':
  case 'W':
  case 'w':
  case 'X':
  case 'x':
  case 'H':
  case 'h':
  case 'f':
    return "1";
  case 'F':
  case 'P':
  case 'C':
  case 'E':
    return "n";
  case 'm':
  case 's':
    return "01";
  case 'Z':
  case 'z':
    return "01:01";
  default:
    return std::nullopt;
  }
}

[[nodiscard]] auto parse_width_value(std::string_view value)
    -> std::optional<std::size_t> {
  if (value.empty() || value == "*") {
    return std::nullopt;
  }
  std::size_t parsed{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), parsed);
  return error == std::errc{} && end == value.data() + value.size()
             ? std::optional<std::size_t>{parsed}
             : std::nullopt;
}

[[nodiscard]] auto analyse_datetime_picture(std::string_view picture,
                                            std::string_view source,
                                            std::size_t byte_offset)
    -> Result<DatePicture> {
  DatePicture result;
  const auto add_literal = [&](std::size_t begin, std::size_t end) {
    if (end <= begin) {
      return;
    }
    std::string literal{picture.substr(begin, end - begin)};
    for (std::size_t position = 0;
         (position = literal.find("]]", position)) != std::string::npos;) {
      literal.replace(position, 2, "]");
      ++position;
    }
    result.parts.push_back(DatePart{.literal = std::move(literal)});
  };

  std::size_t start = 0;
  std::size_t position = 0;
  while (position < picture.size()) {
    if (picture[position] != '[') {
      ++position;
      continue;
    }
    if (position + 1 < picture.size() && picture[position + 1] == '[') {
      add_literal(start, position);
      result.parts.push_back(DatePart{.literal = "["});
      position += 2;
      start = position;
      continue;
    }
    add_literal(start, position);
    const auto close = picture.find(']', position + 1);
    if (close == std::string_view::npos) {
      return std::unexpected(
          dynamic_failure("D3135", "Date/time picture has an unclosed marker",
                          source, byte_offset));
    }
    std::string marker_text{picture.substr(position + 1, close - position - 1)};
    std::erase_if(marker_text, [](unsigned char character) {
      return std::isspace(character) != 0;
    });
    if (marker_text.empty()) {
      return std::unexpected(
          dynamic_failure("D3132", "Date/time picture has an empty marker",
                          source, byte_offset));
    }
    DateMarker marker{.component = marker_text.front()};
    const auto known = default_presentation(marker.component);
    if (!known) {
      return std::unexpected(
          dynamic_failure("D3132", "Unknown date/time component", source,
                          byte_offset, std::string(1, marker.component)));
    }

    std::string presentation;
    const auto comma = marker_text.rfind(',');
    if (comma != std::string::npos) {
      const auto width_text = std::string_view{marker_text}.substr(comma + 1);
      const auto dash = width_text.find('-');
      DateWidth width;
      if (dash == std::string_view::npos) {
        width.minimum = parse_width_value(width_text);
      } else {
        width.minimum = parse_width_value(width_text.substr(0, dash));
        width.maximum = parse_width_value(width_text.substr(dash + 1));
      }
      marker.width = width;
      presentation = marker_text.substr(1, comma - 1);
    } else {
      presentation = marker_text.substr(1);
    }
    if (presentation.empty()) {
      presentation = std::string{*known};
    } else if (presentation.size() > 1) {
      const auto modifier = presentation.back();
      if (std::string_view{"atco"}.contains(modifier)) {
        marker.presentation2 = modifier;
        presentation.pop_back();
      }
    }
    marker.presentation = presentation;
    if (!presentation.empty() && presentation.front() == 'n') {
      marker.names = NameCase::Lower;
    } else if (!presentation.empty() && presentation.front() == 'N') {
      marker.names = presentation.size() > 1 && presentation[1] == 'n'
                         ? NameCase::Title
                         : NameCase::Upper;
    }
    constexpr std::string_view kIntegerComponents{"YMDdFWwXxHhmsf"};
    if (kIntegerComponents.contains(marker.component) && !marker.names) {
      auto integer_picture = presentation;
      if (marker.presentation2) {
        integer_picture += ';';
        integer_picture.push_back(*marker.presentation2);
      }
      auto integer =
          analyse_integer_picture(integer_picture, source, byte_offset);
      if (!integer) {
        return std::unexpected(integer.error());
      }
      if (marker.width && marker.width->minimum &&
          integer->mandatory_digits < *marker.width->minimum) {
        integer->mandatory_digits = *marker.width->minimum;
      }
      if (marker.component == 'Y') {
        const auto width = integer->mandatory_digits + integer->optional_digits;
        if (marker.width && marker.width->maximum) {
          marker.year_digits = static_cast<int>(*marker.width->maximum);
          integer->mandatory_digits = *marker.width->maximum;
        } else if (width >= 2) {
          marker.year_digits = static_cast<int>(width);
        }
      }
      marker.integer = std::move(*integer);
    }
    if (marker.component == 'Z' || marker.component == 'z') {
      auto integer = analyse_integer_picture(presentation, source, byte_offset);
      if (!integer) {
        return std::unexpected(integer.error());
      }
      marker.integer = std::move(*integer);
    }

    if (!result.parts.empty() && result.parts.back().marker &&
        result.parts.back().marker->integer) {
      result.parts.back().marker->parse_width =
          result.parts.back().marker->integer->mandatory_digits;
    }

    result.parts.push_back(DatePart{.marker = std::move(marker)});
    position = close + 1;
    start = position;
  }
  add_literal(start, picture.size());
  return result;
}

struct DateFields {
  int year{};
  unsigned month{};
  unsigned day{};
  unsigned day_of_year{};
  unsigned weekday{};
  unsigned week_of_year{};
  unsigned week_of_month{};
  int iso_year{};
  unsigned iso_month{};
  unsigned hour{};
  unsigned minute{};
  unsigned second{};
  unsigned millisecond{};
};

[[nodiscard]] auto start_of_first_week(int year, unsigned month)
    -> std::chrono::sys_days {
  using namespace std::chrono;
  const auto first = sys_days{year_month_day{
      std::chrono::year{year}, std::chrono::month{month}, day{1}}};
  const auto weekday = std::chrono::weekday{first}.iso_encoding();
  return weekday > 4 ? first + days{8 - weekday} : first - days{weekday - 1};
}

[[nodiscard]] auto iso_week_fields(std::chrono::sys_days date)
    -> std::pair<int, unsigned> {
  using namespace std::chrono;
  const auto iso_day = weekday{date}.iso_encoding();
  const auto thursday = date + days{4 - iso_day};
  const auto iso_year = static_cast<int>(year_month_day{thursday}.year());
  const auto january_fourth =
      sys_days{year_month_day{year{iso_year}, month{1}, day{4}}};
  const auto week_one =
      january_fourth - days{weekday{january_fourth}.iso_encoding() - 1};
  const auto week = static_cast<unsigned>((date - week_one).count() / 7 + 1);
  return {iso_year, week};
}

[[nodiscard]] auto
date_fields(std::chrono::sys_time<std::chrono::milliseconds> time)
    -> DateFields {
  using namespace std::chrono;
  const auto date = floor<days>(time);
  const auto ymd = year_month_day{date};
  const auto daytime = hh_mm_ss{time - date};
  const auto year_value = static_cast<int>(ymd.year());
  const auto month_value = static_cast<unsigned>(ymd.month());
  const auto day_value = static_cast<unsigned>(ymd.day());
  const auto first_january =
      sys_days{year_month_day{ymd.year(), month{1}, day{1}}};
  const auto iso_day = weekday{date}.iso_encoding();
  const auto [iso_year, week_of_year] = iso_week_fields(date);

  auto month_week_start = start_of_first_week(year_value, month_value);
  auto week_of_month =
      static_cast<double>((date - month_week_start).count()) / 7.0 + 1.0;
  if (week_of_month > 4) {
    auto next = ymd.year() / ymd.month() + months{1};
    const auto next_start = start_of_first_week(
        static_cast<int>(next.year()), static_cast<unsigned>(next.month()));
    if (date >= next_start) {
      week_of_month = 1;
    }
  } else if (week_of_month < 1) {
    auto previous = ymd.year() / ymd.month() - months{1};
    const auto previous_start =
        start_of_first_week(static_cast<int>(previous.year()),
                            static_cast<unsigned>(previous.month()));
    week_of_month =
        static_cast<double>((date - previous_start).count()) / 7.0 + 1.0;
  }

  const auto current_month_start = start_of_first_week(year_value, month_value);
  auto next_month = ymd.year() / ymd.month() + months{1};
  const auto next_month_start =
      start_of_first_week(static_cast<int>(next_month.year()),
                          static_cast<unsigned>(next_month.month()));
  unsigned iso_month = month_value;
  if (date < current_month_start) {
    auto previous = ymd.year() / ymd.month() - months{1};
    iso_month = static_cast<unsigned>(previous.month());
  } else if (date >= next_month_start) {
    iso_month = static_cast<unsigned>(next_month.month());
  }

  return DateFields{
      .year = year_value,
      .month = month_value,
      .day = day_value,
      .day_of_year = static_cast<unsigned>((date - first_january).count() + 1),
      .weekday = iso_day,
      .week_of_year = week_of_year,
      .week_of_month = static_cast<unsigned>(std::floor(week_of_month)),
      .iso_year = iso_year,
      .iso_month = iso_month,
      .hour = static_cast<unsigned>(daytime.hours().count()),
      .minute = static_cast<unsigned>(daytime.minutes().count()),
      .second = static_cast<unsigned>(daytime.seconds().count()),
      .millisecond = static_cast<unsigned>(
          duration_cast<milliseconds>(daytime.subseconds()).count()),
  };
}

[[nodiscard]] auto marker_number(const DateMarker &marker,
                                 const DateFields &fields) -> double {
  switch (marker.component) {
  case 'Y':
    return fields.year;
  case 'M':
    return fields.month;
  case 'D':
    return fields.day;
  case 'd':
    return fields.day_of_year;
  case 'F':
    return fields.weekday;
  case 'W':
    return fields.week_of_year;
  case 'w':
    return fields.week_of_month;
  case 'X':
    return fields.iso_year;
  case 'x':
    return fields.iso_month;
  case 'H':
    return fields.hour;
  case 'h':
    return fields.hour % 12 == 0 ? 12 : fields.hour % 12;
  case 'm':
    return fields.minute;
  case 's':
    return fields.second;
  case 'f':
    return fields.millisecond;
  default:
    return 0;
  }
}

[[nodiscard]] auto timezone_offset(std::string_view timezone)
    -> std::pair<int, int> {
  if (timezone.empty()) {
    return {0, 0};
  }
  int sign = 1;
  std::size_t begin = 0;
  if (timezone.front() == '+' || timezone.front() == '-') {
    sign = timezone.front() == '-' ? -1 : 1;
    begin = 1;
  }
  std::string digits;
  for (std::size_t index = begin; index < timezone.size(); ++index) {
    if (std::isdigit(static_cast<unsigned char>(timezone[index])) != 0) {
      digits.push_back(timezone[index]);
    }
  }
  int hours = 0;
  int minutes = 0;
  if (digits.size() <= 2) {
    std::from_chars(digits.data(), digits.data() + digits.size(), hours);
  } else {
    const auto split = digits.size() - 2;
    std::from_chars(digits.data(), digits.data() + split, hours);
    std::from_chars(digits.data() + split, digits.data() + digits.size(),
                    minutes);
  }
  return {sign * hours, sign * minutes};
}

[[nodiscard]] auto apply_name_case(std::string value, NameCase name_case)
    -> std::string {
  if (name_case == NameCase::Upper) {
    return uppercase_ascii(std::move(value));
  }
  if (name_case == NameCase::Lower) {
    return lowercase_ascii(std::move(value));
  }
  return value;
}

[[nodiscard]] auto
format_date_marker(const DateMarker &marker, const DateFields &fields,
                   int offset_hours, int offset_minutes,
                   std::string_view source, std::size_t byte_offset)
    -> Result<std::string> {
  if (marker.component == 'C' || marker.component == 'E') {
    return std::string{"ISO"};
  }
  constexpr std::string_view kIntegerComponents{"YMDdFWwXxHhms"};
  if (kIntegerComponents.contains(marker.component)) {
    auto value = marker_number(marker, fields);
    if (marker.component == 'Y' && marker.year_digits >= 0) {
      value = std::fmod(value, std::pow(10.0, marker.year_digits));
    }
    if (marker.names) {
      std::string named;
      if (marker.component == 'M' || marker.component == 'x') {
        named = kMonths[static_cast<std::size_t>(value) - 1];
      } else if (marker.component == 'F') {
        named = kDays[static_cast<std::size_t>(value)];
      } else {
        return std::unexpected(dynamic_failure(
            "D3133", "Name presentation is invalid for this component", source,
            byte_offset, std::string(1, marker.component)));
      }
      named = apply_name_case(std::move(named), *marker.names);
      if (marker.width && marker.width->maximum) {
        auto characters = unicode_characters(named);
        if (characters.size() > *marker.width->maximum) {
          characters.resize(*marker.width->maximum);
          named.clear();
          for (const auto &character : characters) {
            named += character;
          }
        }
      }
      return named;
    }
    return format_integer(value, *marker.integer, source, byte_offset);
  }
  if (marker.component == 'f') {
    return format_integer(fields.millisecond, *marker.integer, source,
                          byte_offset);
  }
  if (marker.component == 'Z' || marker.component == 'z') {
    const auto signed_minutes = offset_hours * 60 + offset_minutes;
    if (signed_minutes == 0 && marker.presentation2 == 't') {
      return std::string{"Z"};
    }
    const auto sign = signed_minutes < 0 ? -1 : 1;
    const auto absolute_hours = std::abs(signed_minutes) / 60;
    const auto absolute_minutes = std::abs(signed_minutes) % 60;
    const auto combined = absolute_hours * 100 + absolute_minutes;
    std::string formatted;
    if (marker.integer->regular) {
      auto value =
          format_integer(combined, *marker.integer, source, byte_offset);
      if (!value) {
        return std::unexpected(value.error());
      }
      formatted = std::move(*value);
    } else {
      const auto digits = marker.integer->mandatory_digits;
      if (digits == 1 || digits == 2) {
        auto hour = format_integer(absolute_hours, *marker.integer, source,
                                   byte_offset);
        if (!hour) {
          return std::unexpected(hour.error());
        }
        formatted = std::move(*hour);
        if (absolute_minutes != 0) {
          auto minute = format_integer_picture(absolute_minutes, "00", source,
                                               byte_offset);
          if (!minute) {
            return std::unexpected(minute.error());
          }
          formatted += ':' + *minute;
        }
      } else if (digits == 3 || digits == 4) {
        auto value =
            format_integer(combined, *marker.integer, source, byte_offset);
        if (!value) {
          return std::unexpected(value.error());
        }
        formatted = std::move(*value);
      } else {
        return std::unexpected(
            dynamic_failure("D3134", "Unsupported timezone picture width",
                            source, byte_offset));
      }
    }
    formatted.insert(formatted.begin(), sign < 0 ? '-' : '+');
    if (marker.component == 'z') {
      formatted.insert(0, "GMT");
    }
    return formatted;
  }
  if (marker.component == 'P') {
    auto value = fields.hour >= 12 ? std::string{"pm"} : std::string{"am"};
    if (marker.names) {
      value = apply_name_case(std::move(value), *marker.names);
    }
    return value;
  }
  if (marker.component == 'C' || marker.component == 'E') {
    return std::string{"ISO"};
  }
  return std::unexpected(dynamic_failure("D3132", "Unknown date/time component",
                                         source, byte_offset));
}

} // namespace

auto format_datetime_picture(double millis, const std::string *picture,
                             const std::string *timezone,
                             std::string_view source, std::size_t byte_offset)
    -> Result<std::string> {
  static const std::string kDefaultPicture{
      "[Y0001]-[M01]-[D01]T[H01]:[m01]:[s01].[f001][Z01:01t]"};
  const auto &resolved_picture = picture ? *picture : kDefaultPicture;
  auto compiled =
      analyse_datetime_picture(resolved_picture, source, byte_offset);
  if (!compiled) {
    return std::unexpected(compiled.error());
  }
  const auto [offset_hours, offset_minutes] =
      timezone ? timezone_offset(*timezone) : std::pair{0, 0};
  const auto offset = std::chrono::minutes{offset_hours * 60 + offset_minutes};
  const auto time =
      std::chrono::sys_time<std::chrono::milliseconds>{
          std::chrono::milliseconds{static_cast<std::int64_t>(millis)}} +
      offset;
  const auto fields = date_fields(time);

  std::string result;
  for (const auto &part : compiled->parts) {
    if (!part.marker) {
      result += part.literal;
      continue;
    }
    auto formatted = format_date_marker(*part.marker, fields, offset_hours,
                                        offset_minutes, source, byte_offset);
    if (!formatted) {
      return std::unexpected(formatted.error());
    }
    result += *formatted;
  }
  return result;
}

namespace {

[[nodiscard]] auto regex_escape(std::string_view input) -> std::string {
  constexpr std::string_view kSpecial{".^$|()[]{}*+?\\"};
  std::string result;
  result.reserve(input.size() * 2);
  for (const auto character : input) {
    if (kSpecial.contains(character)) {
      result.push_back('\\');
    }
    result.push_back(character);
  }
  return result;
}

[[nodiscard]] auto integer_match_pattern(const DateMarker &marker)
    -> std::string {
  const auto &format = *marker.integer;
  switch (format.primary) {
  case IntegerPrimary::Letters:
    return format.letter_case == LetterCase::Upper ? "[A-Z]+" : "[a-z]+";
  case IntegerPrimary::Roman:
    return format.letter_case == LetterCase::Upper ? "[MDCLXVI]+"
                                                   : "[mdclxvi]+";
  case IntegerPrimary::Words:
    return "[A-Za-z]+(?:[\\s,-]+[A-Za-z]+)*?";
  case IntegerPrimary::Sequence:
    return ".+?";
  case IntegerPrimary::Decimal:
    break;
  }
  if (marker.parse_width && format.separators.empty() &&
      format.zero_code == 0x30) {
    return std::format("[0-9]{{{}}}{}", *marker.parse_width,
                       format.ordinal ? "(?:th|st|nd|rd)" : "");
  }
  std::string character_class{"0-9"};
  for (const auto &separator : format.separators) {
    character_class += regex_escape(separator.character);
  }
  return "[" + character_class + "]+" +
         (format.ordinal ? "(?:th|st|nd|rd)" : "");
}

[[nodiscard]] auto date_marker_pattern(const DateMarker &marker)
    -> std::string {
  if (marker.component == 'Z' || marker.component == 'z') {
    std::string pattern = marker.component == 'z' ? "GMT" : "";
    pattern += "[-+][0-9]+";
    if (marker.integer && marker.integer->regular &&
        !marker.integer->regular_character.empty()) {
      pattern += regex_escape(marker.integer->regular_character) + "[0-9]+";
    }
    return pattern;
  }
  if (marker.component == 'f') {
    return "[0-9]+";
  }
  if (marker.names || marker.component == 'P' || marker.component == 'C' ||
      marker.component == 'E') {
    return "[A-Za-z]+";
  }
  if (marker.integer) {
    return integer_match_pattern(marker);
  }
  return ".+?";
}

struct DateMatcher {
  std::shared_ptr<RegexValue> regex;
  std::vector<const DatePart *> parts;
};

[[nodiscard]] auto compile_date_matcher(const DatePicture &picture,
                                        std::string_view source,
                                        std::size_t byte_offset)
    -> Result<DateMatcher> {
  std::string pattern{"^"};
  DateMatcher result;
  result.parts.reserve(picture.parts.size());
  for (const auto &part : picture.parts) {
    pattern.push_back('(');
    pattern += part.marker ? date_marker_pattern(*part.marker)
                           : regex_escape(part.literal);
    pattern.push_back(')');
    result.parts.push_back(&part);
  }
  pattern.push_back('$');
  auto compiled = compile_regex(std::move(pattern), "i", source, byte_offset);
  if (!compiled) {
    return std::unexpected(compiled.error());
  }
  result.regex = std::move(*compiled);
  return result;
}

[[nodiscard]] auto integer_marker_picture(const DateMarker &marker)
    -> std::string {
  auto picture = marker.presentation;
  if (marker.presentation2) {
    picture += ';';
    picture.push_back(*marker.presentation2);
  }
  return picture;
}

[[nodiscard]] auto parse_named_component(const DateMarker &marker,
                                         std::string value)
    -> std::optional<double> {
  value = lowercase_ascii(std::move(value));
  if (marker.component == 'M' || marker.component == 'x') {
    for (std::size_t index = 0; index < kMonths.size(); ++index) {
      auto candidate = lowercase_ascii(std::string{kMonths[index]});
      if (marker.width && marker.width->maximum &&
          candidate.size() > *marker.width->maximum) {
        candidate.resize(*marker.width->maximum);
      }
      if (candidate == value) {
        return static_cast<double>(index + 1);
      }
    }
    return std::nullopt;
  }
  if (marker.component == 'F') {
    for (std::size_t index = 1; index < kDays.size(); ++index) {
      auto candidate = lowercase_ascii(std::string{kDays[index]});
      if (marker.width && marker.width->maximum &&
          candidate.size() > *marker.width->maximum) {
        candidate.resize(*marker.width->maximum);
      }
      if (candidate == value) {
        return static_cast<double>(index);
      }
    }
    return std::nullopt;
  }
  if (marker.component == 'P') {
    if (value == "am") {
      return 0.0;
    }
    if (value == "pm") {
      return 1.0;
    }
  }
  return std::nullopt;
}

[[nodiscard]] auto parse_timezone_component(std::string value) -> double {
  if (value.starts_with("GMT") || value.starts_with("gmt")) {
    value.erase(0, 3);
  }
  const auto [hours, minutes] = timezone_offset(value);
  return hours * 60.0 + minutes;
}

[[nodiscard]] auto parse_date_marker(const DateMarker &marker,
                                     std::string value, std::string_view source,
                                     std::size_t byte_offset)
    -> Result<std::optional<double>> {
  if (marker.component == 'C' || marker.component == 'E') {
    return std::optional<double>{};
  }
  if (marker.component == 'Z' || marker.component == 'z') {
    return std::optional<double>{parse_timezone_component(std::move(value))};
  }
  if (marker.component == 'f') {
    if (value.size() > 3) {
      value.resize(3);
    }
    while (value.size() < 3) {
      value.push_back('0');
    }
    double parsed{};
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), parsed);
    if (error != std::errc{} || end != value.data() + value.size()) {
      return std::optional<double>{};
    }
    return std::optional<double>{parsed};
  }
  if (marker.names || marker.component == 'P') {
    return parse_named_component(marker, std::move(value));
  }
  if (marker.integer) {
    auto parsed = parse_integer_picture(value, integer_marker_picture(marker),
                                        source, byte_offset);
    if (!parsed) {
      return std::unexpected(parsed.error());
    }
    return std::optional<double>{*parsed};
  }
  return std::optional<double>{};
}

[[nodiscard]] auto current_component(const DateFields &fields, char component)
    -> double {
  switch (component) {
  case 'Y':
    return fields.year;
  case 'X':
    return fields.iso_year;
  case 'M':
    return fields.month;
  case 'x':
    return fields.iso_month;
  case 'W':
    return fields.week_of_year;
  case 'w':
    return fields.week_of_month;
  case 'd':
    return fields.day_of_year;
  case 'D':
    return fields.day;
  case 'F':
    return fields.weekday;
  case 'P':
    return fields.hour >= 12 ? 1 : 0;
  case 'H':
    return fields.hour;
  case 'h':
    return fields.hour % 12 == 0 ? 12 : fields.hour % 12;
  case 'm':
    return fields.minute;
  case 's':
    return fields.second;
  case 'f':
    return fields.millisecond;
  default:
    return 0;
  }
}

[[nodiscard]] auto
components_mask(const std::unordered_map<char, double> &components,
                std::string_view order) -> unsigned {
  unsigned mask = 0;
  for (const auto component : order) {
    mask <<= 1U;
    mask += components.contains(component) ? 1U : 0U;
  }
  return mask;
}

[[nodiscard]] auto mask_is_type(unsigned mask, unsigned type) noexcept -> bool {
  return (mask & ~type) == 0U && (mask & type) != 0U;
}

[[nodiscard]] auto compose_datetime_millis(
    std::unordered_map<char, double> components,
    std::chrono::system_clock::time_point evaluation_timestamp,
    std::string_view source, std::size_t byte_offset)
    -> Result<std::optional<double>> {
  if (components.empty()) {
    return std::optional<double>{};
  }
  constexpr unsigned kDateA = 161;
  constexpr unsigned kDateB = 130;
  constexpr unsigned kDateC = 84;
  constexpr unsigned kDateD = 72;
  constexpr unsigned kTimeA = 23;
  constexpr unsigned kTimeB = 47;

  const auto date_mask = components_mask(components, "YXMxWwdD");
  const bool date_a = mask_is_type(date_mask, kDateA);
  const bool date_b = !date_a && mask_is_type(date_mask, kDateB);
  const bool date_c = mask_is_type(date_mask, kDateC);
  const bool date_d = !date_c && mask_is_type(date_mask, kDateD);
  const auto time_mask = components_mask(components, "PHhmsf");
  const bool time_a = mask_is_type(time_mask, kTimeA);
  const bool time_b = !time_a && mask_is_type(time_mask, kTimeB);

  const std::string date_components = date_b   ? "YD"
                                      : date_c ? "XxwF"
                                      : date_d ? "XWF"
                                               : "YMD";
  const std::string time_components = time_b ? "Phmsf" : "Hmsf";
  const auto now = date_fields(
      std::chrono::floor<std::chrono::milliseconds>(evaluation_timestamp));
  bool started = false;
  bool ended = false;
  for (const auto component : date_components + time_components) {
    if (!components.contains(component)) {
      if (started) {
        components[component] =
            std::string_view{"MDd"}.contains(component) ? 1.0 : 0.0;
        ended = true;
      } else {
        components[component] = current_component(now, component);
      }
    } else {
      started = true;
      if (ended) {
        return std::unexpected(
            dynamic_failure("D3136", "Date/time components contain a gap",
                            source, byte_offset));
      }
    }
  }
  if (date_c || date_d) {
    return std::unexpected(
        dynamic_failure("D3136", "ISO week-date parsing is not supported",
                        source, byte_offset));
  }

  using namespace std::chrono;
  const auto year_value = static_cast<int>(components['Y']);
  unsigned month_value = static_cast<unsigned>(components['M']);
  unsigned day_value = static_cast<unsigned>(components['D']);
  if (date_b) {
    const auto first =
        sys_days{year_month_day{year{year_value}, month{1}, day{1}}};
    const auto derived =
        year_month_day{first + days{static_cast<int>(components['d']) - 1}};
    month_value = static_cast<unsigned>(derived.month());
    day_value = static_cast<unsigned>(derived.day());
  }
  auto hour = static_cast<int>(components['H']);
  if (time_b) {
    hour = static_cast<int>(components['h']);
    hour = hour == 12 ? 0 : hour;
    if (components['P'] == 1) {
      hour += 12;
    }
  }
  const auto date =
      year_month_day{year{year_value}, month{month_value}, day{day_value}};
  if (!date.ok()) {
    return std::optional<double>{};
  }
  auto time = sys_time<milliseconds>{sys_days{date}.time_since_epoch()} +
              hours{hour} + minutes{static_cast<int>(components['m'])} +
              seconds{static_cast<int>(components['s'])} +
              milliseconds{static_cast<int>(components['f'])};
  if (components.contains('Z')) {
    time -= minutes{static_cast<int>(components['Z'])};
  } else if (components.contains('z')) {
    time -= minutes{static_cast<int>(components['z'])};
  }
  return std::optional<double>{
      static_cast<double>(time.time_since_epoch().count())};
}

[[nodiscard]] auto
search_datetime_regex(const RegexValue &regex, std::string_view input,
                      DateTimeRegexBudget *budget, std::string_view source,
                      std::size_t byte_offset)
    -> Result<std::optional<RegexMatch>> {
  auto match =
      search_regex(regex, input, 0, budget ? budget->limits : RegexLimits{},
                   source, byte_offset);
  if (!match) {
    return std::unexpected(match.error());
  }
  if (*match && budget && budget->matches &&
      ++*budget->matches > budget->max_matches) {
    return std::unexpected(
        host_failure("H2102", "Regular expression match count limit exceeded",
                     source, byte_offset));
  }
  return match;
}

[[nodiscard]] auto
parse_iso_datetime(std::string_view timestamp, std::string_view source,
                   std::size_t byte_offset, DateTimeRegexBudget *regex_budget)
    -> Result<double> {
  static constexpr std::string_view kPattern{
      "^([0-9]{4})(?:-([01][0-9]))?(?:-([0-3][0-9]))?"
      "(?:T([0-2][0-9]):([0-5][0-9]):([0-5][0-9]))?"
      "(?:\\.([0-9]+))?([+-][0-2][0-9]:?[0-5][0-9]|Z)?$"};
  auto compiled = compile_regex(std::string{kPattern}, "", source, byte_offset);
  if (!compiled) {
    return std::unexpected(compiled.error());
  }
  auto match = search_datetime_regex(**compiled, timestamp, regex_budget,
                                     source, byte_offset);
  if (!match) {
    return std::unexpected(match.error());
  }
  if (!*match || (*match)->groups.size() < 8) {
    return std::unexpected(
        dynamic_failure("D3110", "Invalid ISO 8601 timestamp", source,
                        byte_offset, std::string{timestamp}));
  }
  const auto parse_int = [](const RegexCapture &capture, int fallback) {
    if (!capture.matched) {
      return fallback;
    }
    int value{};
    std::from_chars(capture.text.data(),
                    capture.text.data() + capture.text.size(), value);
    return value;
  };
  const auto &groups = (*match)->groups;
  const int year_value = parse_int(groups[0], 0);
  const int month_value = parse_int(groups[1], 1);
  const int day_value = parse_int(groups[2], 1);
  const int hour = parse_int(groups[3], 0);
  const int minute = parse_int(groups[4], 0);
  const int second = parse_int(groups[5], 0);
  int millisecond = 0;
  if (groups[6].matched) {
    auto fraction = groups[6].text;
    if (fraction.size() > 3) {
      fraction.resize(3);
    }
    while (fraction.size() < 3) {
      fraction.push_back('0');
    }
    std::from_chars(fraction.data(), fraction.data() + fraction.size(),
                    millisecond);
  }
  using namespace std::chrono;
  const auto date = year_month_day{year{year_value},
                                   month{static_cast<unsigned>(month_value)},
                                   day{static_cast<unsigned>(day_value)}};
  if (!date.ok()) {
    return std::unexpected(
        dynamic_failure("D3110", "Invalid ISO 8601 timestamp", source,
                        byte_offset, std::string{timestamp}));
  }
  auto time = sys_time<milliseconds>{sys_days{date}.time_since_epoch()} +
              hours{hour} + minutes{minute} + seconds{second} +
              milliseconds{millisecond};
  if (groups[7].matched && groups[7].text != "Z") {
    const auto [offset_hours, offset_minutes] = timezone_offset(groups[7].text);
    time -= minutes{offset_hours * 60 + offset_minutes};
  }
  return static_cast<double>(time.time_since_epoch().count());
}

} // namespace

auto parse_datetime_picture(
    std::string_view timestamp, const std::string *picture,
    std::chrono::system_clock::time_point evaluation_timestamp,
    std::string_view source, std::size_t byte_offset,
    DateTimeRegexBudget *regex_budget) -> Result<std::optional<double>> {
  if (picture == nullptr) {
    auto parsed =
        parse_iso_datetime(timestamp, source, byte_offset, regex_budget);
    if (!parsed) {
      return std::unexpected(parsed.error());
    }
    return std::optional<double>{*parsed};
  }
  auto compiled = analyse_datetime_picture(*picture, source, byte_offset);
  if (!compiled) {
    return std::unexpected(compiled.error());
  }
  for (const auto &part : compiled->parts) {
    if (!part.marker || !part.marker->names) {
      continue;
    }
    const auto component = part.marker->component;
    if (component != 'M' && component != 'x' && component != 'F' &&
        component != 'P') {
      return std::unexpected(dynamic_failure(
          "D3133", "Name presentation is invalid for this component", source,
          byte_offset, std::string(1, component)));
    }
  }
  auto matcher = compile_date_matcher(*compiled, source, byte_offset);
  if (!matcher) {
    return std::unexpected(matcher.error());
  }
  auto match = search_datetime_regex(*matcher->regex, timestamp, regex_budget,
                                     source, byte_offset);
  if (!match) {
    return std::unexpected(match.error());
  }
  if (!*match) {
    return std::optional<double>{};
  }
  std::unordered_map<char, double> components;
  const auto &groups = (*match)->groups;
  for (std::size_t index = 0;
       index < matcher->parts.size() && index < groups.size(); ++index) {
    const auto *part = matcher->parts[index];
    if (!part->marker || !groups[index].matched) {
      continue;
    }
    auto value = parse_date_marker(*part->marker, groups[index].text, source,
                                   byte_offset);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (*value) {
      components[part->marker->component] = **value;
    }
  }
  return compose_datetime_millis(std::move(components), evaluation_timestamp,
                                 source, byte_offset);
}

} // namespace dagforge::jsonata::detail
