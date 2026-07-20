#include "evaluator.hpp"

#include "datetime.hpp"
#include "parser.hpp"
#include "regex_adapter.hpp"
#include "unicode.hpp"

#include <boost/multiprecision/cpp_int.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <format>
#include <iomanip>
#include <exception>
#include <limits>
#include <numeric>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

using boost::multiprecision::cpp_int;

[[nodiscard]] auto rounded_binary_integer(double input) -> cpp_int {
  const auto rounded = std::nearbyint(input);
  const auto magnitude = std::abs(rounded);
  if (magnitude == 0.0) {
    return 0;
  }
  int exponent = 0;
  const auto fraction = std::frexp(magnitude, &exponent);
  constexpr int significand_bits = 53;
  const auto significand =
      static_cast<std::uint64_t>(std::ldexp(fraction, significand_bits));
  cpp_int result = significand;
  if (exponent >= significand_bits) {
    result <<= exponent - significand_bits;
  } else {
    result >>= significand_bits - exponent;
  }
  return rounded < 0.0 ? -result : result;
}

[[nodiscard]] auto saturated_nonnegative_size(double input)
    -> std::optional<std::size_t> {
  const auto truncated = std::trunc(static_cast<long double>(input));
  if (std::isnan(truncated) || truncated < 0.0L) {
    return std::nullopt;
  }
  constexpr auto maximum = std::numeric_limits<std::size_t>::max();
  if (!std::isfinite(truncated) ||
      truncated >= static_cast<long double>(maximum)) {
    return maximum;
  }
  return static_cast<std::size_t>(truncated);
}

[[nodiscard]] auto clamped_character_index(double input,
                                           std::size_t character_count)
    -> std::size_t {
  auto index = std::trunc(static_cast<long double>(input));
  if (std::isnan(index)) {
    return 0;
  }
  if (index < 0.0L) {
    index += static_cast<long double>(character_count);
  }
  if (index <= 0.0L) {
    return 0;
  }
  if (!std::isfinite(index) ||
      index >= static_cast<long double>(character_count)) {
    return character_count;
  }
  return static_cast<std::size_t>(index);
}

[[nodiscard]] auto contains_non_finite_number(const Value &value) -> bool {
  if (const auto *number = std::get_if<double>(&value.storage)) {
    return !std::isfinite(*number);
  }
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&value.storage)) {
    return *array &&
           std::ranges::any_of((*array)->values, contains_non_finite_number);
  }
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&value.storage)) {
    return *object &&
           std::ranges::any_of((*object)->members, [](const auto &member) {
             return contains_non_finite_number(member.second);
           });
  }
  if (is_sequence(value)) {
    return std::ranges::any_of(as_sequence(value)->values,
                               contains_non_finite_number);
  }
  return false;
}

[[nodiscard]] auto is_hex_digit(char value) noexcept -> bool {
  return (value >= '0' && value <= '9') || (value >= 'a' && value <= 'f') ||
         (value >= 'A' && value <= 'F');
}

[[nodiscard]] auto hex_value(char value) noexcept -> unsigned char {
  if (value >= '0' && value <= '9') {
    return static_cast<unsigned char>(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return static_cast<unsigned char>(value - 'a' + 10);
  }
  return static_cast<unsigned char>(value - 'A' + 10);
}

[[nodiscard]] auto uri_unescaped(unsigned char value, bool component) noexcept
    -> bool {
  if (std::isalnum(value) != 0 || value == '-' || value == '_' ||
      value == '.' || value == '!' || value == '~' || value == '*' ||
      value == '\'' || value == '(' || value == ')') {
    return true;
  }
  if (component) {
    return false;
  }
  constexpr std::string_view kReserved{";/?:@&=+$,#"};
  return kReserved.contains(static_cast<char>(value));
}

[[nodiscard]] auto percent_encode(std::string_view input, bool component)
    -> std::optional<std::string> {
  if (!valid_utf8(input)) {
    return std::nullopt;
  }
  constexpr char kHex[] = "0123456789ABCDEF";
  std::string result;
  result.reserve(input.size());
  for (const auto raw : input) {
    const auto value = static_cast<unsigned char>(raw);
    if (uri_unescaped(value, component)) {
      result.push_back(static_cast<char>(value));
    } else {
      result.push_back('%');
      result.push_back(kHex[value >> 4U]);
      result.push_back(kHex[value & 0x0fU]);
    }
  }
  return result;
}

[[nodiscard]] auto percent_decode(std::string_view input, bool component)
    -> std::optional<std::string> {
  constexpr std::string_view kReserved{";/?:@&=+$,#"};
  std::string result;
  result.reserve(input.size());
  for (std::size_t index = 0; index < input.size();) {
    if (input[index] != '%') {
      result.push_back(input[index++]);
      continue;
    }
    if (index + 2 >= input.size() || !is_hex_digit(input[index + 1]) ||
        !is_hex_digit(input[index + 2])) {
      return std::nullopt;
    }
    const auto value = static_cast<unsigned char>(
        (hex_value(input[index + 1]) << 4U) | hex_value(input[index + 2]));
    if (!component && value < 0x80U &&
        kReserved.contains(static_cast<char>(value))) {
      result.append(input.substr(index, 3));
    } else {
      result.push_back(static_cast<char>(value));
    }
    index += 3;
  }
  return valid_utf8(result) ? std::optional<std::string>{std::move(result)}
                            : std::nullopt;
}

[[nodiscard]] auto base64_encode(std::string_view input) -> std::string {
  constexpr std::string_view kAlphabet{
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"};
  std::string result;
  result.reserve(((input.size() + 2U) / 3U) * 4U);
  for (std::size_t index = 0; index < input.size(); index += 3) {
    const auto first = static_cast<unsigned char>(input[index]);
    const auto second = index + 1 < input.size()
                            ? static_cast<unsigned char>(input[index + 1])
                            : 0U;
    const auto third = index + 2 < input.size()
                           ? static_cast<unsigned char>(input[index + 2])
                           : 0U;
    const auto packed = (static_cast<std::uint32_t>(first) << 16U) |
                        (static_cast<std::uint32_t>(second) << 8U) | third;
    result.push_back(kAlphabet[(packed >> 18U) & 0x3fU]);
    result.push_back(kAlphabet[(packed >> 12U) & 0x3fU]);
    result.push_back(
        index + 1 < input.size() ? kAlphabet[(packed >> 6U) & 0x3fU] : '=');
    result.push_back(index + 2 < input.size() ? kAlphabet[packed & 0x3fU]
                                              : '=');
  }
  return result;
}

[[nodiscard]] auto base64_decode(std::string_view input)
    -> std::optional<std::string> {
  if (input.size() % 4U != 0U) {
    return std::nullopt;
  }
  const auto decode = [](char value) -> int {
    if (value >= 'A' && value <= 'Z') {
      return value - 'A';
    }
    if (value >= 'a' && value <= 'z') {
      return value - 'a' + 26;
    }
    if (value >= '0' && value <= '9') {
      return value - '0' + 52;
    }
    if (value == '+') {
      return 62;
    }
    if (value == '/') {
      return 63;
    }
    return -1;
  };
  std::string result;
  for (std::size_t index = 0; index < input.size(); index += 4) {
    const int a = decode(input[index]);
    const int b = decode(input[index + 1]);
    const int c = input[index + 2] == '=' ? 0 : decode(input[index + 2]);
    const int d = input[index + 3] == '=' ? 0 : decode(input[index + 3]);
    if (a < 0 || b < 0 || c < 0 || d < 0 ||
        (input[index + 2] == '=' && input[index + 3] != '=') ||
        (index + 4 != input.size() &&
         (input[index + 2] == '=' || input[index + 3] == '='))) {
      return std::nullopt;
    }
    const auto packed = (static_cast<std::uint32_t>(a) << 18U) |
                        (static_cast<std::uint32_t>(b) << 12U) |
                        (static_cast<std::uint32_t>(c) << 6U) |
                        static_cast<std::uint32_t>(d);
    result.push_back(static_cast<char>((packed >> 16U) & 0xffU));
    if (input[index + 2] != '=') {
      result.push_back(static_cast<char>((packed >> 8U) & 0xffU));
    }
    if (input[index + 3] != '=') {
      result.push_back(static_cast<char>(packed & 0xffU));
    }
  }
  return result;
}

[[nodiscard]] auto format_integer_base(double input, int radix) -> std::string {
  constexpr std::string_view kDigits{"0123456789abcdefghijklmnopqrstuvwxyz"};
  const auto rounded = std::nearbyint(input);
  if (radix == 10 && std::abs(rounded) >= 1.0e21) {
    std::array<char, 64> buffer{};
    const auto [end, error] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), rounded,
                      std::chars_format::general);
    if (error == std::errc{}) {
      return std::string{buffer.data(), end};
    }
  }

  auto value = rounded_binary_integer(rounded);
  const bool negative = value < 0;
  if (negative) {
    value = -value;
  }
  std::string result;
  do {
    const auto digit =
        static_cast<std::size_t>((value % radix).convert_to<unsigned>());
    result.push_back(kDigits[digit]);
    value /= radix;
  } while (value != 0);
  if (negative) {
    result.push_back('-');
  }
  std::ranges::reverse(result);
  return result;
}

[[nodiscard]] auto shift_decimal_exponent(double value, int shift)
    -> std::optional<double> {
  std::array<char, 64> buffer{};
  const auto [end, error] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value,
                    std::chars_format::general);
  if (error != std::errc{}) {
    return std::nullopt;
  }
  std::string text{buffer.data(), end};
  const auto exponent_position = text.find_first_of("eE");
  auto mantissa = exponent_position == std::string::npos
                      ? text
                      : text.substr(0, exponent_position);
  int exponent = 0;
  if (exponent_position != std::string::npos) {
    const auto exponent_text =
        std::string_view{text}.substr(exponent_position + 1);
    const auto [parsed_end, parsed_error] =
        std::from_chars(exponent_text.data(),
                        exponent_text.data() + exponent_text.size(), exponent);
    if (parsed_error != std::errc{} ||
        parsed_end != exponent_text.data() + exponent_text.size()) {
      return std::nullopt;
    }
  }
  const auto shifted = mantissa + "e" + std::to_string(exponent + shift);
  double result = 0.0;
  const auto [parsed_end, parsed_error] =
      std::from_chars(shifted.data(), shifted.data() + shifted.size(), result,
                      std::chars_format::general);
  if (parsed_error != std::errc{} ||
      parsed_end != shifted.data() + shifted.size()) {
    return std::nullopt;
  }
  return result;
}

[[nodiscard]] auto round_half_even(double value, int precision) -> double {
  auto shifted = precision == 0 ? std::optional<double>{value}
                                : shift_decimal_exponent(value, precision);
  if (!shifted) {
    shifted = value * std::pow(10.0, precision);
  }
  auto rounded = std::floor(*shifted + 0.5);
  const auto difference = rounded - *shifted;
  if (std::abs(difference) == 0.5 && std::abs(std::fmod(rounded, 2.0)) == 1.0) {
    rounded -= 1.0;
  }
  if (precision != 0) {
    if (auto restored = shift_decimal_exponent(rounded, -precision)) {
      rounded = *restored;
    } else {
      rounded /= std::pow(10.0, precision);
    }
  }
  return rounded == 0.0 ? 0.0 : rounded;
}

[[nodiscard]] auto regex_match_value(const RegexMatch &match,
                                     std::string_view input) -> Value {
  std::vector<Value> groups;
  groups.reserve(match.groups.size());
  for (const auto &group : match.groups) {
    groups.push_back(group.matched ? Value{group.text} : undefined());
  }
  return make_object({
      {"match", Value{match.text}},
      {"index", Value{static_cast<double>(utf16_units(input, match.start))}},
      {"groups", make_array(std::move(groups))},
  });
}

[[nodiscard]] auto expand_regex_replacement(std::string_view replacement,
                                            const RegexMatch &match)
    -> std::string {
  std::string result;
  for (std::size_t position = 0; position < replacement.size();) {
    if (replacement[position] != '$' || position + 1 >= replacement.size()) {
      result.push_back(replacement[position++]);
      continue;
    }
    const auto next = replacement[position + 1];
    if (next == '$') {
      result.push_back('$');
      position += 2;
      continue;
    }
    if (next == '0') {
      result += match.text;
      position += 2;
      continue;
    }
    if (std::isdigit(static_cast<unsigned char>(next)) == 0) {
      result.push_back('$');
      ++position;
      continue;
    }

    const auto maximum_digits =
        match.groups.empty() ? std::size_t{1}
                             : std::to_string(match.groups.size()).size();
    std::size_t digits = 0;
    std::size_t group = 0;
    while (digits < maximum_digits &&
           position + 1 + digits < replacement.size() &&
           std::isdigit(static_cast<unsigned char>(
               replacement[position + 1 + digits])) != 0) {
      group = group * 10U + static_cast<std::size_t>(
                                replacement[position + 1 + digits] - '0');
      ++digits;
    }
    if (digits > 1 && group > match.groups.size()) {
      group /= 10U;
      --digits;
    }
    if (group >= 1 && group <= match.groups.size() &&
        match.groups[group - 1].matched) {
      result += match.groups[group - 1].text;
    }
    position += 1 + digits;
  }
  return result;
}

struct BuiltinSpec {
  std::string_view name;
  std::string_view signature;
  std::size_t arity;
};

struct RegisteredBuiltin {
  BuiltinSpec spec;
  std::shared_ptr<const FunctionSignature> signature;
};

[[nodiscard]] auto registered_builtins()
    -> const std::vector<RegisteredBuiltin> & {
  static const auto registered = [] {
    constexpr auto kSpecs = std::to_array<BuiltinSpec>({
        {"sum", "<a<n>:n>", 1},
        {"count", "<a:n>", 1},
        {"max", "<a<n>:n>", 1},
        {"min", "<a<n>:n>", 1},
        {"average", "<a<n>:n>", 1},
        {"string", "<x-b?:s>", 1},
        {"number", "<(nsb)-:n>", 1},
        {"boolean", "<x-:b>", 1},
        {"not", "<x-:b>", 1},
        {"exists", "<x:b>", 1},
        {"length", "<s-:n>", 1},
        {"substring", "<s-nn?:s>", 3},
        {"substringBefore", "<s-s:s>", 2},
        {"substringAfter", "<s-s:s>", 2},
        {"uppercase", "<s-:s>", 1},
        {"lowercase", "<s-:s>", 1},
        {"trim", "<s-:s>", 1},
        {"pad", "<s-ns?:s>", 3},
        {"contains", "<s-(sf):b>", 2},
        {"split", "<s-(sf)n?:a<s>>", 3},
        {"join", "<a<s>s?:s>", 2},
        {"replace", "<s-(sf)(sf)n?:s>", 4},
        {"match", "<s-f<s:o>n?:a<o>>", 3},
        {"base64encode", "<s-:s>", 1},
        {"base64decode", "<s-:s>", 1},
        {"encodeUrl", "<s-:s>", 1},
        {"decodeUrl", "<s-:s>", 1},
        {"encodeUrlComponent", "<s-:s>", 1},
        {"decodeUrlComponent", "<s-:s>", 1},
        {"append", "<xx:a>", 2},
        {"reverse", "<a:a>", 1},
        {"sort", "<af?:a>", 2},
        {"shuffle", "<a:a>", 1},
        {"distinct", "<x:x>", 1},
        {"zip", "<a+>", 0},
        {"keys", "<x-:a<s>>", 1},
        {"lookup", "<x-s:x>", 2},
        {"spread", "<x-:a<o>>", 1},
        {"merge", "<a<o>:o>", 1},
        {"sift", "<o-f?:o>", 2},
        {"each", "<o-f:a>", 2},
        {"map", "<af>", 2},
        {"filter", "<af>", 2},
        {"reduce", "<afj?:j>", 3},
        {"single", "<af?>", 2},
        {"abs", "<n-:n>", 1},
        {"floor", "<n-:n>", 1},
        {"ceil", "<n-:n>", 1},
        {"round", "<n-n?:n>", 2},
        {"sqrt", "<n-:n>", 1},
        {"power", "<n-n:n>", 2},
        {"random", "<:n>", 0},
        {"formatNumber", "<n-so?:s>", 3},
        {"formatBase", "<n-n?:s>", 2},
        {"formatInteger", "<n-s:s>", 2},
        {"parseInteger", "<s-s:n>", 2},
        {"fromMillis", "<n-s?s?:s>", 3},
        {"toMillis", "<s-s?:n>", 2},
        {"type", "<x:s>", 1},
        {"typeOf", "<x:s>", 1},
        {"now", "<s?s?:s>", 2},
        {"millis", "<:n>", 0},
        {"assert", "<bs?:x>", 2},
        {"error", "<s?:x>", 1},
        {"eval", "<sx?:x>", 2},
    });
    std::vector<RegisteredBuiltin> result;
    result.reserve(kSpecs.size());
    for (const auto &spec : kSpecs) {
      auto signature =
          parse_function_signature(spec.signature, spec.signature, 0);
      if (!signature) {
        std::terminate();
      }
      result.push_back(
          RegisteredBuiltin{.spec = spec, .signature = std::move(*signature)});
    }
    return result;
  }();
  return registered;
}

} // namespace

auto Evaluator::install_builtins(Environment &environment) -> void {
  for (const auto &registered : registered_builtins()) {
    auto function = std::make_shared<Function>();
    function->kind = FunctionKind::Builtin;
    function->name = registered.spec.name;
    function->arity = registered.spec.arity;
    function->signature = registered.signature;
    environment.bindings.emplace(std::string{registered.spec.name},
                                 Value{std::move(function)});
  }
}

auto Evaluator::value_list(const Value &raw) -> std::vector<Value> {
  const auto value = normalize_sequence(raw);
  if (is_undefined(value)) {
    return {};
  }
  if (is_sequence(value)) {
    return as_sequence(value)->values;
  }
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&value.storage)) {
    return (*array)->values;
  }
  return {value};
}

auto Evaluator::function_arity(const Value &raw) const noexcept -> std::size_t {
  const auto value = normalize_sequence(raw);
  if (const auto *function =
          std::get_if<std::shared_ptr<Function>>(&value.storage)) {
    return *function ? (*function)->arity : 0;
  }
  if (std::holds_alternative<std::shared_ptr<RegexValue>>(value.storage)) {
    return 2;
  }
  return 0;
}

auto Evaluator::higher_order_arguments(
    const Value &function, std::initializer_list<Value> candidates) const
    -> std::vector<Value> {
  const auto arity = function_arity(function);
  std::vector<Value> result;
  result.reserve(std::min(arity, candidates.size()));
  auto candidate = candidates.begin();
  for (std::size_t index = 0; index < arity && candidate != candidates.end();
       ++index, ++candidate) {
    result.push_back(*candidate);
  }
  return result;
}

auto Evaluator::require_string(const Value &raw, const Node &call_node,
                               std::string_view function)
    -> Result<std::string> {
  const auto value = normalize_sequence(raw);
  if (const auto *string = std::get_if<std::string>(&value.storage)) {
    return *string;
  }
  return std::unexpected(type_failure(
      "T0410", std::format("${} requires a string argument", function),
      program_.source, call_node.span.end));
}

auto Evaluator::require_number(const Value &raw, const Node &call_node,
                               std::string_view function) -> Result<double> {
  const auto value = normalize_sequence(raw);
  if (const auto *number = std::get_if<double>(&value.storage)) {
    return *number;
  }
  return std::unexpected(type_failure(
      "T0410", std::format("${} requires a number argument", function),
      program_.source, call_node.span.end));
}

auto Evaluator::require_regex(const Value &raw, const Node &call_node,
                              std::string_view function)
    -> Result<std::shared_ptr<RegexValue>> {
  const auto value = normalize_sequence(raw);
  if (const auto *regex =
          std::get_if<std::shared_ptr<RegexValue>>(&value.storage)) {
    return *regex;
  }
  return std::unexpected(type_failure(
      "T0410",
      std::format("${} requires a regular expression argument", function),
      program_.source, call_node.span.end));
}

auto Evaluator::next_regex_match(const RegexValue &regex,
                                 std::string_view input,
                                 std::size_t start_offset,
                                 const Node &call_node)
    -> Result<std::optional<RegexMatch>> {
  auto interrupted = check_interrupt(call_node);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  auto match = search_regex(regex, input, start_offset, regex_limits(),
                            program_.source, call_node.span.end);
  if (!match) {
    return std::unexpected(match.error());
  }
  interrupted = check_interrupt(call_node);
  if (!interrupted) {
    return std::unexpected(interrupted.error());
  }
  if (*match &&
      ++state_->regex_matches > state_->request.limits.max_regex_matches) {
    return std::unexpected(
        host_failure("H2102", "Regular expression match count limit exceeded",
                     program_.source, call_node.span.end));
  }
  return match;
}

auto Evaluator::regex_limits() const noexcept -> RegexLimits {
  RegexLimits limits;
  limits.match_limit = static_cast<std::uint32_t>(
      std::min<std::uint64_t>(state_->request.limits.max_steps,
                              std::numeric_limits<std::uint32_t>::max()));
  limits.depth_limit = static_cast<std::uint32_t>(
      std::min<std::size_t>(state_->request.limits.max_call_depth,
                            std::numeric_limits<std::uint32_t>::max()));
  return limits;
}

auto Evaluator::invoke_higher_order(
    std::string_view name, std::vector<Value> arguments, const Value &input,
    const std::shared_ptr<Environment> &environment, const Node &call_node,
    std::size_t call_depth) -> Result<Value> {
  if (name == "each" || name == "sift") {
    if (arguments.size() == 1) {
      arguments.insert(arguments.begin(), input);
    }
    if (arguments.size() != 2) {
      return std::unexpected(type_failure(
          "T0410", std::format("${} requires an object and function", name),
          program_.source, call_node.span.end));
    }
    const auto object_value = normalize_sequence(arguments[0]);
    if (is_undefined(object_value)) {
      return undefined();
    }
    const auto *object =
        std::get_if<std::shared_ptr<Object>>(&object_value.storage);
    if (object == nullptr) {
      return std::unexpected(type_failure(
          "T0410", std::format("${} requires an object and function", name),
          program_.source, call_node.span.end));
    }
    std::vector<Value> mapped;
    auto selected = std::make_shared<Object>();
    for (const auto &[key, value] : (*object)->members) {
      auto applied = apply(arguments[1],
                           higher_order_arguments(
                               arguments[1], {value, Value{key}, object_value}),
                           value, environment, call_node, call_depth + 1);
      if (!applied) {
        return std::unexpected(applied.error());
      }
      if (name == "each") {
        append_flattened(mapped, std::move(*applied));
      } else if (effective_boolean(*applied)) {
        object_set(*selected, key, value);
      }
    }
    if (name == "sift" && selected->members.empty()) {
      return undefined();
    }
    return name == "each" ? Result<Value>{normalize_sequence(
                                make_sequence(std::move(mapped)))}
                          : Result<Value>{Value{std::move(selected)}};
  }

  const auto valid_arity =
      name == "single"   ? arguments.size() >= 1 && arguments.size() <= 2
      : name == "reduce" ? arguments.size() >= 2 && arguments.size() <= 3
                         : arguments.size() == 2;
  if (!valid_arity) {
    return std::unexpected(type_failure(
        "T0410", std::format("${} requires an array and function", name),
        program_.source, call_node.span.end));
  }
  const auto source = normalize_sequence(arguments[0]);
  if (is_undefined(source)) {
    return undefined();
  }
  auto values = value_list(arguments[0]);
  if (name == "reduce") {
    if (function_arity(arguments[1]) < 2) {
      return std::unexpected(dynamic_failure(
          "D3050", "$reduce callback must accept at least two arguments",
          program_.source, call_node.span.end));
    }
    if (values.empty()) {
      return arguments.size() >= 3 ? arguments[2] : undefined();
    }
    Value accumulator = arguments.size() >= 3 ? arguments[2] : values.front();
    const auto first = arguments.size() >= 3 ? std::size_t{0} : std::size_t{1};
    for (std::size_t index = first; index < values.size(); ++index) {
      auto applied = apply(
          arguments[1],
          higher_order_arguments(
              arguments[1], {accumulator, values[index],
                             Value{static_cast<double>(index)}, arguments[0]}),
          values[index], environment, call_node, call_depth + 1);
      if (!applied) {
        return std::unexpected(applied.error());
      }
      accumulator = std::move(*applied);
    }
    return accumulator;
  }

  std::vector<Value> result;
  std::optional<Value> single;
  for (std::size_t index = 0; index < values.size(); ++index) {
    Result<Value> applied = Value{true};
    if (name != "single" || arguments.size() == 2) {
      const auto &function = arguments[1];
      applied =
          apply(function,
                higher_order_arguments(
                    function, {values[index], Value{static_cast<double>(index)},
                               arguments[0]}),
                values[index], environment, call_node, call_depth + 1);
    }
    if (!applied) {
      return std::unexpected(applied.error());
    }
    if (name == "map") {
      append_flattened(result, std::move(*applied));
    } else if (effective_boolean(*applied)) {
      if (name == "single") {
        if (single) {
          return std::unexpected(
              dynamic_failure("D3138", "$single matched more than one value",
                              program_.source, call_node.span.end));
        }
        single = values[index];
      } else {
        result.push_back(values[index]);
      }
    }
  }
  if (name == "single") {
    if (!single) {
      return std::unexpected(
          dynamic_failure("D3139", "$single matched no values", program_.source,
                          call_node.span.end));
    }
    return *single;
  }
  return normalize_sequence(make_sequence(std::move(result)));
}

auto Evaluator::invoke_builtin(std::string_view name,
                               std::vector<Value> arguments, const Value &input,
                               const std::shared_ptr<Environment> &environment,
                               const Node &call_node, std::size_t call_depth)
    -> Result<Value> {
  const auto argument = [&](std::size_t index) -> Value {
    return index < arguments.size() ? normalize_sequence(arguments[index])
                                    : undefined();
  };
  const auto context_default = [&](std::size_t index) -> Value {
    return index < arguments.size() ? argument(index) : input;
  };
  const auto read_nonnegative_limit =
      [&](std::size_t index, std::string_view code,
          std::string_view message) -> Result<std::optional<std::size_t>> {
    if (is_undefined(argument(index))) {
      return std::optional<std::size_t>{};
    }
    auto requested = require_number(argument(index), call_node, name);
    if (!requested) {
      return std::unexpected(requested.error());
    }
    auto converted = saturated_nonnegative_size(*requested);
    if (!converted) {
      return std::unexpected(
          dynamic_failure(std::string{code}, std::string{message},
                          program_.source, call_node.span.end));
    }
    return std::optional<std::size_t>{*converted};
  };
  const auto ensure_sequence_push = [&](std::size_t current) -> Result<void> {
    if (current >= state_->request.limits.max_sequence_items) {
      return std::unexpected(
          dynamic_failure("D2015", "Maximum sequence length exceeded",
                          program_.source, call_node.span.end));
    }
    return check_interrupt(call_node);
  };
  const auto append_bounded = [&](std::string &target,
                                  std::string_view chunk) -> Result<void> {
    const auto limit = state_->request.limits.max_string_bytes;
    if (chunk.size() > limit || target.size() > limit - chunk.size()) {
      return std::unexpected(
          host_failure("H2101", "JSONata value string byte limit exceeded",
                       program_.source, call_node.span.end));
    }
    target.append(chunk);
    return check_interrupt(call_node);
  };

  if (name == "count") {
    if (arguments.size() != 1) {
      return std::unexpected(
          type_failure("T0410", "$count requires exactly one argument",
                       program_.source, call_node.span.end));
    }
    return Value{static_cast<double>(value_list(context_default(0)).size())};
  }
  if (name == "sum" || name == "max" || name == "min" || name == "average") {
    if (arguments.size() != 1) {
      return std::unexpected(type_failure(
          "T0410", std::format("${} requires exactly one argument", name),
          program_.source, call_node.span.end));
    }
    const auto source = argument(0);
    if (is_undefined(source)) {
      return undefined();
    }
    const auto values = value_list(source);
    if (values.empty()) {
      return name == "sum" ? Result<Value>{Value{0.0}}
                           : Result<Value>{undefined()};
    }
    std::vector<double> numbers;
    numbers.reserve(values.size());
    for (const auto &value : values) {
      const auto *number = std::get_if<double>(&value.storage);
      if (number == nullptr) {
        return std::unexpected(
            type_failure("T0412", "Aggregation requires an array of numbers",
                         program_.source, call_node.span.end));
      }
      numbers.push_back(*number);
    }
    double total = 0.0;
    for (const auto number : numbers) {
      total += number;
    }
    if (name == "sum") {
      return Value{total};
    }
    if (name == "max") {
      return Value{*std::max_element(numbers.begin(), numbers.end())};
    }
    if (name == "min") {
      return Value{*std::min_element(numbers.begin(), numbers.end())};
    }
    return Value{total / static_cast<double>(numbers.size())};
  }
  if (name == "string") {
    if (arguments.size() > 2) {
      return std::unexpected(
          type_failure("T0410", "$string accepts at most two arguments",
                       program_.source, call_node.span.end));
    }
    const auto value = context_default(0);
    bool prettify = false;
    if (arguments.size() == 2) {
      const auto pretty_value = argument(1);
      const auto *pretty = std::get_if<bool>(&pretty_value.storage);
      if (pretty == nullptr) {
        return std::unexpected(
            type_failure("T0410", "$string prettify argument must be boolean",
                         program_.source, call_node.span.end));
      }
      prettify = *pretty;
    }
    if (const auto *number = std::get_if<double>(&value.storage);
        number != nullptr && !std::isfinite(*number)) {
      return std::unexpected(
          dynamic_failure("D3001", "$string cannot serialize Infinity or NaN",
                          program_.source, call_node.span.end));
    }
    if (contains_non_finite_number(value)) {
      return std::unexpected(dynamic_failure(
          "D1001", "Number cannot be represented as a JSON number",
          program_.source, call_node.span.end));
    }
    return is_undefined(value)
               ? Result<Value>{undefined()}
               : Result<Value>{Value{value_to_string(value, prettify)}};
  }
  if (name == "number") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    if (std::holds_alternative<double>(value.storage)) {
      return value;
    }
    if (const auto *boolean = std::get_if<bool>(&value.storage)) {
      return Value{*boolean ? 1.0 : 0.0};
    }
    if (const auto *string = std::get_if<std::string>(&value.storage)) {
      if (string->size() > 2 && (*string)[0] == '0') {
        int base = 0;
        switch ((*string)[1]) {
        case 'x':
        case 'X':
          base = 16;
          break;
        case 'o':
        case 'O':
          base = 8;
          break;
        case 'b':
        case 'B':
          base = 2;
          break;
        default:
          break;
        }
        if (base != 0) {
          std::uint64_t integer = 0;
          const auto digits = std::string_view{*string}.substr(2);
          const auto [end, error] = std::from_chars(
              digits.data(), digits.data() + digits.size(), integer, base);
          if (error == std::errc{} && end == digits.data() + digits.size()) {
            return Value{static_cast<double>(integer)};
          }
        }
      }
      double number = 0.0;
      const auto [end, error] =
          std::from_chars(string->data(), string->data() + string->size(),
                          number, std::chars_format::general);
      if (error == std::errc{} && end == string->data() + string->size() &&
          std::isfinite(number)) {
        return Value{number};
      }
      return std::unexpected(
          dynamic_failure("D3030", "Unable to cast value to a number",
                          program_.source, call_node.span.end));
    }
    return std::unexpected(type_failure(
        "T0410", "$number argument must be string, number, or boolean",
        program_.source, call_node.span.end));
  }
  if (name == "boolean") {
    if (arguments.size() > 1) {
      return std::unexpected(
          type_failure("T0410", "$boolean accepts at most one argument",
                       program_.source, call_node.span.end));
    }
    const auto value = context_default(0);
    return is_undefined(value) ? Result<Value>{undefined()}
                               : Result<Value>{Value{effective_boolean(value)}};
  }
  if (name == "not") {
    if (arguments.size() > 1) {
      return std::unexpected(type_failure("T0410",
                                          "$not accepts at most one argument",
                                          program_.source, call_node.span.end));
    }
    const auto value = context_default(0);
    return is_undefined(value)
               ? Result<Value>{undefined()}
               : Result<Value>{Value{!effective_boolean(value)}};
  }
  if (name == "exists") {
    if (arguments.size() != 1) {
      return std::unexpected(
          type_failure("T0410", "$exists requires exactly one argument",
                       program_.source, call_node.span.end));
    }
    return Value{!is_undefined(argument(0))};
  }
  if (name == "length") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    return Value{static_cast<double>(unicode_characters(*string).size())};
  }
  if (name == "substring") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    auto start = require_number(argument(1), call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    if (!start) {
      return std::unexpected(start.error());
    }
    const auto characters = unicode_characters(*string);
    const auto begin = clamped_character_index(*start, characters.size());
    auto end = characters.size();
    if (!is_undefined(argument(2))) {
      auto length = require_number(argument(2), call_node, name);
      if (!length) {
        return std::unexpected(length.error());
      }
      if (*length <= 0) {
        return Value{std::string{}};
      }
      const auto truncated = std::trunc(static_cast<long double>(*length));
      const auto remaining = characters.size() - begin;
      const auto selected =
          !std::isfinite(truncated) ||
                  truncated >= static_cast<long double>(remaining)
              ? remaining
              : static_cast<std::size_t>(truncated);
      end = begin + selected;
    }
    std::string result;
    for (auto index = begin; index < end; ++index) {
      result += characters[index];
    }
    return Value{std::move(result)};
  }
  if (name == "substringBefore" || name == "substringAfter") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    auto token = require_string(argument(1), call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    if (!token) {
      return std::unexpected(token.error());
    }
    const auto position = string->find(*token);
    if (position == std::string::npos) {
      return Value{*string};
    }
    return Value{name == "substringBefore"
                     ? string->substr(0, position)
                     : string->substr(position + token->size())};
  }
  if (name == "uppercase" || name == "lowercase") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    std::ranges::transform(*string, string->begin(), [name](unsigned char c) {
      return static_cast<char>(name == "uppercase" ? std::toupper(c)
                                                   : std::tolower(c));
    });
    return Value{std::move(*string)};
  }
  if (name == "trim") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    std::string result;
    result.reserve(string->size());
    bool pending_space = false;
    for (const auto character : *string) {
      const bool whitespace = character == ' ' || character == '\t' ||
                              character == '\n' || character == '\r';
      if (whitespace) {
        pending_space = !result.empty();
        continue;
      }
      if (pending_space) {
        result.push_back(' ');
        pending_space = false;
      }
      result.push_back(character);
    }
    return Value{std::move(result)};
  }
  if (name == "pad") {
    const auto value = argument(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    auto width = require_number(argument(1), call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    if (!width) {
      return std::unexpected(width.error());
    }
    std::string padding_token{" "};
    if (!is_undefined(argument(2))) {
      auto token = require_string(argument(2), call_node, name);
      if (!token) {
        return std::unexpected(token.error());
      }
      if (!token->empty()) {
        padding_token = std::move(*token);
      }
    }
    const auto target_width =
        std::abs(std::trunc(static_cast<long double>(*width)));
    const auto current_size = unicode_characters(*string).size();
    const auto string_limit = state_->request.limits.max_string_bytes;
    if (std::isnan(target_width) || !std::isfinite(target_width) ||
        target_width > static_cast<long double>(string_limit)) {
      return std::unexpected(
          host_failure("H2101", "JSONata value string byte limit exceeded",
                       program_.source, call_node.span.end));
    }
    const auto target = static_cast<std::size_t>(target_width);
    if (target <= current_size) {
      return Value{std::move(*string)};
    }
    const auto padding_size = target - current_size;
    const auto token_characters = unicode_characters(padding_token);
    const auto token_bytes = padding_token.size();
    const auto full_cycles = padding_size / token_characters.size();
    const auto remainder = padding_size % token_characters.size();
    if (full_cycles > string_limit / token_bytes) {
      return std::unexpected(
          host_failure("H2101", "JSONata value string byte limit exceeded",
                       program_.source, call_node.span.end));
    }
    auto padding_bytes = full_cycles * token_bytes;
    for (std::size_t index = 0; index < remainder; ++index) {
      if (token_characters[index].size() > string_limit - padding_bytes) {
        return std::unexpected(
            host_failure("H2101", "JSONata value string byte limit exceeded",
                         program_.source, call_node.span.end));
      }
      padding_bytes += token_characters[index].size();
    }
    if (string->size() > string_limit - padding_bytes) {
      return std::unexpected(
          host_failure("H2101", "JSONata value string byte limit exceeded",
                       program_.source, call_node.span.end));
    }
    std::string result;
    result.reserve(string->size() + padding_bytes);
    const bool pad_right = *width >= 0.0;
    if (pad_right) {
      result += *string;
    }
    for (std::size_t index = 0; index < padding_size; ++index) {
      result += token_characters[index % token_characters.size()];
      if ((index & 0x3FFU) == 0U) {
        auto interrupted = check_interrupt(call_node);
        if (!interrupted) {
          return std::unexpected(interrupted.error());
        }
        auto charged = consume_step(call_node, call_depth);
        if (!charged) {
          return std::unexpected(charged.error());
        }
      }
    }
    if (!pad_right) {
      result += *string;
    }
    return Value{std::move(result)};
  }
  if (name == "contains") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    const auto pattern = argument(1);
    if (is_undefined(pattern)) {
      return undefined();
    }
    if (const auto *token = std::get_if<std::string>(&pattern.storage)) {
      return Value{string->contains(*token)};
    }
    auto regex = require_regex(pattern, call_node, name);
    if (!regex) {
      return std::unexpected(regex.error());
    }
    auto match = next_regex_match(**regex, *string, 0, call_node);
    if (!match) {
      return std::unexpected(match.error());
    }
    return Value{match->has_value()};
  }
  if (name == "match") {
    const auto value = argument(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    const auto matcher = argument(1);
    if (is_undefined(matcher)) {
      return std::unexpected(type_failure("T0410", "$match requires a matcher",
                                          program_.source, call_node.span.end));
    }
    std::optional<std::size_t> limit;
    if (!is_undefined(argument(2))) {
      auto requested = read_nonnegative_limit(
          2, "D3040", "$match limit must be non-negative");
      if (!requested) {
        return std::unexpected(requested.error());
      }
      limit = *requested;
    }
    if (limit && *limit == 0) {
      return make_array();
    }

    std::vector<Value> matches;
    if (const auto *regex =
            std::get_if<std::shared_ptr<RegexValue>>(&matcher.storage)) {
      std::size_t offset = 0;
      while (!limit || matches.size() < *limit) {
        auto found = next_regex_match(**regex, *string, offset, call_node);
        if (!found) {
          return std::unexpected(found.error());
        }
        if (!*found) {
          break;
        }
        auto growth = ensure_sequence_push(matches.size());
        if (!growth) {
          return std::unexpected(growth.error());
        }
        matches.push_back(regex_match_value(**found, *string));
        if ((*found)->end == (*found)->start) {
          return std::unexpected(dynamic_failure(
              "D1004", "Regular expression matched an empty string",
              program_.source, call_node.span.end));
        }
        offset = (*found)->end;
      }
      return make_array(std::move(matches));
    }

    const auto *matcher_function =
        std::get_if<std::shared_ptr<Function>>(&matcher.storage);
    if (matcher_function == nullptr || !*matcher_function) {
      return std::unexpected(type_failure("T0410",
                                          "$match matcher must be a function",
                                          program_.source, call_node.span.end));
    }
    Value next_function = matcher;
    bool first = true;
    while (!limit || matches.size() < *limit) {
      auto matched = apply(next_function,
                           first ? std::vector<Value>{Value{*string}}
                                 : std::vector<Value>{},
                           input, environment, call_node, call_depth + 1);
      first = false;
      if (!matched) {
        return std::unexpected(matched.error());
      }
      const auto normalized = normalize_sequence(*matched);
      if (is_undefined(normalized)) {
        break;
      }
      const auto *object =
          std::get_if<std::shared_ptr<Object>>(&normalized.storage);
      if (object == nullptr || !*object) {
        return std::unexpected(
            type_failure("T1010", "Matcher function returned an invalid result",
                         program_.source, call_node.span.end));
      }
      auto match_value = object_lookup(**object, "match");
      auto start_value = object_lookup(**object, "start");
      auto end_value = object_lookup(**object, "end");
      auto groups_value = object_lookup(**object, "groups");
      auto next_value = object_lookup(**object, "next");
      if (!match_value || !start_value || !end_value || !groups_value ||
          !next_value ||
          !std::holds_alternative<std::string>(match_value->storage) ||
          !std::holds_alternative<double>(start_value->storage) ||
          !std::holds_alternative<double>(end_value->storage) ||
          !std::holds_alternative<std::shared_ptr<Array>>(
              groups_value->storage) ||
          !std::holds_alternative<std::shared_ptr<Function>>(
              next_value->storage)) {
        return std::unexpected(
            type_failure("T1010", "Matcher function returned an invalid result",
                         program_.source, call_node.span.end));
      }
      auto growth = ensure_sequence_push(matches.size());
      if (!growth) {
        return std::unexpected(growth.error());
      }
      matches.push_back(make_object({
          {"match", *match_value},
          {"index", *start_value},
          {"groups", *groups_value},
      }));
      if (std::get<double>(start_value->storage) ==
          std::get<double>(end_value->storage)) {
        return std::unexpected(
            dynamic_failure("D1004", "Matcher function did not advance",
                            program_.source, call_node.span.end));
      }
      next_function = std::move(*next_value);
    }
    return make_array(std::move(matches));
  }
  if (name == "split") {
    const auto value = argument(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    const auto separator = argument(1);
    if (is_undefined(separator)) {
      return std::unexpected(type_failure("T0410",
                                          "$split requires a separator",
                                          program_.source, call_node.span.end));
    }
    std::optional<std::size_t> limit;
    if (!is_undefined(argument(2))) {
      auto requested = read_nonnegative_limit(
          2, "D3020", "$split limit must be non-negative");
      if (!requested) {
        return std::unexpected(requested.error());
      }
      limit = *requested;
    }
    std::vector<Value> result;
    if (limit && *limit == 0) {
      return make_array();
    }
    if (const auto *token = std::get_if<std::string>(&separator.storage)) {
      if (token->empty()) {
        for (auto &character : unicode_characters(*string)) {
          if (limit && result.size() >= *limit) {
            break;
          }
          auto growth = ensure_sequence_push(result.size());
          if (!growth) {
            return std::unexpected(growth.error());
          }
          result.emplace_back(std::move(character));
        }
        return make_array(std::move(result));
      }
      std::size_t begin = 0;
      while (!limit || result.size() < *limit) {
        const auto position = string->find(*token, begin);
        if (position == std::string::npos) {
          auto growth = ensure_sequence_push(result.size());
          if (!growth) {
            return std::unexpected(growth.error());
          }
          result.emplace_back(string->substr(begin));
          break;
        }
        auto growth = ensure_sequence_push(result.size());
        if (!growth) {
          return std::unexpected(growth.error());
        }
        result.emplace_back(string->substr(begin, position - begin));
        begin = position + token->size();
      }
      return make_array(std::move(result));
    }
    auto regex = require_regex(separator, call_node, name);
    if (!regex) {
      if (std::holds_alternative<std::shared_ptr<Function>>(
              separator.storage)) {
        return std::unexpected(
            type_failure("T1010", "Matcher function returned an invalid result",
                         program_.source, call_node.span.end));
      }
      return std::unexpected(regex.error());
    }
    std::size_t begin = 0;
    while (!limit || result.size() < *limit) {
      auto match = next_regex_match(**regex, *string, begin, call_node);
      if (!match) {
        return std::unexpected(match.error());
      }
      if (!*match) {
        break;
      }
      auto growth = ensure_sequence_push(result.size());
      if (!growth) {
        return std::unexpected(growth.error());
      }
      result.emplace_back(string->substr(begin, (*match)->start - begin));
      if ((*match)->end == (*match)->start) {
        return std::unexpected(dynamic_failure(
            "D1004", "Regular expression matched an empty string",
            program_.source, call_node.span.end));
      }
      begin = (*match)->end;
    }
    if (!limit || result.size() < *limit) {
      auto growth = ensure_sequence_push(result.size());
      if (!growth) {
        return std::unexpected(growth.error());
      }
      result.emplace_back(string->substr(begin));
    }
    return make_array(std::move(result));
  }
  if (name == "replace") {
    const auto value = argument(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    const auto pattern = argument(1);
    const auto replacement = argument(2);
    if (is_undefined(pattern) || is_undefined(replacement)) {
      return std::unexpected(type_failure(
          "T0410", "$replace requires pattern and replacement arguments",
          program_.source, call_node.span.end));
    }
    std::optional<std::size_t> limit;
    if (!is_undefined(argument(3))) {
      auto requested = read_nonnegative_limit(
          3, "D3011", "$replace limit must be non-negative");
      if (!requested) {
        return std::unexpected(requested.error());
      }
      limit = *requested;
    }
    if (limit && *limit == 0) {
      return Value{std::move(*string)};
    }
    const auto *replacement_string =
        std::get_if<std::string>(&replacement.storage);
    const auto *replacement_function =
        std::get_if<std::shared_ptr<Function>>(&replacement.storage);
    if (replacement_string == nullptr && replacement_function == nullptr) {
      return std::unexpected(type_failure(
          "T0410", "$replace replacement must be a string or function",
          program_.source, call_node.span.end));
    }
    if (const auto *token = std::get_if<std::string>(&pattern.storage)) {
      if (token->empty()) {
        return std::unexpected(
            dynamic_failure("D3010", "$replace pattern cannot be empty",
                            program_.source, call_node.span.end));
      }
      if (replacement_string == nullptr) {
        return std::unexpected(type_failure(
            "T0410", "A string pattern requires a string replacement",
            program_.source, call_node.span.end));
      }
      std::string result;
      std::size_t begin = 0;
      std::size_t count = 0;
      while (!limit || count < *limit) {
        const auto position = string->find(*token, begin);
        if (position == std::string::npos) {
          break;
        }
        auto appended = append_bounded(
            result, std::string_view{*string}.substr(begin, position - begin));
        if (!appended) {
          return std::unexpected(appended.error());
        }
        appended = append_bounded(result, *replacement_string);
        if (!appended) {
          return std::unexpected(appended.error());
        }
        begin = position + token->size();
        ++count;
      }
      auto appended =
          append_bounded(result, std::string_view{*string}.substr(begin));
      if (!appended) {
        return std::unexpected(appended.error());
      }
      return Value{std::move(result)};
    }
    auto regex = require_regex(pattern, call_node, name);
    if (!regex) {
      return std::unexpected(regex.error());
    }
    std::string result;
    std::size_t begin = 0;
    std::size_t count = 0;
    while (!limit || count < *limit) {
      auto match = next_regex_match(**regex, *string, begin, call_node);
      if (!match) {
        return std::unexpected(match.error());
      }
      if (!*match) {
        break;
      }
      auto appended = append_bounded(
          result,
          std::string_view{*string}.substr(begin, (*match)->start - begin));
      if (!appended) {
        return std::unexpected(appended.error());
      }
      if (replacement_string != nullptr) {
        const auto expanded =
            expand_regex_replacement(*replacement_string, **match);
        appended = append_bounded(result, expanded);
        if (!appended) {
          return std::unexpected(appended.error());
        }
      } else {
        auto match_object = regex_match_value(**match, *string);
        auto replaced = apply(replacement, {match_object}, input, environment,
                              call_node, call_depth + 1);
        if (!replaced) {
          return std::unexpected(replaced.error());
        }
        const auto replaced_value = normalize_sequence(*replaced);
        const auto *text = std::get_if<std::string>(&replaced_value.storage);
        if (text == nullptr) {
          return std::unexpected(
              dynamic_failure("D3012", "$replace function must return a string",
                              program_.source, call_node.span.end));
        }
        appended = append_bounded(result, *text);
        if (!appended) {
          return std::unexpected(appended.error());
        }
      }
      if ((*match)->end == (*match)->start) {
        return std::unexpected(dynamic_failure(
            "D1004", "Regular expression matched an empty string",
            program_.source, call_node.span.end));
      }
      begin = (*match)->end;
      ++count;
    }
    auto appended =
        append_bounded(result, std::string_view{*string}.substr(begin));
    if (!appended) {
      return std::unexpected(appended.error());
    }
    return Value{std::move(result)};
  }
  if (name == "base64encode" || name == "base64decode") {
    const auto value = context_default(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    if (name == "base64encode") {
      return Value{base64_encode(*string)};
    }
    auto decoded = base64_decode(*string);
    return decoded ? Result<Value>{Value{std::move(*decoded)}}
                   : Result<Value>{std::unexpected(
                         dynamic_failure("D3140", "Malformed base64 input",
                                         program_.source, call_node.span.end))};
  }
  if (name == "encodeUrl" || name == "decodeUrl" ||
      name == "encodeUrlComponent" || name == "decodeUrlComponent") {
    const auto value = argument(0);
    if (is_undefined(value)) {
      return undefined();
    }
    auto string = require_string(value, call_node, name);
    if (!string) {
      return std::unexpected(string.error());
    }
    const bool component = name.ends_with("Component");
    const bool encoding = name.starts_with("encode");
    auto converted = encoding ? percent_encode(*string, component)
                              : percent_decode(*string, component);
    if (!converted) {
      return std::unexpected(dynamic_failure(
          "D3140", std::format("Malformed URL passed to ${}()", name),
          program_.source, call_node.span.end));
    }
    return Value{std::move(*converted)};
  }
  if (name == "join") {
    const auto source = argument(0);
    if (is_undefined(source)) {
      return undefined();
    }
    const auto values = value_list(source);
    auto separator = is_undefined(argument(1))
                         ? Result<std::string>{std::string{}}
                         : require_string(argument(1), call_node, name);
    if (!separator) {
      return std::unexpected(separator.error());
    }
    std::string result;
    for (std::size_t index = 0; index < values.size(); ++index) {
      auto item = require_string(values[index], call_node, name);
      if (!item) {
        return std::unexpected(item.error());
      }
      if (index != 0) {
        auto appended = append_bounded(result, *separator);
        if (!appended) {
          return std::unexpected(appended.error());
        }
      }
      auto appended = append_bounded(result, *item);
      if (!appended) {
        return std::unexpected(appended.error());
      }
    }
    return Value{std::move(result)};
  }
  if (name == "append") {
    if (is_undefined(argument(0))) {
      return argument(1);
    }
    if (is_undefined(argument(1))) {
      return argument(0);
    }
    auto result = value_list(argument(0));
    auto tail = value_list(argument(1));
    result.insert(result.end(), std::make_move_iterator(tail.begin()),
                  std::make_move_iterator(tail.end()));
    return make_array(std::move(result));
  }
  if (name == "reverse") {
    if (is_undefined(argument(0))) {
      return undefined();
    }
    auto result = value_list(context_default(0));
    std::ranges::reverse(result);
    return make_array(std::move(result));
  }
  if (name == "sort") {
    const auto source = argument(0);
    if (is_undefined(source)) {
      return undefined();
    }
    auto values = value_list(source);
    const auto comparator = argument(1);
    for (std::size_t index = 1; index < values.size(); ++index) {
      std::size_t current = index;
      while (current > 0) {
        bool swap = false;
        if (!is_undefined(comparator)) {
          auto compared =
              apply(comparator, {values[current - 1], values[current]}, input,
                    environment, call_node, call_depth + 1);
          if (!compared) {
            return std::unexpected(compared.error());
          }
          swap = effective_boolean(*compared);
        } else {
          const auto left = normalize_sequence(values[current - 1]);
          const auto right = normalize_sequence(values[current]);
          if (const auto *left_number = std::get_if<double>(&left.storage)) {
            const auto *right_number = std::get_if<double>(&right.storage);
            if (right_number == nullptr) {
              return std::unexpected(dynamic_failure(
                  "D3070", "$sort values must all be numbers or all strings",
                  program_.source, call_node.span.end));
            }
            swap = *left_number > *right_number;
          } else if (const auto *left_string =
                         std::get_if<std::string>(&left.storage)) {
            const auto *right_string = std::get_if<std::string>(&right.storage);
            if (right_string == nullptr) {
              return std::unexpected(dynamic_failure(
                  "D3070", "$sort values must all be numbers or all strings",
                  program_.source, call_node.span.end));
            }
            swap = *left_string > *right_string;
          } else {
            return std::unexpected(dynamic_failure(
                "D3070", "$sort values must be numbers or strings",
                program_.source, call_node.span.end));
          }
        }
        if (!swap) {
          break;
        }
        std::swap(values[current - 1], values[current]);
        --current;
      }
    }
    return make_array(std::move(values));
  }
  if (name == "shuffle") {
    const auto source = argument(0);
    if (is_undefined(source)) {
      return undefined();
    }
    auto values = value_list(source);
    std::shuffle(values.begin(), values.end(), state_->random_engine);
    return make_array(std::move(values));
  }
  if (name == "distinct") {
    if (arguments.size() > 1) {
      return std::unexpected(
          type_failure("T0410", "$distinct accepts at most one argument",
                       program_.source, call_node.span.end));
    }
    const auto source = context_default(0);
    if (is_undefined(source)) {
      return undefined();
    }
    if (!is_sequence(source) &&
        !std::holds_alternative<std::shared_ptr<Array>>(source.storage)) {
      return source;
    }
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&source.storage);
        array != nullptr && (*array)->values.size() <= 1) {
      return source;
    }
    std::vector<Value> result;
    for (const auto &value : value_list(source)) {
      if (!std::ranges::any_of(result, [&](const Value &existing) {
            return value_equal(existing, value);
          })) {
        result.push_back(value);
      }
    }
    return normalize_sequence(make_sequence(std::move(result)));
  }
  if (name == "zip") {
    if (arguments.empty()) {
      return make_array();
    }
    std::vector<std::vector<Value>> columns;
    columns.reserve(arguments.size());
    std::size_t length = std::numeric_limits<std::size_t>::max();
    for (const auto &item : arguments) {
      auto values = value_list(item);
      length = std::min(length, values.size());
      columns.push_back(std::move(values));
    }
    std::vector<Value> result;
    result.reserve(length);
    for (std::size_t row = 0; row < length; ++row) {
      std::vector<Value> tuple;
      tuple.reserve(columns.size());
      for (const auto &column : columns) {
        tuple.push_back(column[row]);
      }
      result.push_back(make_array(std::move(tuple)));
    }
    return make_array(std::move(result));
  }
  if (name == "keys") {
    std::vector<Value> keys;
    const auto value = context_default(0);
    for (const auto &candidate : value_list(value)) {
      if (const auto *object =
              std::get_if<std::shared_ptr<Object>>(&candidate.storage)) {
        for (const auto &[key, item] : (*object)->members) {
          (void)item;
          if (std::ranges::none_of(keys, [&](const Value &existing) {
                return std::get<std::string>(existing.storage) == key;
              })) {
            keys.emplace_back(key);
          }
        }
      }
    }
    return normalize_sequence(make_sequence(std::move(keys)));
  }
  if (name == "lookup") {
    const auto value = context_default(0);
    auto key = require_string(argument(1), call_node, name);
    if (!key) {
      return std::unexpected(key.error());
    }
    if (const auto *object =
            std::get_if<std::shared_ptr<Object>>(&value.storage)) {
      return object_lookup(**object, *key).value_or(undefined());
    }
    std::vector<Value> result;
    for (const auto &item : value_list(value)) {
      if (const auto *object =
              std::get_if<std::shared_ptr<Object>>(&item.storage)) {
        if (auto found = object_lookup(**object, *key)) {
          append_flattened(result, *found);
        }
      }
    }
    return normalize_sequence(make_sequence(std::move(result)));
  }
  if (name == "spread") {
    const auto spread_value = [&](const auto &self, const Value &raw) -> Value {
      const auto value = normalize_sequence(raw);
      if (is_undefined(value)) {
        return undefined();
      }
      std::vector<Value> result;
      if (is_sequence(value)) {
        for (const auto &item : as_sequence(value)->values) {
          append_flattened(result, self(self, item));
        }
        return normalize_sequence(make_sequence(std::move(result)));
      }
      if (const auto *array =
              std::get_if<std::shared_ptr<Array>>(&value.storage)) {
        for (const auto &item : (*array)->values) {
          append_flattened(result, self(self, item));
        }
        return normalize_sequence(make_sequence(std::move(result)));
      }
      if (const auto *object =
              std::get_if<std::shared_ptr<Object>>(&value.storage)) {
        for (const auto &[key, item] : (*object)->members) {
          result.push_back(make_object({{key, item}}));
        }
        return normalize_sequence(make_sequence(std::move(result)));
      }
      return value;
    };
    return spread_value(spread_value, context_default(0));
  }
  if (name == "merge") {
    if (is_undefined(argument(0))) {
      return undefined();
    }
    auto result = std::make_shared<Object>();
    for (const auto &value : value_list(context_default(0))) {
      const auto *object = std::get_if<std::shared_ptr<Object>>(&value.storage);
      if (object == nullptr) {
        return std::unexpected(
            type_failure("T0412", "$merge expects an array of objects",
                         program_.source, call_node.span.end));
      }
      for (const auto &[key, item] : (*object)->members) {
        object_set(*result, key, item);
      }
    }
    return Value{std::move(result)};
  }
  if (name == "map" || name == "filter" || name == "reduce" ||
      name == "single" || name == "each" || name == "sift") {
    return invoke_higher_order(name, std::move(arguments), input, environment,
                               call_node, call_depth);
  }
  if (name == "abs" || name == "floor" || name == "ceil" || name == "round" ||
      name == "sqrt") {
    if (is_undefined(argument(0))) {
      return undefined();
    }
    auto number = require_number(context_default(0), call_node, name);
    if (!number) {
      return std::unexpected(number.error());
    }
    if (name == "abs") {
      return Value{std::abs(*number)};
    }
    if (name == "floor") {
      return Value{std::floor(*number)};
    }
    if (name == "ceil") {
      return Value{std::ceil(*number)};
    }
    if (name == "sqrt") {
      if (*number < 0) {
        return std::unexpected(
            dynamic_failure("D3060", "Square root of a negative number",
                            program_.source, call_node.span.end));
      }
      return Value{std::sqrt(*number)};
    }
    int precision = 0;
    if (!is_undefined(argument(1))) {
      auto requested = require_number(argument(1), call_node, name);
      if (!requested) {
        return std::unexpected(requested.error());
      }
      const auto truncated = std::trunc(static_cast<long double>(*requested));
      if (std::isnan(truncated) || truncated > 308.0L) {
        return Value{std::numeric_limits<double>::quiet_NaN()};
      }
      if (truncated < -308.0L) {
        return Value{0.0};
      }
      precision = static_cast<int>(truncated);
    }
    return Value{round_half_even(*number, precision)};
  }
  if (name == "power") {
    if (is_undefined(argument(0))) {
      return undefined();
    }
    auto base = require_number(argument(0), call_node, name);
    auto exponent = require_number(argument(1), call_node, name);
    if (!base) {
      return std::unexpected(base.error());
    }
    if (!exponent) {
      return std::unexpected(exponent.error());
    }
    const auto result = std::pow(*base, *exponent);
    if (!std::isfinite(result)) {
      return std::unexpected(
          dynamic_failure("D3061", "Power result is not finite",
                          program_.source, call_node.span.end));
    }
    return Value{result};
  }
  if (name == "formatBase") {
    const auto source = argument(0);
    if (is_undefined(source)) {
      return undefined();
    }
    auto value = require_number(source, call_node, name);
    if (!value) {
      return std::unexpected(value.error());
    }
    int radix = 10;
    if (!is_undefined(argument(1))) {
      auto requested = require_number(argument(1), call_node, name);
      if (!requested) {
        return std::unexpected(requested.error());
      }
      const auto rounded = std::nearbyint(static_cast<long double>(*requested));
      if (!std::isfinite(rounded) || rounded < 2.0L || rounded > 36.0L) {
        return std::unexpected(dynamic_failure(
            "D3100", "$formatBase radix must be between 2 and 36",
            program_.source, call_node.span.end));
      }
      radix = static_cast<int>(rounded);
    }
    if (radix < 2 || radix > 36) {
      return std::unexpected(
          dynamic_failure("D3100", "$formatBase radix must be between 2 and 36",
                          program_.source, call_node.span.end));
    }
    if (!std::isfinite(*value)) {
      return std::unexpected(
          dynamic_failure("D1001", "$formatBase value must be finite",
                          program_.source, call_node.span.end));
    }
    return Value{format_integer_base(*value, radix)};
  }
  if (name == "formatNumber") {
    const auto source_value = argument(0);
    if (is_undefined(source_value)) {
      return undefined();
    }
    auto value = require_number(source_value, call_node, name);
    auto picture = require_string(argument(1), call_node, name);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (!picture) {
      return std::unexpected(picture.error());
    }
    const Object *options = nullptr;
    const auto option_value = argument(2);
    if (!is_undefined(option_value)) {
      const auto *object =
          std::get_if<std::shared_ptr<Object>>(&option_value.storage);
      if (object == nullptr || !*object) {
        return std::unexpected(
            type_failure("T0410", "$formatNumber options must be an object",
                         program_.source, call_node.span.end));
      }
      options = object->get();
    }
    auto formatted = format_number_picture(*value, *picture, options,
                                           program_.source, call_node.span.end);
    return formatted ? Result<Value>{Value{std::move(*formatted)}}
                     : Result<Value>{std::unexpected(formatted.error())};
  }
  if (name == "formatInteger") {
    const auto source_value = argument(0);
    if (is_undefined(source_value)) {
      return undefined();
    }
    auto value = require_number(source_value, call_node, name);
    auto picture = require_string(argument(1), call_node, name);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (!picture) {
      return std::unexpected(picture.error());
    }
    auto formatted = format_integer_picture(*value, *picture, program_.source,
                                            call_node.span.end);
    return formatted ? Result<Value>{Value{std::move(*formatted)}}
                     : Result<Value>{std::unexpected(formatted.error())};
  }
  if (name == "parseInteger") {
    const auto source_value = argument(0);
    if (is_undefined(source_value)) {
      return undefined();
    }
    auto value = require_string(source_value, call_node, name);
    auto picture = require_string(argument(1), call_node, name);
    if (!value) {
      return std::unexpected(value.error());
    }
    if (!picture) {
      return std::unexpected(picture.error());
    }
    auto parsed = parse_integer_picture(*value, *picture, program_.source,
                                        call_node.span.end);
    return parsed ? Result<Value>{Value{*parsed}}
                  : Result<Value>{std::unexpected(parsed.error())};
  }
  if (name == "fromMillis") {
    const auto source_value = context_default(0);
    if (is_undefined(source_value)) {
      return undefined();
    }
    auto millis = require_number(source_value, call_node, name);
    if (!millis) {
      return std::unexpected(millis.error());
    }
    std::optional<std::string> picture;
    if (arguments.size() >= 2 && !is_undefined(argument(1))) {
      auto value = require_string(argument(1), call_node, name);
      if (!value) {
        return std::unexpected(value.error());
      }
      picture = std::move(*value);
    }
    std::optional<std::string> timezone;
    if (arguments.size() >= 3 && !is_undefined(argument(2))) {
      auto value = require_string(argument(2), call_node, name);
      if (!value) {
        return std::unexpected(value.error());
      }
      timezone = std::move(*value);
    }
    auto formatted = format_datetime_picture(
        *millis, picture ? &*picture : nullptr, timezone ? &*timezone : nullptr,
        program_.source, call_node.span.end);
    return formatted ? Result<Value>{Value{std::move(*formatted)}}
                     : Result<Value>{std::unexpected(formatted.error())};
  }
  if (name == "toMillis") {
    const auto source_value = context_default(0);
    if (is_undefined(source_value)) {
      return undefined();
    }
    auto timestamp = require_string(source_value, call_node, name);
    if (!timestamp) {
      return std::unexpected(timestamp.error());
    }
    std::optional<std::string> picture;
    if (arguments.size() >= 2 && !is_undefined(argument(1))) {
      auto value = require_string(argument(1), call_node, name);
      if (!value) {
        return std::unexpected(value.error());
      }
      picture = std::move(*value);
    }
    auto interrupted = check_interrupt(call_node);
    if (!interrupted) {
      return std::unexpected(interrupted.error());
    }
    DateTimeRegexBudget regex_budget{
        .limits = regex_limits(),
        .matches = &state_->regex_matches,
        .max_matches = state_->request.limits.max_regex_matches,
    };
    auto parsed = parse_datetime_picture(
        *timestamp, picture ? &*picture : nullptr, state_->request.timestamp,
        program_.source, call_node.span.end, &regex_budget);
    if (!parsed) {
      return std::unexpected(parsed.error());
    }
    interrupted = check_interrupt(call_node);
    if (!interrupted) {
      return std::unexpected(interrupted.error());
    }
    return *parsed ? Result<Value>{Value{**parsed}}
                   : Result<Value>{undefined()};
  }
  if (name == "random") {
    return Value{std::generate_canonical<double, 53>(state_->random_engine)};
  }
  if (name == "type" || name == "typeOf") {
    const auto value = context_default(0);
    return is_undefined(value)
               ? Result<Value>{undefined()}
               : Result<Value>{Value{std::string{runtime_type(value)}}};
  }
  if (name == "millis") {
    return Value{static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            state_->request.timestamp.time_since_epoch())
            .count())};
  }
  if (name == "now") {
    const auto millis = static_cast<double>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            state_->request.timestamp.time_since_epoch())
            .count());
    auto formatted = format_datetime_picture(
        millis, nullptr, nullptr, program_.source, call_node.span.end);
    return formatted ? Result<Value>{Value{std::move(*formatted)}}
                     : Result<Value>{std::unexpected(formatted.error())};
  }
  if (name == "eval") {
    const auto expression_value = argument(0);
    if (is_undefined(expression_value)) {
      return undefined();
    }
    auto expression = require_string(expression_value, call_node, name);
    if (!expression) {
      return std::unexpected(expression.error());
    }
    if (eval_depth_ >= state_->request.limits.max_eval_nesting) {
      return std::unexpected(host_failure("H2003",
                                          "JSONata nested eval limit exceeded",
                                          program_.source, call_node.span.end));
    }
    const auto deadline = state_->request.limits.timeout >
                                  std::chrono::steady_clock::duration::zero()
                              ? std::optional{state_->deadline}
                              : std::nullopt;
    Parser parser(
        *expression, program_.compile_limits,
        CompileInterrupt{.stop_token = state_->request.stop_token,
                         .deadline = deadline,
                         .diagnostic_source = program_.source,
                         .diagnostic_byte_offset = call_node.span.end});
    auto parsed = parser.parse();
    if (!parsed) {
      if (parsed.error().code == "H1001" || parsed.error().code == "D1012") {
        return std::unexpected(parsed.error());
      }
      return std::unexpected(dynamic_failure(
          "D3120",
          std::format("Syntax error in expression passed to $eval: {}",
                      parsed.error().message),
          program_.source, call_node.span.end, parsed.error().token));
    }
    auto nested_program = std::make_shared<ProgramData>(std::move(*parsed));
    Evaluator nested(*nested_program, state_, eval_depth_ + 1, nested_program);
    nested.root_ = root_;
    nested.base_environment_ = base_environment_;
    nested.environment_ = environment;
    const auto nested_input = arguments.size() >= 2 ? arguments[1] : input;
    auto result = nested.evaluate(nested_program->root, nested_input,
                                  environment, call_depth + 1);
    if (!result) {
      if (result.error().kind == FailureKind::Host) {
        return std::unexpected(result.error());
      }
      return std::unexpected(dynamic_failure(
          "D3121",
          std::format("Dynamic error evaluating expression passed to $eval: {}",
                      result.error().message),
          program_.source, call_node.span.end, result.error().token));
    }
    return result;
  }
  if (name == "assert") {
    if (!effective_boolean(argument(0))) {
      const auto message = is_undefined(argument(1))
                               ? std::string{"Assertion failed"}
                               : value_to_string(argument(1));
      return std::unexpected(dynamic_failure("D3141", message, program_.source,
                                             call_node.span.end));
    }
    return undefined();
  }
  if (name == "error") {
    const auto message = is_undefined(argument(0))
                             ? std::string{"$error() function evaluated"}
                             : value_to_string(argument(0));
    return std::unexpected(
        dynamic_failure("D3137", message, program_.source, call_node.span.end));
  }

  return std::unexpected(host_failure(
      "H9002", std::format("Builtin '${}' is not implemented", name),
      program_.source, call_node.span.end));
}

} // namespace dagforge::jsonata::detail
