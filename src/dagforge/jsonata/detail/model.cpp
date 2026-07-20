#include "model.hpp"

#include <array>
#include <charconv>
#include <cmath>
#include <format>
#include <ranges>

namespace dagforge::jsonata::detail {

namespace {

[[nodiscard]] auto saturated_add(std::size_t left, std::size_t right) noexcept
    -> std::size_t {
  return right > std::numeric_limits<std::size_t>::max() - left
             ? std::numeric_limits<std::size_t>::max()
             : left + right;
}

auto add_footprint(ValueFootprint &target, const ValueFootprint &value) noexcept
    -> void {
  target.nodes = saturated_add(target.nodes, value.nodes);
  target.string_bytes = saturated_add(target.string_bytes, value.string_bytes);
  target.peak_sequence_items =
      std::max(target.peak_sequence_items, value.peak_sequence_items);
}

} // namespace

auto value_footprint(const Value &value) noexcept -> ValueFootprint {
  if (const auto *string = std::get_if<std::string>(&value.storage)) {
    return ValueFootprint{.nodes = 1, .string_bytes = string->size()};
  }
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&value.storage)) {
    return *array ? (*array)->footprint : ValueFootprint{};
  }
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&value.storage)) {
    return *object ? (*object)->footprint : ValueFootprint{};
  }
  if (const auto *sequence =
          std::get_if<std::shared_ptr<Sequence>>(&value.storage)) {
    return *sequence ? (*sequence)->footprint : ValueFootprint{};
  }
  if (const auto *function =
          std::get_if<std::shared_ptr<Function>>(&value.storage)) {
    ValueFootprint result;
    if (*function) {
      result.string_bytes = (*function)->name.size();
      for (const auto &parameter : (*function)->parameters) {
        result.string_bytes =
            saturated_add(result.string_bytes, parameter.size());
      }
    }
    return result;
  }
  if (const auto *regex =
          std::get_if<std::shared_ptr<RegexValue>>(&value.storage)) {
    ValueFootprint result;
    if (*regex) {
      result.string_bytes =
          saturated_add((*regex)->pattern.size(), (*regex)->flags.size());
    }
    return result;
  }
  return ValueFootprint{};
}

auto recompute_footprint(Array &array) noexcept -> void {
  ValueFootprint result;
  for (const auto &value : array.values) {
    add_footprint(result, value_footprint(value));
  }
  array.footprint = result;
}

auto recompute_footprint(Object &object) noexcept -> void {
  ValueFootprint result;
  for (const auto &[key, value] : object.members) {
    result.string_bytes = saturated_add(result.string_bytes, key.size());
    add_footprint(result, value_footprint(value));
  }
  object.footprint = result;
}

auto recompute_footprint(Sequence &sequence) noexcept -> void {
  ValueFootprint result{.nodes = 1,
                        .peak_sequence_items = sequence.values.size()};
  for (const auto &value : sequence.values) {
    add_footprint(result, value_footprint(value));
  }
  sequence.footprint = result;
}

auto runtime_type(const Value &value) -> std::string_view {
  if (is_undefined(value)) {
    return "undefined";
  }
  if (std::holds_alternative<std::nullptr_t>(value.storage)) {
    return "null";
  }
  if (std::holds_alternative<bool>(value.storage)) {
    return "boolean";
  }
  if (std::holds_alternative<double>(value.storage)) {
    return "number";
  }
  if (std::holds_alternative<std::string>(value.storage)) {
    return "string";
  }
  if (std::holds_alternative<std::shared_ptr<Array>>(value.storage)) {
    return "array";
  }
  if (std::holds_alternative<std::shared_ptr<Object>>(value.storage)) {
    return "object";
  }
  if (is_sequence(value)) {
    return "sequence";
  }
  return "function";
}

auto effective_boolean(const Value &raw) -> bool {
  const auto value = normalize_sequence(raw);
  if (is_undefined(value) ||
      std::holds_alternative<std::nullptr_t>(value.storage)) {
    return false;
  }
  if (const auto *boolean = std::get_if<bool>(&value.storage)) {
    return *boolean;
  }
  if (const auto *number = std::get_if<double>(&value.storage)) {
    return *number != 0.0 && !std::isnan(*number);
  }
  if (const auto *string = std::get_if<std::string>(&value.storage)) {
    return !string->empty();
  }
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&value.storage)) {
    if (!*array) {
      return false;
    }
    if ((*array)->values.empty()) {
      return false;
    }
    if ((*array)->values.size() == 1) {
      return effective_boolean((*array)->values.front());
    }
    return std::ranges::any_of((*array)->values, effective_boolean);
  }
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&value.storage)) {
    return *object && !(*object)->members.empty();
  }
  if (std::holds_alternative<std::shared_ptr<Function>>(value.storage) ||
      std::holds_alternative<std::shared_ptr<RegexValue>>(value.storage)) {
    return false;
  }
  return true;
}

namespace {

auto arrays_equal(const Array &left, const Array &right) -> bool {
  return left.values.size() == right.values.size() &&
         std::ranges::equal(left.values, right.values, value_equal);
}

auto objects_equal(const Object &left, const Object &right) -> bool {
  if (left.members.size() != right.members.size()) {
    return false;
  }
  for (const auto &[key, value] : left.members) {
    auto other = object_lookup(right, key);
    if (!other || !value_equal(value, *other)) {
      return false;
    }
  }
  return true;
}

auto json_string_escape(std::string_view input) -> std::string {
  std::string out;
  out.push_back('"');
  for (const auto value : input) {
    switch (value) {
    case '"':
      out += "\\\"";
      break;
    case '\\':
      out += "\\\\";
      break;
    case '\b':
      out += "\\b";
      break;
    case '\f':
      out += "\\f";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (static_cast<unsigned char>(value) < 0x20U) {
        out += std::format("\\u{:04x}", static_cast<unsigned char>(value));
      } else {
        out.push_back(value);
      }
    }
  }
  out.push_back('"');
  return out;
}

[[nodiscard]] auto trim_fixed_number(std::string text) -> std::string {
  const auto point = text.find('.');
  if (point == std::string::npos) {
    return text;
  }
  while (!text.empty() && text.back() == '0') {
    text.pop_back();
  }
  if (!text.empty() && text.back() == '.') {
    text.pop_back();
  }
  return text == "-0" ? std::string{"0"} : text;
}

[[nodiscard]] auto normalize_exponent(std::string text) -> std::string {
  const auto exponent = text.find_first_of("eE");
  if (exponent == std::string::npos) {
    return text;
  }
  auto mantissa = trim_fixed_number(text.substr(0, exponent));
  auto exponent_text = text.substr(exponent + 1);
  char sign = '+';
  if (!exponent_text.empty() &&
      (exponent_text.front() == '+' || exponent_text.front() == '-')) {
    sign = exponent_text.front();
    exponent_text.erase(exponent_text.begin());
  }
  const auto first_digit = exponent_text.find_first_not_of('0');
  exponent_text = first_digit == std::string::npos
                      ? std::string{"0"}
                      : exponent_text.substr(first_digit);
  return mantissa + "e" + sign + exponent_text;
}

[[nodiscard]] auto format_jsonata_number(double value) -> std::string {
  if (value == 0.0) {
    return "0";
  }

  double rounded = value;
  if (std::trunc(value) != value) {
    std::array<char, 64> significant{};
    const auto [end, error] = std::to_chars(
        significant.data(), significant.data() + significant.size(), value,
        std::chars_format::general, 15);
    if (error == std::errc{}) {
      const auto [parsed_end, parsed_error] = std::from_chars(
          significant.data(), end, rounded, std::chars_format::general);
      if (parsed_error != std::errc{} || parsed_end != end) {
        rounded = value;
      }
    }
  }

  const auto magnitude = std::abs(rounded);
  std::array<char, 128> buffer{};
  if (std::trunc(rounded) == rounded && magnitude < 1e21) {
    const auto [end, error] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), rounded,
                      std::chars_format::fixed, 0);
    if (error == std::errc{}) {
      return rounded == 0.0 ? std::string{"0"}
                            : std::string{buffer.data(), end};
    }
  }

  if (magnitude >= 1e-6 && magnitude < 1e21) {
    const auto decimal_exponent =
        static_cast<int>(std::floor(std::log10(magnitude))) + 1;
    const auto precision = std::max(0, 15 - decimal_exponent);
    const auto [end, error] =
        std::to_chars(buffer.data(), buffer.data() + buffer.size(), rounded,
                      std::chars_format::fixed, precision);
    if (error == std::errc{}) {
      return trim_fixed_number(std::string{buffer.data(), end});
    }
  }

  const auto [end, error] =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), rounded,
                    std::chars_format::scientific, 14);
  if (error == std::errc{}) {
    return normalize_exponent(std::string{buffer.data(), end});
  }
  return std::format("{}", rounded);
}

auto append_indent(std::string &out, std::size_t depth) -> void {
  out.append(depth * 2, ' ');
}

auto value_to_json_text(const Value &raw, bool prettify, std::size_t depth = 0)
    -> std::string {
  const auto value = normalize_sequence(raw);
  if (is_undefined(value)) {
    return "undefined";
  }
  if (std::holds_alternative<std::nullptr_t>(value.storage)) {
    return "null";
  }
  if (const auto *boolean = std::get_if<bool>(&value.storage)) {
    return *boolean ? "true" : "false";
  }
  if (const auto *number = std::get_if<double>(&value.storage)) {
    return format_jsonata_number(*number);
  }
  if (const auto *string = std::get_if<std::string>(&value.storage)) {
    return json_string_escape(*string);
  }
  if (const auto *array = std::get_if<std::shared_ptr<Array>>(&value.storage)) {
    std::string out{"["};
    bool first = true;
    for (const auto &item : (*array)->values) {
      if (!first) {
        out += prettify ? ",\n" : ",";
      } else if (prettify && !(*array)->values.empty()) {
        out.push_back('\n');
      }
      first = false;
      if (prettify) {
        append_indent(out, depth + 1);
      }
      const auto encoded = value_to_json_text(item, prettify, depth + 1);
      out += encoded == "undefined" ? "null" : encoded;
    }
    if (prettify && !(*array)->values.empty()) {
      out.push_back('\n');
      append_indent(out, depth);
    }
    out.push_back(']');
    return out;
  }
  if (const auto *object =
          std::get_if<std::shared_ptr<Object>>(&value.storage)) {
    if (!*object) {
      return "null";
    }
    std::string out{"{"};
    bool first = true;
    for (const auto &[key, item] : (*object)->members) {
      if (is_undefined(item)) {
        continue;
      }
      if (!first) {
        out += prettify ? ",\n" : ",";
      } else if (prettify) {
        out.push_back('\n');
      }
      first = false;
      if (prettify) {
        append_indent(out, depth + 1);
      }
      out += json_string_escape(key);
      out += prettify ? ": " : ":";
      out += value_to_json_text(item, prettify, depth + 1);
    }
    if (prettify && !first) {
      out.push_back('\n');
      append_indent(out, depth);
    }
    out.push_back('}');
    return out;
  }
  if (is_sequence(value)) {
    const auto sequence = as_sequence(value);
    return sequence ? value_to_json_text(make_array(sequence->values), prettify,
                                         depth)
                    : "null";
  }
  if (std::holds_alternative<std::shared_ptr<Function>>(value.storage) ||
      std::holds_alternative<std::shared_ptr<RegexValue>>(value.storage)) {
    return "\"\"";
  }
  return "";
}

} // namespace

auto value_equal(const Value &left_raw, const Value &right_raw) -> bool {
  const auto left = normalize_sequence(left_raw);
  const auto right = normalize_sequence(right_raw);
  if (is_undefined(left) || is_undefined(right)) {
    return is_undefined(left) && is_undefined(right);
  }
  if (left.storage.index() != right.storage.index()) {
    return false;
  }
  if (std::holds_alternative<std::nullptr_t>(left.storage)) {
    return true;
  }
  if (const auto *value = std::get_if<bool>(&left.storage)) {
    return *value == std::get<bool>(right.storage);
  }
  if (const auto *value = std::get_if<double>(&left.storage)) {
    return *value == std::get<double>(right.storage);
  }
  if (const auto *value = std::get_if<std::string>(&left.storage)) {
    return *value == std::get<std::string>(right.storage);
  }
  if (const auto *value = std::get_if<std::shared_ptr<Array>>(&left.storage)) {
    const auto &other = std::get<std::shared_ptr<Array>>(right.storage);
    if (!*value || !other) {
      return *value == other;
    }
    return arrays_equal(**value, *other);
  }
  if (const auto *value = std::get_if<std::shared_ptr<Object>>(&left.storage)) {
    const auto &other = std::get<std::shared_ptr<Object>>(right.storage);
    if (!*value || !other) {
      return *value == other;
    }
    return objects_equal(**value, *other);
  }
  if (is_sequence(left)) {
    const auto &left_sequence = as_sequence(left);
    const auto &right_sequence = as_sequence(right);
    if (!left_sequence || !right_sequence) {
      return left_sequence == right_sequence;
    }
    return left_sequence->keep_singleton == right_sequence->keep_singleton &&
           std::ranges::equal(left_sequence->values, right_sequence->values,
                              value_equal);
  }
  if (const auto *function =
          std::get_if<std::shared_ptr<Function>>(&left.storage)) {
    return *function == std::get<std::shared_ptr<Function>>(right.storage);
  }
  if (const auto *regex =
          std::get_if<std::shared_ptr<RegexValue>>(&left.storage)) {
    return *regex == std::get<std::shared_ptr<RegexValue>>(right.storage);
  }
  return false;
}

auto value_to_string(const Value &raw, bool prettify) -> std::string {
  const auto value = normalize_sequence(raw);
  if (is_undefined(value)) {
    return {};
  }
  if (const auto *string = std::get_if<std::string>(&value.storage)) {
    return *string;
  }
  if (std::holds_alternative<std::shared_ptr<Function>>(value.storage) ||
      std::holds_alternative<std::shared_ptr<RegexValue>>(value.storage)) {
    return {};
  }
  return value_to_json_text(value, prettify);
}

auto to_json(const Value &raw, std::string_view source)
    -> Result<std::optional<JsonValue>> {
  struct Frame {
    Value value;
    bool finalize{false};
  };

  std::vector<Frame> pending{{.value = raw}};
  std::vector<std::optional<JsonValue>> converted;
  while (!pending.empty()) {
    auto frame = std::move(pending.back());
    pending.pop_back();
    const auto value = normalize_sequence(std::move(frame.value));

    if (frame.finalize) {
      if (const auto *array =
              std::get_if<std::shared_ptr<Array>>(&value.storage)) {
        JsonValue::array_t out((*array)->values.size());
        for (std::size_t index = out.size(); index > 0; --index) {
          auto child = std::move(converted.back());
          converted.pop_back();
          if (child) {
            out[index - 1] = std::move(*child);
          } else {
            out[index - 1] = nullptr;
          }
        }
        JsonValue result;
        result = std::move(out);
        converted.emplace_back(std::move(result));
        continue;
      }
      if (const auto *object =
              std::get_if<std::shared_ptr<Object>>(&value.storage)) {
        JsonValue::object_t out;
        std::vector<std::optional<JsonValue>> children(
            (*object)->members.size());
        for (std::size_t index = children.size(); index > 0; --index) {
          children[index - 1] = std::move(converted.back());
          converted.pop_back();
        }
        for (std::size_t index = 0; index < children.size(); ++index) {
          if (children[index]) {
            out.emplace((*object)->members[index].first,
                        std::move(*children[index]));
          }
        }
        JsonValue result;
        result = std::move(out);
        converted.emplace_back(std::move(result));
        continue;
      }
      const auto &sequence = as_sequence(value);
      JsonValue::array_t out;
      out.reserve(sequence->values.size());
      std::vector<std::optional<JsonValue>> children(sequence->values.size());
      for (std::size_t index = children.size(); index > 0; --index) {
        children[index - 1] = std::move(converted.back());
        converted.pop_back();
      }
      for (auto &child : children) {
        if (child) {
          out.push_back(std::move(*child));
        }
      }
      JsonValue result;
      result = std::move(out);
      converted.emplace_back(std::move(result));
      continue;
    }

    if (is_undefined(value)) {
      converted.emplace_back(std::nullopt);
      continue;
    }
    if (std::holds_alternative<std::nullptr_t>(value.storage)) {
      JsonValue result;
      result = nullptr;
      converted.emplace_back(std::move(result));
      continue;
    }
    if (const auto *boolean = std::get_if<bool>(&value.storage)) {
      JsonValue result;
      result = *boolean;
      converted.emplace_back(std::move(result));
      continue;
    }
    if (const auto *number = std::get_if<double>(&value.storage)) {
      if (!std::isfinite(*number)) {
        return std::unexpected(dynamic_failure(
            "D1001", "Number cannot be represented as JSON", source, 0));
      }
      JsonValue result;
      result = *number;
      converted.emplace_back(std::move(result));
      continue;
    }
    if (const auto *string = std::get_if<std::string>(&value.storage)) {
      JsonValue result;
      result = *string;
      converted.emplace_back(std::move(result));
      continue;
    }
    if (const auto *array =
            std::get_if<std::shared_ptr<Array>>(&value.storage)) {
      if (!*array) {
        return std::unexpected(host_failure(
            "H9004", "JSONata runtime contains an invalid array value",
            source));
      }
      pending.push_back(Frame{.value = value, .finalize = true});
      for (auto iterator = (*array)->values.rbegin();
           iterator != (*array)->values.rend(); ++iterator) {
        pending.push_back(Frame{.value = *iterator});
      }
      continue;
    }
    if (const auto *object =
            std::get_if<std::shared_ptr<Object>>(&value.storage)) {
      if (!*object) {
        return std::unexpected(host_failure(
            "H9004", "JSONata runtime contains an invalid object value",
            source));
      }
      pending.push_back(Frame{.value = value, .finalize = true});
      for (auto iterator = (*object)->members.rbegin();
           iterator != (*object)->members.rend(); ++iterator) {
        pending.push_back(Frame{.value = iterator->second});
      }
      continue;
    }
    if (is_sequence(value)) {
      const auto &sequence = as_sequence(value);
      if (!sequence) {
        return std::unexpected(host_failure(
            "H9004", "JSONata runtime contains an invalid sequence value",
            source));
      }
      pending.push_back(Frame{.value = value, .finalize = true});
      for (auto iterator = sequence->values.rbegin();
           iterator != sequence->values.rend(); ++iterator) {
        pending.push_back(Frame{.value = *iterator});
      }
      continue;
    }
    return std::unexpected(type_failure(
        "T1006", "The evaluated result is not a JSON value", source, 0));
  }
  if (converted.size() != 1) {
    return std::unexpected(host_failure(
        "H9004", "JSONata JSON conversion stack is inconsistent", source));
  }
  return std::move(converted.back());
}

} // namespace dagforge::jsonata::detail
