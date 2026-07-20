#include "dagforge/jsonata/program.hpp"
#include "dagforge/util/json.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <print>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

namespace fs = std::filesystem;
using dagforge::JsonValue;
using dagforge::jsonata::Binding;
using dagforge::jsonata::CompileRequest;
using dagforge::jsonata::EvaluationRequest;
using dagforge::jsonata::EvaluationSuccess;
using dagforge::jsonata::EvaluationValueKind;
using dagforge::jsonata::Failure;
using dagforge::jsonata::Program;

struct Counts {
  std::size_t total{};
  std::size_t passed{};
  std::size_t failed{};
};

struct SurrogateMarker {
  std::string marker;
  std::uint16_t code_unit{};
};

[[nodiscard]] auto fixture_timeout_scale() noexcept -> std::uint64_t {
  constexpr std::uint64_t maximum_scale = 100;
  const auto *raw = std::getenv("DAGFORGE_JSONATA_TIMEOUT_SCALE");
  if (raw == nullptr || *raw == '\0') {
    return 1;
  }
  std::uint64_t scale{};
  const auto end = raw + std::char_traits<char>::length(raw);
  const auto parsed = std::from_chars(raw, end, scale);
  return parsed.ec == std::errc{} && parsed.ptr == end && scale > 0 &&
                 scale <= maximum_scale
             ? scale
             : 1;
}

[[nodiscard]] auto hex_digit(char value) -> std::optional<unsigned> {
  if (value >= '0' && value <= '9') {
    return static_cast<unsigned>(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return static_cast<unsigned>(value - 'a' + 10);
  }
  if (value >= 'A' && value <= 'F') {
    return static_cast<unsigned>(value - 'A' + 10);
  }
  return std::nullopt;
}

[[nodiscard]] auto escaped_code_unit(std::string_view text, std::size_t offset)
    -> std::optional<std::uint16_t> {
  if (offset + 6 > text.size() || text[offset] != '\\' ||
      text[offset + 1] != 'u') {
    return std::nullopt;
  }
  std::uint16_t result{};
  for (std::size_t index = 0; index < 4; ++index) {
    const auto digit = hex_digit(text[offset + 2 + index]);
    if (!digit) {
      return std::nullopt;
    }
    result = static_cast<std::uint16_t>((result << 4U) | *digit);
  }
  return result;
}

[[nodiscard]] auto surrogate_utf8(std::uint16_t code_unit) -> std::string {
  const auto value = static_cast<std::uint32_t>(code_unit);
  return std::string{
      static_cast<char>(0xE0U | (value >> 12U)),
      static_cast<char>(0x80U | ((value >> 6U) & 0x3FU)),
      static_cast<char>(0x80U | (value & 0x3FU)),
  };
}

[[nodiscard]] auto
sanitize_lone_surrogates(std::string text,
                         std::vector<SurrogateMarker> &markers) -> std::string {
  for (std::size_t offset = 0; offset + 6 <= text.size();) {
    const auto code_unit = escaped_code_unit(text, offset);
    if (!code_unit || *code_unit < 0xD800U || *code_unit > 0xDFFFU) {
      ++offset;
      continue;
    }

    std::size_t preceding_slashes = 0;
    for (auto cursor = offset; cursor > 0 && text[cursor - 1] == '\\';
         --cursor) {
      ++preceding_slashes;
    }
    if ((preceding_slashes % 2U) != 0U) {
      ++offset;
      continue;
    }

    if (*code_unit <= 0xDBFFU) {
      const auto low = escaped_code_unit(text, offset + 6);
      if (low && *low >= 0xDC00U && *low <= 0xDFFFU) {
        offset += 12;
        continue;
      }
    }

    const auto marker =
        "__DAGFORGE_JSONATA_SURROGATE_" + std::to_string(markers.size()) + "__";
    markers.push_back(
        SurrogateMarker{.marker = marker, .code_unit = *code_unit});
    text.replace(offset, 6, marker);
    offset += marker.size();
  }
  return text;
}

auto restore_lone_surrogates(JsonValue &value,
                             std::span<const SurrogateMarker> markers) -> void {
  if (value.is_string()) {
    auto &text = value.get_string();
    for (const auto &marker : markers) {
      for (std::size_t offset = 0;
           (offset = text.find(marker.marker, offset)) != std::string::npos;) {
        const auto replacement = surrogate_utf8(marker.code_unit);
        text.replace(offset, marker.marker.size(), replacement);
        offset += replacement.size();
      }
    }
    return;
  }
  if (value.is_array()) {
    for (auto &item : value.get_array()) {
      restore_lone_surrogates(item, markers);
    }
    return;
  }
  if (value.is_object()) {
    for (auto &[key, item] : value.get_object()) {
      (void)key;
      restore_lone_surrogates(item, markers);
    }
  }
}

[[nodiscard]] auto read_text(const fs::path &path)
    -> std::optional<std::string> {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return std::nullopt;
  }
  return std::string{std::istreambuf_iterator<char>{input},
                     std::istreambuf_iterator<char>{}};
}

[[nodiscard]] auto member(const JsonValue &value, std::string_view key)
    -> const JsonValue * {
  if (!value.is_object()) {
    return nullptr;
  }
  const auto found = value.get_object().find(key);
  return found == value.get_object().end() ? nullptr : &found->second;
}

[[nodiscard]] auto string_member(const JsonValue &value, std::string_view key)
    -> std::optional<std::string> {
  const auto *found = member(value, key);
  if (found == nullptr || !found->is_string()) {
    return std::nullopt;
  }
  return found->get_string();
}

[[nodiscard]] auto bool_member(const JsonValue &value, std::string_view key)
    -> bool {
  const auto *found = member(value, key);
  return found != nullptr && found->is_boolean() && found->get_boolean();
}

[[nodiscard]] auto json_equal(const JsonValue &left, const JsonValue &right,
                              bool unordered = false) -> bool {
  if (left.is_null() || right.is_null()) {
    return left.is_null() && right.is_null();
  }
  if (left.is_boolean() || right.is_boolean()) {
    return left.is_boolean() && right.is_boolean() &&
           left.get_boolean() == right.get_boolean();
  }
  if (left.is_number() || right.is_number()) {
    return left.is_number() && right.is_number() &&
           left.as_number() == right.as_number();
  }
  if (left.is_string() || right.is_string()) {
    return left.is_string() && right.is_string() &&
           left.get_string() == right.get_string();
  }
  if (left.is_array() || right.is_array()) {
    if (!left.is_array() || !right.is_array() ||
        left.get_array().size() != right.get_array().size()) {
      return false;
    }
    if (!unordered) {
      for (std::size_t index = 0; index < left.get_array().size(); ++index) {
        if (!json_equal(left.get_array()[index], right.get_array()[index])) {
          return false;
        }
      }
      return true;
    }
    std::vector<bool> matched(right.get_array().size(), false);
    for (const auto &left_item : left.get_array()) {
      bool found = false;
      for (std::size_t index = 0; index < right.get_array().size(); ++index) {
        if (!matched[index] &&
            json_equal(left_item, right.get_array()[index])) {
          matched[index] = true;
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }
  if (!left.is_object() || !right.is_object() ||
      left.get_object().size() != right.get_object().size()) {
    return false;
  }
  for (const auto &[key, left_item] : left.get_object()) {
    const auto found = right.get_object().find(key);
    if (found == right.get_object().end() ||
        !json_equal(left_item, found->second)) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] auto encode(const JsonValue &value) -> std::string {
  auto encoded = dagforge::serialize_json(value);
  return encoded ? std::move(*encoded) : std::string{"<json-error>"};
}

struct CaseContext {
  fs::path suite;
  fs::path file;
  std::size_t index{};
};

[[nodiscard]] auto case_name(const CaseContext &context) -> std::string {
  auto relative = fs::relative(context.file, context.suite).string();
  return context.index == 0
             ? relative
             : relative + "[" + std::to_string(context.index) + "]";
}

[[nodiscard]] auto load_json(const fs::path &path) -> std::optional<JsonValue> {
  auto text = read_text(path);
  if (!text) {
    return std::nullopt;
  }
  std::vector<SurrogateMarker> markers;
  auto parsed =
      dagforge::parse_json(sanitize_lone_surrogates(std::move(*text), markers));
  if (parsed && !markers.empty()) {
    restore_lone_surrogates(*parsed, markers);
  }
  return parsed ? std::optional<JsonValue>{std::move(*parsed)} : std::nullopt;
}

[[nodiscard]] auto run_case(const JsonValue &test, const CaseContext &context,
                            std::string &reason) -> bool {
  auto expression = string_member(test, "expr");
  if (!expression) {
    auto expression_file = string_member(test, "expr-file");
    if (!expression_file) {
      reason = "missing expr/expr-file";
      return false;
    }
    expression = read_text(context.file.parent_path() / *expression_file);
  }
  if (!expression) {
    reason = "cannot read expression";
    return false;
  }

  std::optional<JsonValue> input;
  if (const auto *data = member(test, "data")) {
    input = *data;
  } else if (const auto *dataset = member(test, "dataset");
             dataset != nullptr && !dataset->is_null()) {
    if (!dataset->is_string()) {
      reason = "unsupported dataset identifier";
      return false;
    }
    input = load_json(context.suite / "datasets" /
                      (dataset->get_string() + ".json"));
    if (!input) {
      reason = "cannot load dataset";
      return false;
    }
  }

  std::vector<JsonValue> binding_values;
  std::vector<Binding> bindings;
  if (const auto *binding_object = member(test, "bindings");
      binding_object != nullptr && binding_object->is_object()) {
    binding_values.reserve(binding_object->get_object().size());
    bindings.reserve(binding_object->get_object().size());
    for (const auto &[name, value] : binding_object->get_object()) {
      binding_values.push_back(value);
      bindings.push_back(
          Binding{.name = name, .value = std::cref(binding_values.back())});
    }
  }

  const auto expected_code = string_member(test, "code");
  const auto expects_error = expected_code.has_value() || member(test, "error");
  auto program = Program::compile(CompileRequest{.source = *expression});
  if (!program) {
    if (!expects_error) {
      reason = "compile failed " + program.error().code + " at " +
               std::to_string(program.error().position) + ": " +
               program.error().message;
      return false;
    }
    if (expected_code && program.error().code != *expected_code) {
      reason = "compile code " + program.error().code + " expected " +
               *expected_code;
      return false;
    }
    return true;
  }

  EvaluationRequest request{.bindings = bindings};
  request.limits.timeout = std::chrono::steady_clock::duration::zero();
  request.limits.max_value_nodes = request.limits.max_sequence_items + 1U;
  if (input) {
    request.input = std::cref(*input);
  }
  const auto *time_limit = member(test, "timelimit");
  const auto *depth_limit = member(test, "depth");
  if (time_limit != nullptr && time_limit->is_number()) {
    const auto milliseconds =
        static_cast<std::uint64_t>(time_limit->as_number());
    const auto scale = fixture_timeout_scale();
    const auto maximum =
        static_cast<std::uint64_t>(std::numeric_limits<long long>::max());
    const auto scaled =
        milliseconds > maximum / scale ? maximum : milliseconds * scale;
    request.limits.timeout =
        std::chrono::milliseconds{static_cast<long long>(scaled)};
  }
  if (depth_limit != nullptr && depth_limit->is_number()) {
    // The official JavaScript harness counts three evaluator entry callbacks
    // per ordinary recursive lambda frame. DAGForge exposes logical function
    // depth instead, so adapt the fixture limit at this test seam.
    request.limits.max_call_depth = std::max<std::size_t>(
        1, static_cast<std::size_t>(depth_limit->as_number()) / 3U);
  }

  auto actual = program->evaluate(request);
  if (!actual) {
    auto actual_code = actual.error().code;
    if (time_limit != nullptr && depth_limit != nullptr &&
        (actual_code == "D1011" || actual_code == "D1012")) {
      actual_code = "U1001";
    }
    if (!expects_error) {
      reason = "evaluation failed " + actual_code + " at " +
               std::to_string(actual.error().position) + ": " +
               actual.error().message;
      return false;
    }
    if (expected_code && actual_code != *expected_code) {
      reason = "evaluation code " + actual_code + " expected " + *expected_code;
      return false;
    }
    return true;
  }
  if (expects_error) {
    reason = "expected error";
    return false;
  }
  if (bool_member(test, "undefinedResult")) {
    if (actual->kind != EvaluationValueKind::Undefined) {
      reason = "expected undefined";
      return false;
    }
    return true;
  }
  const auto *expected = member(test, "result");
  if (expected == nullptr) {
    reason = "missing expected result";
    return false;
  }
  if (actual->kind != EvaluationValueKind::Json || !actual->value) {
    reason = "expected JSON result";
    return false;
  }
  if (!json_equal(*actual->value, *expected, bool_member(test, "unordered"))) {
    reason =
        "actual=" + encode(*actual->value) + " expected=" + encode(*expected);
    return false;
  }
  return true;
}

auto run_file(const fs::path &suite, const fs::path &file, Counts &counts,
              std::size_t &printed_failures) -> void {
  auto document = load_json(file);
  if (!document) {
    ++counts.total;
    ++counts.failed;
    std::println("FAIL {}: cannot parse case file", file.string());
    return;
  }
  std::vector<const JsonValue *> cases;
  if (document->is_array()) {
    for (const auto &item : document->get_array()) {
      cases.push_back(&item);
    }
  } else {
    cases.push_back(&*document);
  }
  for (std::size_t index = 0; index < cases.size(); ++index) {
    ++counts.total;
    std::string reason;
    const CaseContext context{.suite = suite, .file = file, .index = index};
    if (run_case(*cases[index], context, reason)) {
      ++counts.passed;
    } else {
      ++counts.failed;
      if (printed_failures < 100) {
        ++printed_failures;
        std::println("FAIL {}: {}", case_name(context), reason);
      }
    }
  }
}

} // namespace

auto main(int argc, char **argv) -> int {
  if (argc < 2 || argc > 3) {
    std::println(
        "usage: jsonata-conformance <test-suite-directory> [path-filter]");
    return 2;
  }
  const fs::path suite = fs::canonical(argv[1]);
  const std::string_view filter = argc == 3 ? argv[2] : std::string_view{};
  const auto groups = suite / "groups";
  if (!fs::is_directory(groups) || !fs::is_directory(suite / "datasets")) {
    std::println("invalid JSONata test-suite directory: {}", suite.string());
    return 2;
  }

  std::vector<fs::path> files;
  for (const auto &entry : fs::recursive_directory_iterator(groups)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      const auto relative = fs::relative(entry.path(), suite).string();
      if (filter.empty() || relative.contains(filter)) {
        files.push_back(entry.path());
      }
    }
  }
  std::ranges::sort(files);

  Counts counts;
  std::size_t printed_failures = 0;
  for (const auto &file : files) {
    run_file(suite, file, counts, printed_failures);
  }
  std::println("JSONata conformance: passed={} failed={} total={}",
               counts.passed, counts.failed, counts.total);
  return counts.failed == 0 ? 0 : 1;
}
