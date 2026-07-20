#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

#include "regex_adapter.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge::jsonata::detail {

struct RegexProgram {
  explicit RegexProgram(pcre2_code *compiled) : compiled(compiled) {}
  ~RegexProgram() { pcre2_code_free(compiled); }

  RegexProgram(const RegexProgram &) = delete;
  auto operator=(const RegexProgram &) -> RegexProgram & = delete;

  pcre2_code *compiled{};
};

namespace {

[[nodiscard]] auto pcre_message(int error_code) -> std::string {
  std::string message(256, '\0');
  const auto size = pcre2_get_error_message(
      error_code, reinterpret_cast<PCRE2_UCHAR *>(message.data()),
      message.size());
  if (size < 0) {
    return "regular expression error";
  }
  message.resize(static_cast<std::size_t>(size));
  return message;
}

[[nodiscard]] auto match_failure(int code, std::string_view source,
                                 std::size_t byte_offset) -> Failure {
  switch (code) {
  case PCRE2_ERROR_MATCHLIMIT:
  case PCRE2_ERROR_DEPTHLIMIT:
  case PCRE2_ERROR_HEAPLIMIT:
    return host_failure("H2103", "Regular expression resource limit exceeded",
                        source, byte_offset);
  case PCRE2_ERROR_BADUTFOFFSET:
  case PCRE2_ERROR_UTF8_ERR1:
  case PCRE2_ERROR_UTF8_ERR2:
  case PCRE2_ERROR_UTF8_ERR3:
  case PCRE2_ERROR_UTF8_ERR4:
  case PCRE2_ERROR_UTF8_ERR5:
  case PCRE2_ERROR_UTF8_ERR6:
  case PCRE2_ERROR_UTF8_ERR7:
  case PCRE2_ERROR_UTF8_ERR8:
  case PCRE2_ERROR_UTF8_ERR9:
  case PCRE2_ERROR_UTF8_ERR10:
  case PCRE2_ERROR_UTF8_ERR11:
  case PCRE2_ERROR_UTF8_ERR12:
  case PCRE2_ERROR_UTF8_ERR13:
  case PCRE2_ERROR_UTF8_ERR14:
  case PCRE2_ERROR_UTF8_ERR15:
  case PCRE2_ERROR_UTF8_ERR16:
  case PCRE2_ERROR_UTF8_ERR17:
  case PCRE2_ERROR_UTF8_ERR18:
  case PCRE2_ERROR_UTF8_ERR19:
  case PCRE2_ERROR_UTF8_ERR20:
  case PCRE2_ERROR_UTF8_ERR21:
    return dynamic_failure("D1004", "Invalid UTF-8 in regular expression input",
                           source, byte_offset);
  default:
    return dynamic_failure("D1004", pcre_message(code), source, byte_offset);
  }
}

} // namespace

auto compile_regex(std::string pattern, std::string flags,
                   std::string_view source, std::size_t byte_offset)
    -> Result<std::shared_ptr<RegexValue>> {
  std::uint32_t options = PCRE2_UTF | PCRE2_ALT_BSUX;
  if (flags.contains('i')) {
    options |= PCRE2_CASELESS;
  }
  if (flags.contains('m')) {
    options |= PCRE2_MULTILINE;
  }

  int error_code = 0;
  PCRE2_SIZE error_offset = 0;
  auto *compiled = pcre2_compile(reinterpret_cast<PCRE2_SPTR>(pattern.data()),
                                 pattern.size(), options, &error_code,
                                 &error_offset, nullptr);
  if (compiled == nullptr) {
    return std::unexpected(
        syntax_failure("S0302", pcre_message(error_code), source,
                       byte_offset + static_cast<std::size_t>(error_offset)));
  }

  auto program = std::make_shared<RegexProgram>(compiled);
  return std::make_shared<RegexValue>(RegexValue{
      .pattern = std::move(pattern),
      .flags = std::move(flags),
      .program = std::move(program),
  });
}

auto search_regex(const RegexValue &regex, std::string_view input,
                  std::size_t start_offset, RegexLimits limits,
                  std::string_view source, std::size_t byte_offset)
    -> Result<std::optional<RegexMatch>> {
  if (!regex.program || !regex.program->compiled) {
    return std::unexpected(host_failure(
        "H9004", "JSONata runtime contains an invalid regular expression",
        source, byte_offset));
  }
  if (start_offset > input.size()) {
    return std::optional<RegexMatch>{};
  }

  auto *match_data =
      pcre2_match_data_create_from_pattern(regex.program->compiled, nullptr);
  auto *match_context = pcre2_match_context_create(nullptr);
  if (match_data == nullptr || match_context == nullptr) {
    pcre2_match_data_free(match_data);
    pcre2_match_context_free(match_context);
    return std::unexpected(host_failure(
        "H2004", "Unable to allocate regular expression match state", source,
        byte_offset));
  }
  const auto match_guard =
      std::unique_ptr<pcre2_match_data, decltype(&pcre2_match_data_free)>(
          match_data, &pcre2_match_data_free);
  const auto context_guard =
      std::unique_ptr<pcre2_match_context, decltype(&pcre2_match_context_free)>(
          match_context, &pcre2_match_context_free);
  pcre2_set_match_limit(match_context, limits.match_limit);
  pcre2_set_depth_limit(match_context, limits.depth_limit);
  pcre2_set_heap_limit(match_context, limits.heap_limit_kib);

  const auto result = pcre2_match(
      regex.program->compiled, reinterpret_cast<PCRE2_SPTR>(input.data()),
      input.size(), start_offset, 0, match_data, match_context);
  if (result == PCRE2_ERROR_NOMATCH) {
    return std::optional<RegexMatch>{};
  }
  if (result < 0) {
    return std::unexpected(match_failure(result, source, byte_offset));
  }

  const auto *ovector = pcre2_get_ovector_pointer(match_data);
  RegexMatch match{
      .start = static_cast<std::size_t>(ovector[0]),
      .end = static_cast<std::size_t>(ovector[1]),
  };
  match.text = std::string{input.substr(match.start, match.end - match.start)};
  const auto capture_count = pcre2_get_ovector_count(match_data);
  match.groups.reserve(capture_count > 0 ? capture_count - 1 : 0);
  for (std::uint32_t group = 1; group < capture_count; ++group) {
    const auto begin = ovector[group * 2U];
    const auto end = ovector[group * 2U + 1U];
    if (begin == PCRE2_UNSET || end == PCRE2_UNSET) {
      match.groups.push_back(RegexCapture{});
      continue;
    }
    match.groups.push_back(RegexCapture{
        .matched = true,
        .text =
            std::string{input.substr(static_cast<std::size_t>(begin),
                                     static_cast<std::size_t>(end - begin))},
    });
  }
  return std::optional<RegexMatch>{std::move(match)};
}

} // namespace dagforge::jsonata::detail
