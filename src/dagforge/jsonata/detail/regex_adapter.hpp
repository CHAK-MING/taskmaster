#pragma once

#include "model.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge::jsonata::detail {

struct RegexCapture {
  bool matched{false};
  std::string text;
};

struct RegexMatch {
  std::size_t start{};
  std::size_t end{};
  std::string text;
  std::vector<RegexCapture> groups;
};

struct RegexLimits {
  std::uint32_t match_limit{1'000'000};
  std::uint32_t depth_limit{1'000};
  std::uint32_t heap_limit_kib{64 * 1024};
};

[[nodiscard]] auto compile_regex(std::string pattern, std::string flags,
                                 std::string_view source,
                                 std::size_t byte_offset)
    -> Result<std::shared_ptr<RegexValue>>;

[[nodiscard]] auto search_regex(const RegexValue &regex, std::string_view input,
                                std::size_t start_offset, RegexLimits limits,
                                std::string_view source,
                                std::size_t byte_offset)
    -> Result<std::optional<RegexMatch>>;

} // namespace dagforge::jsonata::detail
