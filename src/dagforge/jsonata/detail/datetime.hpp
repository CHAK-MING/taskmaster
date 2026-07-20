#pragma once

#include "model.hpp"
#include "regex_adapter.hpp"

#include <cstddef>
#include <chrono>
#include <string>
#include <string_view>

namespace dagforge::jsonata::detail {

struct DateTimeRegexBudget {
  RegexLimits limits;
  std::size_t *matches{};
  std::size_t max_matches{};
};

[[nodiscard]] auto
format_integer_picture(double value, std::string_view picture,
                       std::string_view source, std::size_t byte_offset)
    -> Result<std::string>;

[[nodiscard]] auto
parse_integer_picture(std::string_view value, std::string_view picture,
                      std::string_view source, std::size_t byte_offset)
    -> Result<double>;

[[nodiscard]] auto format_number_picture(double value, std::string_view picture,
                                         const Object *options,
                                         std::string_view source,
                                         std::size_t byte_offset)
    -> Result<std::string>;

[[nodiscard]] auto
format_datetime_picture(double millis, const std::string *picture,
                        const std::string *timezone, std::string_view source,
                        std::size_t byte_offset) -> Result<std::string>;

[[nodiscard]] auto parse_datetime_picture(
    std::string_view timestamp, const std::string *picture,
    std::chrono::system_clock::time_point evaluation_timestamp,
    std::string_view source, std::size_t byte_offset,
    DateTimeRegexBudget *regex_budget = nullptr)
    -> Result<std::optional<double>>;

} // namespace dagforge::jsonata::detail
