#pragma once

#include "model.hpp"

#include <chrono>
#include <cstddef>
#include <stop_token>
#include <string_view>

namespace dagforge::jsonata::detail {

[[nodiscard]] auto import_json(const JsonValue &json,
                               const EvaluationLimits &limits,
                               std::stop_token stop_token,
                               std::chrono::steady_clock::time_point deadline,
                               std::string_view source, std::size_t byte_offset)
    -> Result<Value>;

} // namespace dagforge::jsonata::detail
