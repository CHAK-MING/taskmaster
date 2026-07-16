#pragma once

#include "dagforge/core/error.hpp"

#include <cstddef>
#include <span>
#include <string>
#include <string_view>

namespace dagforge::workflow::detail {

[[nodiscard]] auto sha256_hex(std::span<const std::byte> data)
    -> Result<std::string>;

[[nodiscard]] inline auto sha256_hex(std::string_view data)
    -> Result<std::string> {
  return sha256_hex(std::as_bytes(std::span{data.data(), data.size()}));
}

} // namespace dagforge::workflow::detail
