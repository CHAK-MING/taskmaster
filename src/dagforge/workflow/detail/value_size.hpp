#pragma once

#include "dagforge/workflow/workflow_value.hpp"

#include <cstdint>
#include <type_traits>

namespace dagforge::workflow::detail {

[[nodiscard]] inline auto value_size_bytes(const WorkflowValue &value)
    -> std::uint64_t {
  return std::visit(
      [](const auto &typed) -> std::uint64_t {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return 0;
        } else if constexpr (std::is_same_v<T, bool>) {
          return 1;
        } else if constexpr (std::is_same_v<T, std::int64_t> ||
                             std::is_same_v<T, double>) {
          return sizeof(T);
        } else if constexpr (std::is_same_v<T, std::string>) {
          return typed.size();
        } else if constexpr (std::is_same_v<T, JsonPayload>) {
          return typed.size();
        } else if constexpr (std::is_same_v<T, ArtifactRef>) {
          return typed.size_bytes;
        }
        return 0;
      },
      value);
}

} // namespace dagforge::workflow::detail
