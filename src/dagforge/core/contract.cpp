#include "dagforge/core/contract.hpp"

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>

namespace dagforge {

auto contract_violation(std::string_view message,
                        std::source_location origin) noexcept -> void {
  const auto message_size = static_cast<int>(
      std::min<std::size_t>(message.size(), static_cast<std::size_t>(INT_MAX)));
  std::fprintf(stderr, "DAGForge contract violation at %s:%u in %s: %.*s\n",
               origin.file_name(), origin.line(), origin.function_name(),
               message_size, message.empty() ? "" : message.data());
  std::fflush(stderr);
  std::abort();
}

} // namespace dagforge
