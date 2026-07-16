#include "dagforge/workflow/workflow_value.hpp"

#include <format>
#include <string>
#include <type_traits>
#include <variant>

namespace dagforge::workflow {

auto workflow_value_text(const WorkflowValue &value) -> std::string {
  return std::visit(
      [](const auto &typed) -> std::string {
        using T = std::remove_cvref_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return {};
        } else if constexpr (std::same_as<T, bool>) {
          return typed ? "true" : "false";
        } else if constexpr (std::same_as<T, std::int64_t> ||
                             std::same_as<T, double>) {
          return std::format("{}", typed);
        } else if constexpr (std::same_as<T, std::string>) {
          return typed;
        } else if constexpr (std::same_as<T, JsonPayload>) {
          return std::string{typed.encoded()};
        } else {
          return typed.artifact_id.str();
        }
      },
      value);
}

} // namespace dagforge::workflow
