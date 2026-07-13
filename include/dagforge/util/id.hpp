#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <boost/uuid/time_generator_v7.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cctype>
#include <compare>
#include <concepts>
#include <cstddef>
#include <format>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#endif

namespace dagforge {

[[nodiscard]] inline auto has_control_chars(std::string_view value) noexcept
    -> bool {
  return std::any_of(value.begin(), value.end(),
                     [](unsigned char ch) { return std::iscntrl(ch) != 0; });
}

[[nodiscard]] inline auto is_valid_id_text(std::string_view value) noexcept
    -> bool {
  return !value.empty() && !has_control_chars(value);
}

struct InstanceTag {};
struct WorkflowTag {};
struct WorkflowRunTag {};
struct WorkflowNodeTag {};
struct WorkflowPlanTag {};
struct WorkflowPortTag {};
struct WorkflowTriggerTag {};
struct ArtifactTag {};
struct EvidenceTag {};
struct AttemptTag {};

template <typename Tag> class TypedId {
public:
  explicit TypedId(std::string value) : value_(std::move(value)) {}
  explicit TypedId(std::string_view value) : value_(value) {}
  explicit TypedId(const char *value) : value_(value ? value : "") {}

  TypedId() = default;

  [[nodiscard]] auto value() const noexcept -> std::string_view {
    return value_;
  }
  [[nodiscard]] auto str() const noexcept -> const std::string & {
    return value_;
  }
  [[nodiscard]] auto c_str() const noexcept -> const char * {
    return value_.c_str();
  }

  [[nodiscard]] explicit operator const std::string &() const noexcept {
    return value_;
  }
  [[nodiscard]] explicit operator std::string_view() const noexcept {
    return value_;
  }

  [[nodiscard]] friend auto operator<=>(const TypedId &lhs,
                                        const TypedId &rhs) = default;
  [[nodiscard]] friend auto operator==(const TypedId &lhs, const TypedId &rhs)
      -> bool = default;

  [[nodiscard]] friend auto operator==(const TypedId &lhs,
                                       std::string_view rhs) noexcept -> bool {
    return lhs.value_ == rhs;
  }
  [[nodiscard]] friend auto operator==(std::string_view lhs,
                                       const TypedId &rhs) noexcept -> bool {
    return lhs == rhs.value_;
  }

  [[nodiscard]] friend auto operator<(const TypedId &lhs,
                                      std::string_view rhs) noexcept -> bool {
    return std::string_view{lhs.value_} < rhs;
  }
  [[nodiscard]] friend auto operator<(std::string_view lhs,
                                      const TypedId &rhs) noexcept -> bool {
    return lhs < std::string_view{rhs.value_};
  }

  [[nodiscard]] auto clone() const -> TypedId { return TypedId{value_}; }
  [[nodiscard]] auto empty() const noexcept -> bool { return value_.empty(); }
  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return value_.size();
  }

private:
  std::string value_;
};

using InstanceId = TypedId<InstanceTag>;
using WorkflowId = TypedId<WorkflowTag>;
using WorkflowRunId = TypedId<WorkflowRunTag>;
using WorkflowNodeId = TypedId<WorkflowNodeTag>;
using WorkflowPlanId = TypedId<WorkflowPlanTag>;
using WorkflowPortId = TypedId<WorkflowPortTag>;
using WorkflowTriggerId = TypedId<WorkflowTriggerTag>;
using ArtifactId = TypedId<ArtifactTag>;
using EvidenceId = TypedId<EvidenceTag>;
using AttemptId = TypedId<AttemptTag>;

template <typename T>
concept IsTypedId = requires(T id) {
  { id.value() } -> std::convertible_to<std::string_view>;
  { id.empty() } -> std::convertible_to<bool>;
};

template <typename Tag>
inline auto operator<<(std::ostream &os, const TypedId<Tag> &id)
    -> std::ostream & {
  return os << id.value();
}

} // namespace dagforge

template <typename Tag> struct std::hash<dagforge::TypedId<Tag>> {
  using is_avalanching = void;

  auto operator()(const dagforge::TypedId<Tag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <typename Tag>
struct std::formatter<dagforge::TypedId<Tag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<Tag> &id, auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};

namespace dagforge {

namespace detail {
[[nodiscard]] inline auto generate_uuid_v7() -> std::string {
  thread_local boost::uuids::time_generator_v7 generator;
  return boost::uuids::to_string(generator());
}

inline constexpr std::string_view kRunSeparator = "__";
} // namespace detail

[[nodiscard]] inline auto generate_workflow_run_id(const WorkflowId &workflow_id)
    -> WorkflowRunId {
  return WorkflowRunId{std::format("{}{}{}", workflow_id,
                                   detail::kRunSeparator,
                                   detail::generate_uuid_v7())};
}

[[nodiscard]] inline auto generate_workflow_plan_id() -> WorkflowPlanId {
  return WorkflowPlanId{detail::generate_uuid_v7()};
}

[[nodiscard]] inline auto generate_workflow_trigger_id() -> WorkflowTriggerId {
  return WorkflowTriggerId{detail::generate_uuid_v7()};
}

[[nodiscard]] inline auto generate_artifact_id() -> ArtifactId {
  return ArtifactId{detail::generate_uuid_v7()};
}

[[nodiscard]] inline auto generate_evidence_id() -> EvidenceId {
  return EvidenceId{detail::generate_uuid_v7()};
}

[[nodiscard]] inline auto generate_attempt_id() -> AttemptId {
  return AttemptId{detail::generate_uuid_v7()};
}

} // namespace dagforge
