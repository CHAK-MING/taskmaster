#pragma once

#include "dagforge/util/typed_id.hpp"

#include <string_view>

namespace dagforge {

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

template <> struct TypedIdTraits<InstanceTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 512,
  };
};

template <> struct TypedIdTraits<WorkflowTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<WorkflowRunTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 192,
  };
};

template <> struct TypedIdTraits<WorkflowNodeTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<WorkflowPlanTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<WorkflowPortTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<WorkflowTriggerTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<ArtifactTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<EvidenceTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
};

template <> struct TypedIdTraits<AttemptTag> {
  static constexpr IdTextRules rules{
      .policy = IdTextPolicy::AllowEmptyNoControl,
      .max_bytes = 128,
  };
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

namespace detail {
inline constexpr std::string_view kRunSeparator = "__";
}

[[nodiscard]] auto generate_workflow_run_id(const WorkflowId &workflow_id)
    -> WorkflowRunId;
[[nodiscard]] auto generate_workflow_plan_id() -> WorkflowPlanId;
[[nodiscard]] auto generate_workflow_trigger_id() -> WorkflowTriggerId;
[[nodiscard]] auto generate_artifact_id() -> ArtifactId;
[[nodiscard]] auto generate_evidence_id() -> EvidenceId;
[[nodiscard]] auto generate_attempt_id() -> AttemptId;

} // namespace dagforge
