#pragma once

#include "dagforge/workflow/evidence_ledger.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

namespace dagforge::api_detail::workflow_contract {

struct StartRunRequest {
  std::optional<WorkflowPlanId> plan_id;
  std::string source{"api"};
  std::string event_type{"request"};
  std::optional<JsonPayload> payload;
  std::string idempotency_key;
  workflow::Principal principal;
  workflow::TraceContext trace;
};

struct RepairRunRequest {
  std::optional<workflow::WorkflowPlan> plan;
  std::string reason;
  std::string idempotency_key;
};

struct PlanSummary {
  WorkflowId workflow_id;
  WorkflowPlanId plan_id;
  std::string digest;
  std::size_t nodes{0};
};

struct ArtifactResponse {
  std::string_view type{"artifact"};
  ArtifactId artifact_id;
  std::string media_type;
  std::uint64_t size_bytes{0};
  std::string digest;
};

using WorkflowValueResponse =
    std::variant<std::monostate, bool, std::int64_t, double, std::string,
                 JsonPayload, ArtifactResponse>;

struct EvidenceResponseRecord {
  EvidenceId evidence_id;
  WorkflowRunId run_id;
  WorkflowNodeId node_id;
  workflow::EvidenceType type{workflow::EvidenceType::TriggerReceived};
  std::string actor;
  JsonPayload metadata;
  std::optional<ArtifactResponse> artifact;
  std::string content_digest;
};

[[nodiscard]] inline auto artifact_response(const workflow::ArtifactRef &ref)
    -> ArtifactResponse {
  return ArtifactResponse{
      .artifact_id = ref.artifact_id.clone(),
      .media_type = ref.media_type,
      .size_bytes = ref.size_bytes,
      .digest = ref.digest,
  };
}

[[nodiscard]] inline auto
workflow_value_response(const workflow::WorkflowValue &value)
    -> WorkflowValueResponse {
  return std::visit(
      [](const auto &item) -> WorkflowValueResponse {
        using T = std::remove_cvref_t<decltype(item)>;
        if constexpr (std::is_same_v<T, workflow::ArtifactRef>) {
          return artifact_response(item);
        } else {
          return item;
        }
      },
      value);
}

[[nodiscard]] inline auto plan_summary(const workflow::ExecutionPlan &plan)
    -> PlanSummary {
  return PlanSummary{
      .workflow_id = plan.workflow_id.clone(),
      .plan_id = plan.plan_id.clone(),
      .digest = plan.digest,
      .nodes = plan.nodes.size(),
  };
}

[[nodiscard]] inline auto
evidence_response(const workflow::EvidenceRecord &record)
    -> EvidenceResponseRecord {
  return EvidenceResponseRecord{
      .evidence_id = record.evidence_id.clone(),
      .run_id = record.run_id.clone(),
      .node_id = record.node_id.clone(),
      .type = record.type,
      .actor = record.actor.subject,
      .metadata = record.metadata,
      .artifact = record.artifact
                      ? std::optional{artifact_response(*record.artifact)}
                      : std::nullopt,
      .content_digest = record.content_digest,
  };
}

} // namespace dagforge::api_detail::workflow_contract
