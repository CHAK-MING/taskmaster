#include "dagforge/util/id.hpp"

#include <boost/uuid/time_generator_v7.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <format>
#include <string>

namespace dagforge {

namespace {

[[nodiscard]] auto generate_uuid_v7() -> std::string {
  thread_local boost::uuids::time_generator_v7 generator;
  return boost::uuids::to_string(generator());
}

} // namespace

auto generate_workflow_run_id(const WorkflowId &workflow_id) -> WorkflowRunId {
  return WorkflowRunId{std::format("{}{}{}", workflow_id, detail::kRunSeparator,
                                   generate_uuid_v7())};
}

auto generate_workflow_plan_id() -> WorkflowPlanId {
  return WorkflowPlanId{generate_uuid_v7()};
}

auto generate_workflow_trigger_id() -> WorkflowTriggerId {
  return WorkflowTriggerId{generate_uuid_v7()};
}

auto generate_artifact_id() -> ArtifactId {
  return ArtifactId{generate_uuid_v7()};
}

auto generate_evidence_id() -> EvidenceId {
  return EvidenceId{generate_uuid_v7()};
}

auto generate_attempt_id() -> AttemptId {
  return AttemptId{generate_uuid_v7()};
}

} // namespace dagforge
