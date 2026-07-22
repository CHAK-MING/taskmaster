module;

#include "dagforge/util/id.hpp"
#include "dagforge/util/typed_id.hpp"

export module dagforge.domain;

export namespace dagforge {
using ::dagforge::ArtifactId;
using ::dagforge::ArtifactTag;
using ::dagforge::AttemptId;
using ::dagforge::AttemptTag;
using ::dagforge::EvidenceId;
using ::dagforge::EvidenceTag;
using ::dagforge::generate_artifact_id;
using ::dagforge::generate_attempt_id;
using ::dagforge::generate_evidence_id;
using ::dagforge::generate_workflow_plan_id;
using ::dagforge::generate_workflow_run_id;
using ::dagforge::generate_workflow_trigger_id;
using ::dagforge::has_control_chars;
using ::dagforge::IdTextPolicy;
using ::dagforge::IdTextRules;
using ::dagforge::InstanceId;
using ::dagforge::InstanceTag;
using ::dagforge::is_valid_id_text;
using ::dagforge::IsTypedId;
using ::dagforge::kDefaultIdTextMaxBytes;
using ::dagforge::operator<<;
using ::dagforge::TypedId;
using ::dagforge::TypedIdTraits;
using ::dagforge::WorkflowId;
using ::dagforge::WorkflowNodeId;
using ::dagforge::WorkflowNodeTag;
using ::dagforge::WorkflowPlanId;
using ::dagforge::WorkflowPlanTag;
using ::dagforge::WorkflowPortId;
using ::dagforge::WorkflowPortTag;
using ::dagforge::WorkflowRunId;
using ::dagforge::WorkflowRunTag;
using ::dagforge::WorkflowTag;
using ::dagforge::WorkflowTriggerId;
using ::dagforge::WorkflowTriggerTag;
} // namespace dagforge
