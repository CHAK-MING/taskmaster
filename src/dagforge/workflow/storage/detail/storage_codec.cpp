#include "storage_codec.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/plan_validator.hpp"

#include "../../detail/sha256.hpp"
#include "checkpoint_validator.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge::workflow {
namespace storage_codec_detail {

constexpr std::uint32_t kStorageEnvelopeVersion = 1;
constexpr std::string_view kArtifactMetadataFormat =
    "dagforge.artifact-metadata";
constexpr std::string_view kCheckpointFormat = "dagforge.checkpoint";
constexpr std::string_view kEvidenceFormat = "dagforge.evidence";
constexpr std::string_view kStoredPlanFormat = "dagforge.stored-plan";

struct StorageEnvelopeHeader {
  std::string format;
  std::uint32_t version{0};
};

template <typename T> struct StorageEnvelope {
  std::string format;
  std::uint32_t version{kStorageEnvelopeVersion};
  T payload;
};

template <typename T>
[[nodiscard]] auto encode_envelope(std::string_view format, const T &payload)
    -> Result<std::string> {
  return serialize_json(StorageEnvelope<T>{
      .format = std::string{format},
      .payload = payload,
  });
}

template <typename T>
[[nodiscard]] auto decode_envelope(std::string_view json,
                                   std::string_view format) -> Result<T> {
  auto header = parse_json_as_allow_unknown<StorageEnvelopeHeader>(json);
  if (!header) {
    return fail(header.error());
  }
  if (header->format != format) {
    return fail(Error::ParseError);
  }
  if (header->version > kStorageEnvelopeVersion) {
    return fail(Error::Unsupported);
  }
  if (header->version != kStorageEnvelopeVersion) {
    return fail(Error::ParseError);
  }
  auto envelope = parse_json_as<StorageEnvelope<T>>(json);
  if (!envelope || envelope->format != format ||
      envelope->version != kStorageEnvelopeVersion) {
    return fail(Error::ParseError);
  }
  return ok(std::move(envelope->payload));
}

[[nodiscard]] auto valid_artifact(const ArtifactRef &artifact) -> bool {
  return !artifact.artifact_id.empty() && !artifact.media_type.empty() &&
         !artifact.digest.empty();
}

[[nodiscard]] auto valid_evidence(const EvidenceRecord &record) -> bool {
  return !record.evidence_id.empty() && !record.run_id.empty() &&
         record.metadata.is_object() &&
         (!record.artifact || valid_artifact(*record.artifact));
}

} // namespace storage_codec_detail

namespace storage_detail {

using namespace storage_codec_detail;

auto compute_digest(std::span<const std::byte> data) -> Result<std::string> {
  return detail::sha256_hex(data);
}

auto encode_artifact_metadata(const ArtifactRef &artifact)
    -> Result<std::string> {
  if (!valid_artifact(artifact)) {
    return fail(Error::InvalidArgument);
  }
  return encode_envelope(kArtifactMetadataFormat, artifact);
}

auto decode_artifact_metadata(std::string_view json) -> Result<ArtifactRef> {
  auto artifact = decode_envelope<ArtifactRef>(json, kArtifactMetadataFormat);
  if (!artifact || !valid_artifact(*artifact)) {
    return fail(artifact ? Error::ParseError : artifact.error());
  }
  return artifact;
}

auto encode_evidence(const EvidenceRecord &record) -> Result<std::string> {
  if (!valid_evidence(record)) {
    return fail(Error::InvalidArgument);
  }
  return encode_envelope(kEvidenceFormat, record);
}

auto decode_evidence(std::string_view json) -> Result<EvidenceRecord> {
  switch (classify_json_input(json)) {
  case JsonInputState::Incomplete:
    return fail(Error::Incomplete);
  case JsonInputState::Invalid:
    return fail(Error::ParseError);
  case JsonInputState::Valid:
    break;
  }
  auto record = decode_envelope<EvidenceRecord>(json, kEvidenceFormat);
  if (!record || !valid_evidence(*record)) {
    return fail(record ? Error::ParseError : record.error());
  }
  return record;
}

auto encode_checkpoint(const WorkflowCheckpoint &checkpoint)
    -> Result<std::string> {
  auto validated = validate_checkpoint(checkpoint);
  if (!validated) {
    return fail(validated.error());
  }
  return encode_envelope(kCheckpointFormat, checkpoint);
}

auto decode_checkpoint(std::string_view json) -> Result<WorkflowCheckpoint> {
  auto checkpoint =
      decode_envelope<WorkflowCheckpoint>(json, kCheckpointFormat);
  if (!checkpoint) {
    return fail(checkpoint.error());
  }
  auto validated = validate_checkpoint(*checkpoint);
  if (!validated) {
    return fail(Error::ParseError);
  }
  return checkpoint;
}

auto validate_checkpoint(const WorkflowCheckpoint &checkpoint) -> Result<void> {
  auto validated_plan = PlanValidator{}.validate_model(checkpoint.plan);
  if (!validated_plan) {
    return fail(validated_plan.error().error_code());
  }
  return detail::validate_checkpoint_model(checkpoint);
}

auto encode_stored_plan(const StoredPlan &plan) -> Result<std::string> {
  if (plan.plan_id.empty() || plan.execution_digest.empty() ||
      plan.source_digest.empty()) {
    return fail(Error::InvalidArgument);
  }
  auto validated = PlanValidator{}.validate(plan.source_plan);
  if (!validated) {
    return fail(validated.error().error_code());
  }
  auto source_digest = PlanCompiler::digest(plan.source_plan);
  if (!source_digest || *source_digest != plan.source_digest) {
    return fail(Error::InvalidArgument);
  }
  return encode_envelope(kStoredPlanFormat, plan);
}

auto decode_stored_plan(std::string_view json) -> Result<StoredPlan> {
  auto stored = decode_envelope<StoredPlan>(json, kStoredPlanFormat);
  if (!stored) {
    return fail(stored.error());
  }
  if (stored->plan_id.empty() || stored->execution_digest.empty() ||
      stored->source_digest.empty()) {
    return fail(Error::ParseError);
  }
  auto validated = PlanValidator{}.validate(stored->source_plan);
  if (!validated) {
    return fail(validated.error().error_code());
  }
  auto source_digest = PlanCompiler::digest(stored->source_plan);
  if (!source_digest || *source_digest != stored->source_digest) {
    return fail(Error::ParseError);
  }
  return stored;
}

} // namespace storage_detail
} // namespace dagforge::workflow
