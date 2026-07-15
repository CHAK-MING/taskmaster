#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/artifact_store.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"
#include "dagforge/workflow/evidence_ledger.hpp"

#include <filesystem>
#include <span>
#include <string>
#include <string_view>

namespace dagforge::workflow::storage_detail {

[[nodiscard]] auto compute_digest(std::span<const std::byte> data)
    -> Result<std::string>;

[[nodiscard]] auto encode_artifact_metadata(const ArtifactRef &artifact)
    -> Result<std::string>;
[[nodiscard]] auto decode_artifact_metadata(std::string_view json)
    -> Result<ArtifactRef>;

[[nodiscard]] auto encode_evidence(const EvidenceRecord &record)
    -> Result<std::string>;
[[nodiscard]] auto decode_evidence(std::string_view json)
    -> Result<EvidenceRecord>;

[[nodiscard]] auto encode_checkpoint(const WorkflowCheckpoint &checkpoint)
    -> Result<std::string>;
[[nodiscard]] auto decode_checkpoint(std::string_view json)
    -> Result<WorkflowCheckpoint>;
[[nodiscard]] auto validate_checkpoint(const WorkflowCheckpoint &checkpoint)
    -> Result<void>;

[[nodiscard]] auto load_text_file(const std::filesystem::path &path)
    -> Result<std::string>;
auto store_text_file_atomic(const std::filesystem::path &path,
                            std::string_view text) -> Result<void>;

} // namespace dagforge::workflow::storage_detail
