#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/evidence_types.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#endif

namespace dagforge::workflow {

struct EvidenceRecord {
  EvidenceId evidence_id;
  WorkflowRunId run_id;
  WorkflowNodeId node_id;
  EvidenceType type{EvidenceType::TriggerReceived};
  std::chrono::system_clock::time_point timestamp{
      std::chrono::system_clock::now()};
  Principal actor;
  JsonValue metadata;
  std::optional<ArtifactRef> artifact;
  std::string content_digest;
};

class EvidenceLedger {
public:
  explicit EvidenceLedger(std::size_t max_records = 100'000)
      : max_records_(max_records) {}
  EvidenceLedger(std::filesystem::path file,
                 std::size_t max_records = 100'000);

  [[nodiscard]] auto append(EvidenceRecord record) -> Result<EvidenceId>;
  [[nodiscard]] auto records(const WorkflowRunId &run_id) const
      -> std::vector<EvidenceRecord>;
  [[nodiscard]] auto size() const -> std::size_t;

private:
  auto load_file() -> void;
  auto append_file(const EvidenceRecord &record) -> Result<void>;
  auto rewrite_file() -> Result<void>;

  mutable std::mutex mutex_;
  std::vector<EvidenceRecord> records_;
  std::filesystem::path file_;
  std::size_t max_records_{100'000};
};

} // namespace dagforge::workflow
