#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/evidence_types.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <glaze/json/chrono_format.hpp>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
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
  JsonPayload metadata;
  std::optional<ArtifactRef> artifact;
  std::string content_digest;
};

struct EvidenceAppendResult {
  EvidenceId evidence_id;
  bool durability_deferred{false};

  [[nodiscard]] auto empty() const noexcept -> bool {
    return evidence_id.empty();
  }
};

class EvidenceLedger {
public:
  explicit EvidenceLedger(std::size_t max_records = 100'000)
      : max_records_(max_records) {}

  [[nodiscard]] static auto open(std::filesystem::path file,
                                 std::size_t max_records,
                                 std::size_t max_file_bytes,
                                 std::size_t max_record_bytes)
      -> Result<std::shared_ptr<EvidenceLedger>>;

  [[nodiscard]] auto append(EvidenceRecord record)
      -> Result<EvidenceAppendResult>;
  [[nodiscard]] auto records(const WorkflowRunId &run_id) const
      -> std::vector<EvidenceRecord>;
  [[nodiscard]] auto size() const -> std::size_t;

private:
  struct WriteResult {
    bool durability_deferred{false};
  };

  EvidenceLedger(std::filesystem::path file, std::size_t max_records,
                 std::size_t max_file_bytes, std::size_t max_record_bytes)
      : file_(std::move(file)), max_records_(max_records),
        max_file_bytes_(max_file_bytes),
        max_record_bytes_(max_record_bytes) {}

  auto load_file() -> Result<void>;
  [[nodiscard]] auto encode_line(const EvidenceRecord &record) const
      -> Result<std::string>;
  auto append_line(std::string_view line) -> Result<WriteResult>;
  auto rewrite_file(const std::vector<EvidenceRecord> &records)
      -> Result<WriteResult>;
  [[nodiscard]] auto compaction_record_threshold() const noexcept
      -> std::size_t;

  mutable std::mutex mutex_;
  std::vector<EvidenceRecord> records_;
  std::filesystem::path file_;
  std::size_t max_records_{100'000};
  std::size_t max_file_bytes_{0};
  std::size_t max_record_bytes_{0};
  std::size_t file_bytes_{0};
  std::size_t stale_records_{0};
  bool durability_deferred_{false};
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::EvidenceRecord> {
  using T = dagforge::workflow::EvidenceRecord;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    return key == "timestamp" ? "timestamp_ms" : key;
  }
  static constexpr auto modify = object(
      "timestamp_ms", epoch_count<std::chrono::milliseconds>(&T::timestamp));
};

} // namespace glz
