#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge::workflow {

struct ArtifactBlob {
  ArtifactRef ref;
  std::vector<std::byte> data;
};

class IArtifactStore {
public:
  virtual ~IArtifactStore() = default;

  [[nodiscard]] virtual auto put(std::span<const std::byte> data,
                                 std::string media_type)
      -> Result<ArtifactRef> = 0;
  [[nodiscard]] virtual auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> = 0;
  virtual auto erase(const ArtifactId &artifact_id) -> Result<void> = 0;
};

class InMemoryArtifactStore final : public IArtifactStore {
public:
  [[nodiscard]] auto put(std::span<const std::byte> data,
                         std::string media_type) -> Result<ArtifactRef> override;
  [[nodiscard]] auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> override;
  auto erase(const ArtifactId &artifact_id) -> Result<void> override;

  [[nodiscard]] auto size() const -> std::size_t;

private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, ArtifactBlob> artifacts_;
};

class FileArtifactStore final : public IArtifactStore {
public:
  explicit FileArtifactStore(std::filesystem::path directory);

  [[nodiscard]] auto put(std::span<const std::byte> data,
                         std::string media_type) -> Result<ArtifactRef> override;
  [[nodiscard]] auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> override;
  auto erase(const ArtifactId &artifact_id) -> Result<void> override;

private:
  std::filesystem::path directory_;
  mutable std::mutex mutex_;
};

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

struct WorkflowCheckpoint {
  WorkflowPlan plan;
  TriggerEnvelope trigger;
  RunSnapshot snapshot;
  std::vector<std::pair<OutputRef, WorkflowValue>> values;
  std::chrono::system_clock::time_point created_at{
      std::chrono::system_clock::now()};
};

class CheckpointStore {
public:
  CheckpointStore() = default;
  explicit CheckpointStore(std::filesystem::path directory);

  auto save(WorkflowCheckpoint checkpoint) -> Result<void>;
  [[nodiscard]] auto load(const WorkflowRunId &run_id) const
      -> Result<WorkflowCheckpoint>;
  auto erase(const WorkflowRunId &run_id) -> Result<void>;
  [[nodiscard]] auto list() const -> Result<std::vector<WorkflowCheckpoint>>;

private:
  [[nodiscard]] auto file_path(const WorkflowRunId &run_id) const
      -> std::filesystem::path;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, WorkflowCheckpoint> checkpoints_;
  std::filesystem::path directory_;
};

} // namespace dagforge::workflow
