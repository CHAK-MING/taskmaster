#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
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
  [[nodiscard]] auto append(EvidenceRecord record) -> Result<EvidenceId>;
  [[nodiscard]] auto records(const WorkflowRunId &run_id) const
      -> std::vector<EvidenceRecord>;
  [[nodiscard]] auto size() const -> std::size_t;

private:
  mutable std::mutex mutex_;
  std::vector<EvidenceRecord> records_;
};

struct WorkflowCheckpoint {
  WorkflowRunId run_id;
  WorkflowPlanId plan_id;
  RunState state{RunState::Running};
  std::optional<StopIntent> stop_intent;
  std::string stop_reason;
  std::vector<TaskSnapshot> tasks;
  std::vector<std::pair<OutputRef, WorkflowValue>> values;
  std::chrono::system_clock::time_point created_at{
      std::chrono::system_clock::now()};
};

class CheckpointStore {
public:
  auto save(WorkflowCheckpoint checkpoint) -> Result<void>;
  [[nodiscard]] auto load(const WorkflowRunId &run_id) const
      -> Result<WorkflowCheckpoint>;
  auto erase(const WorkflowRunId &run_id) -> Result<void>;

private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, WorkflowCheckpoint> checkpoints_;
};

} // namespace dagforge::workflow
