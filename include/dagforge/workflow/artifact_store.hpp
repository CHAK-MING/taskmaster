#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

struct ArtifactBlob {
  ArtifactRef ref;
  std::vector<std::byte> data;
};

struct ArtifactPutResult : ArtifactRef {
  bool durability_deferred{false};

  ArtifactPutResult() = default;
  explicit ArtifactPutResult(ArtifactRef ref,
                             bool deferred = false) noexcept
      : ArtifactRef(std::move(ref)), durability_deferred(deferred) {}

  [[nodiscard]] auto take_ref() && noexcept -> ArtifactRef {
    return std::move(static_cast<ArtifactRef &>(*this));
  }
};

struct ArtifactEraseResult {
  bool logical_deleted{false};
  bool cleanup_deferred{false};
  bool durability_deferred{false};
};

enum class ArtifactReconciliationState : std::uint8_t {
  Complete,
  OrphanData,
  OrphanMetadata,
  MalformedMetadata,
  ContentMismatch,
  InvalidEntry,
};

struct ArtifactReconciliationEntry {
  std::string storage_key;
  ArtifactReconciliationState state{ArtifactReconciliationState::Complete};
};

struct ArtifactReconciliationReport {
  std::vector<ArtifactReconciliationEntry> entries;

  [[nodiscard]] auto count(ArtifactReconciliationState state) const noexcept
      -> std::size_t;
  [[nodiscard]] auto clean() const noexcept -> bool;
};

class IArtifactStore {
public:
  virtual ~IArtifactStore() = default;

  [[nodiscard]] virtual auto put(std::span<const std::byte> data,
                                 std::string media_type)
      -> Result<ArtifactPutResult> = 0;
  [[nodiscard]] virtual auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> = 0;
  virtual auto erase(const ArtifactId &artifact_id)
      -> Result<ArtifactEraseResult> = 0;
};

class InMemoryArtifactStore final : public IArtifactStore {
public:
  [[nodiscard]] auto put(std::span<const std::byte> data,
                         std::string media_type)
      -> Result<ArtifactPutResult> override;
  [[nodiscard]] auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> override;
  auto erase(const ArtifactId &artifact_id)
      -> Result<ArtifactEraseResult> override;

  [[nodiscard]] auto size() const -> std::size_t;

private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, ArtifactBlob> artifacts_;
};

class FileArtifactStore final : public IArtifactStore {
public:
  FileArtifactStore(std::filesystem::path directory,
                    std::size_t max_metadata_bytes,
                    std::size_t max_artifact_bytes);

  [[nodiscard]] auto put(std::span<const std::byte> data,
                         std::string media_type)
      -> Result<ArtifactPutResult> override;
  [[nodiscard]] auto get(const ArtifactId &artifact_id) const
      -> Result<ArtifactBlob> override;
  auto erase(const ArtifactId &artifact_id)
      -> Result<ArtifactEraseResult> override;

  [[nodiscard]] auto reconcile() const
      -> Result<ArtifactReconciliationReport>;

private:
  std::filesystem::path directory_;
  std::size_t max_metadata_bytes_{0};
  std::size_t max_artifact_bytes_{0};
  mutable std::mutex mutex_;
};

} // namespace dagforge::workflow
