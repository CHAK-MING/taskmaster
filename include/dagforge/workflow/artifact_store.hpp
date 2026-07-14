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

} // namespace dagforge::workflow
