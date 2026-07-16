#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/workflow/artifact_store.hpp"
#include "dagforge/workflow/workflow_value.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge {
class Runtime;
}

namespace dagforge::workflow {

class RunValueStore {
public:
  RunValueStore(Runtime &runtime, shard_id owner, IArtifactStore &artifacts,
                std::uint64_t max_total_output_bytes,
                std::size_t artifact_threshold_bytes = 256 * 1024);

  [[nodiscard]] auto put(OutputRef output, WorkflowValue value) -> Result<void>;
  [[nodiscard]] auto get(const OutputRef &output) const
      -> Result<std::shared_ptr<const WorkflowValue>>;
  [[nodiscard]] auto contains(const OutputRef &output) const -> bool;
  [[nodiscard]] auto snapshot() const -> Result<std::vector<OutputValue>>;
  auto erase_node(const WorkflowNodeId &node_id) -> Result<void>;

  [[nodiscard]] auto total_output_bytes() const noexcept -> std::uint64_t {
    return total_output_bytes_;
  }

private:
  struct PreparedValue {
    WorkflowValue value;
    std::optional<ArtifactId> owned_artifact_id;
  };

  struct Entry {
    std::shared_ptr<const WorkflowValue> value;
    std::uint64_t accounted_bytes{0};
    std::optional<ArtifactId> owned_artifact_id;
  };

  [[nodiscard]] auto ensure_owner() const -> Result<void>;
  [[nodiscard]] auto maybe_externalize(WorkflowValue value)
      -> Result<PreparedValue>;
  auto erase_owned_artifact(const std::optional<ArtifactId> &artifact_id)
      -> Result<void>;

  Runtime &runtime_;
  shard_id owner_;
  IArtifactStore &artifacts_;
  std::uint64_t max_total_output_bytes_;
  std::size_t artifact_threshold_bytes_;
  std::uint64_t total_output_bytes_{0};
  std::unordered_map<OutputRef, Entry, OutputRefHash> values_;
};

} // namespace dagforge::workflow
