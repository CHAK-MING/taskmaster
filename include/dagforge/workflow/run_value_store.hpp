#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/shard.hpp"
#include "dagforge/workflow/workflow_storage.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <cstddef>
#include <memory>
#include <string>
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
  [[nodiscard]] auto snapshot() const
      -> Result<std::vector<std::pair<OutputRef, WorkflowValue>>>;
  auto erase_node(const WorkflowNodeId &node_id) -> Result<void>;

  [[nodiscard]] auto total_output_bytes() const noexcept -> std::uint64_t {
    return total_output_bytes_;
  }

private:
  struct Entry {
    OutputRef output;
    std::shared_ptr<const WorkflowValue> value;
    std::uint64_t accounted_bytes{0};
  };

  [[nodiscard]] auto ensure_owner() const -> Result<void>;
  [[nodiscard]] static auto key(const OutputRef &output) -> std::string;
  [[nodiscard]] auto maybe_externalize(WorkflowValue value)
      -> Result<std::pair<WorkflowValue, std::uint64_t>>;

  Runtime &runtime_;
  shard_id owner_;
  IArtifactStore &artifacts_;
  std::uint64_t max_total_output_bytes_;
  std::size_t artifact_threshold_bytes_;
  std::uint64_t total_output_bytes_{0};
  std::unordered_map<std::string, Entry> values_;
};

} // namespace dagforge::workflow
