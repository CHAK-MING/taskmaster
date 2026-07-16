#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <glaze/json/chrono_format.hpp>

#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

struct WorkflowCheckpoint {
  WorkflowPlan plan;
  TriggerEnvelope trigger;
  RunSnapshot snapshot;
  std::vector<OutputValue> values;
  std::chrono::system_clock::time_point created_at{
      std::chrono::system_clock::now()};
};

struct CheckpointEraseResult {
  bool removed{false};
  bool durability_deferred{false};
};

struct CheckpointSaveResult {
  bool durability_deferred{false};
};

class CheckpointStore {
public:
  CheckpointStore() = default;
  CheckpointStore(std::filesystem::path directory,
                  std::size_t max_checkpoint_bytes);

  auto save(WorkflowCheckpoint checkpoint) -> Result<CheckpointSaveResult>;
  [[nodiscard]] auto load(const WorkflowRunId &run_id) const
      -> Result<WorkflowCheckpoint>;
  auto erase(const WorkflowRunId &run_id) -> Result<CheckpointEraseResult>;
  [[nodiscard]] auto list() const -> Result<std::vector<WorkflowCheckpoint>>;

private:
  [[nodiscard]] auto file_path(const WorkflowRunId &run_id) const
      -> std::filesystem::path;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, WorkflowCheckpoint> checkpoints_;
  std::filesystem::path directory_;
  std::size_t max_checkpoint_bytes_{0};
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::WorkflowCheckpoint> {
  using T = dagforge::workflow::WorkflowCheckpoint;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    return key == "created_at" ? "created_at_ms" : key;
  }
  static constexpr auto modify = object(
      "created_at_ms", epoch_count<std::chrono::milliseconds>(&T::created_at));
};

} // namespace glz
