#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::workflow {

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
