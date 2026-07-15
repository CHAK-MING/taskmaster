#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge::workflow {

struct StoredPlan {
  WorkflowPlanId plan_id;
  std::string digest;
  WorkflowPlan plan;
  std::chrono::system_clock::time_point created_at{
      std::chrono::system_clock::now()};
};

class PlanStore {
public:
  PlanStore() = default;
  explicit PlanStore(std::filesystem::path directory);

  auto save(const ExecutionPlan &plan) -> Result<void>;
  [[nodiscard]] auto load(const WorkflowPlanId &plan_id) const
      -> Result<StoredPlan>;
  [[nodiscard]] auto list() const -> Result<std::vector<StoredPlan>>;

private:
  [[nodiscard]] auto file_path(const WorkflowPlanId &plan_id) const
      -> std::filesystem::path;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, StoredPlan> plans_;
  std::filesystem::path directory_;
};

} // namespace dagforge::workflow
