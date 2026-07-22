#pragma once

#include "dagforge/core/error.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <glaze/json/chrono_format.hpp>

#include <chrono>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace dagforge::workflow {

struct StoredPlan {
  WorkflowPlanId plan_id;
  std::string execution_digest;
  std::string source_digest;
  WorkflowPlan source_plan;
  std::chrono::system_clock::time_point created_at{
      std::chrono::system_clock::now()};
};

struct PlanSaveResult {
  bool durability_deferred{false};
};

class PlanStore {
public:
  PlanStore() = default;
  PlanStore(std::filesystem::path directory, std::size_t max_plan_bytes);

  auto save(const ExecutionPlan &plan) -> Result<PlanSaveResult>;
  [[nodiscard]] auto load(const WorkflowPlanId &plan_id) const
      -> Result<StoredPlan>;
  [[nodiscard]] auto list() const -> Result<std::vector<StoredPlan>>;

private:
  [[nodiscard]] auto file_path(const WorkflowPlanId &plan_id) const
      -> std::filesystem::path;

  mutable std::mutex mutex_;
  std::unordered_map<std::string, StoredPlan> plans_;
  std::unordered_map<std::string, bool> durability_deferred_;
  std::filesystem::path directory_;
  std::size_t max_plan_bytes_{0};
};

} // namespace dagforge::workflow

namespace glz {

template <> struct meta<dagforge::workflow::StoredPlan> {
  using T = dagforge::workflow::StoredPlan;
  static constexpr auto rename_key(std::string_view key) -> std::string_view {
    return key == "created_at" ? "created_at_ms" : key;
  }
  static constexpr auto modify = object(
      "created_at_ms", epoch_count<std::chrono::milliseconds>(&T::created_at));
};

} // namespace glz
