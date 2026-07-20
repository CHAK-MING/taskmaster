#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/plan_store.hpp"
#include "dagforge/workflow/workflow_capabilities.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge::workflow {

struct PlanRegistration {
  std::shared_ptr<const ExecutionPlan> plan;
  bool durability_deferred{false};

  [[nodiscard]] auto operator->() const noexcept -> const ExecutionPlan * {
    return plan.get();
  }
  [[nodiscard]] auto operator*() const noexcept -> const ExecutionPlan & {
    return *plan;
  }
  [[nodiscard]]
  operator const std::shared_ptr<const ExecutionPlan> &() const noexcept {
    return plan;
  }
};

class WorkflowControlPlane {
public:
  explicit WorkflowControlPlane(
      const ExecutorRegistry &executors,
      PlanValidator validator = PlanValidator{config::AdmissionConfig{}},
      std::shared_ptr<PlanStore> plan_store = {});

  [[nodiscard]] auto register_plan(WorkflowPlan plan)
      -> PlanResult<PlanRegistration>;
  [[nodiscard]] auto restore_plan(WorkflowPlan plan,
                                  const WorkflowPlanId &plan_id,
                                  std::string_view expected_digest = {})
      -> PlanResult<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto get_latest(const WorkflowId &workflow_id) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto get_plan(const WorkflowPlanId &plan_id) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto list_plans() const
      -> std::vector<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto capabilities() const -> Result<WorkflowCapabilities>;

private:
  PlanCompiler compiler_;
  const ExecutorRegistry *executors_;
  std::optional<config::AdmissionConfig> admission_;
  std::shared_ptr<PlanStore> plan_store_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_id_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_digest_;
  std::unordered_map<std::string, bool> durability_deferred_by_plan_id_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      latest_by_workflow_;
};

} // namespace dagforge::workflow
