#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/admission_policy.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/plan_store.hpp"
#include "dagforge/workflow/workflow_plan.hpp"

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge::workflow {

class WorkflowControlPlane {
public:
  explicit WorkflowControlPlane(const ExecutorRegistry &executors,
                                AdmissionPolicy admission = {},
                                std::shared_ptr<PlanStore> plan_store = {});

  [[nodiscard]] auto register_plan(WorkflowPlan plan)
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto restore_plan(WorkflowPlan plan,
                                  const WorkflowPlanId &plan_id,
                                  std::string_view expected_digest = {})
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto get_latest(const WorkflowId &workflow_id) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto get_plan(const WorkflowPlanId &plan_id) const
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto list_plans() const
      -> std::vector<std::shared_ptr<const ExecutionPlan>>;

private:
  PlanCompiler compiler_;
  AdmissionPolicy admission_;
  std::shared_ptr<PlanStore> plan_store_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_id_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_digest_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      latest_by_workflow_;
};

} // namespace dagforge::workflow
