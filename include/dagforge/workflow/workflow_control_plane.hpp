#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/workflow/admission_policy.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_types.hpp"

#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#endif

namespace dagforge::workflow {

class WorkflowPlanLoader {
public:
  [[nodiscard]] static auto from_json(std::string_view text)
      -> Result<WorkflowPlan>;
  [[nodiscard]] static auto from_toml(std::string_view text)
      -> Result<WorkflowPlan>;
  [[nodiscard]] static auto to_json(const WorkflowPlan &plan)
      -> Result<std::string>;
};

class WorkflowControlPlane {
public:
  WorkflowControlPlane();
  explicit WorkflowControlPlane(PlanCompiler compiler,
                                AdmissionPolicy admission = {});

  [[nodiscard]] auto register_plan(WorkflowPlan plan)
      -> Result<std::shared_ptr<const ExecutionPlan>>;
  [[nodiscard]] auto restore_plan(WorkflowPlan plan,
                                  const WorkflowPlanId &plan_id)
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
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_id_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      plans_by_digest_;
  std::unordered_map<std::string, std::shared_ptr<const ExecutionPlan>>
      latest_by_workflow_;
};

} // namespace dagforge::workflow
