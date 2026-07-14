#include "dagforge/workflow/workflow_control_plane.hpp"

#include <algorithm>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>

namespace dagforge::workflow {

WorkflowControlPlane::WorkflowControlPlane(const ExecutorRegistry &executors,
                                           AdmissionPolicy admission)
    : compiler_(executors), admission_(std::move(admission)) {}

auto WorkflowControlPlane::register_plan(WorkflowPlan plan)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto admitted = admission_.validate(plan);
  if (!admitted) {
    return fail(admitted.error());
  }
  auto compiled = compiler_.compile(std::move(plan));
  if (!compiled) {
    return fail(compiled.error());
  }

  std::lock_guard lock(mutex_);
  if (const auto existing = plans_by_digest_.find((*compiled)->digest);
      existing != plans_by_digest_.end()) {
    latest_by_workflow_[existing->second->workflow_id.str()] = existing->second;
    return ok(existing->second);
  }
  plans_by_id_[(*compiled)->plan_id.str()] = *compiled;
  plans_by_digest_[(*compiled)->digest] = *compiled;
  latest_by_workflow_[(*compiled)->workflow_id.str()] = *compiled;
  return ok(std::move(*compiled));
}

auto WorkflowControlPlane::restore_plan(WorkflowPlan plan,
                                        const WorkflowPlanId &plan_id)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto admitted = admission_.validate(plan);
  if (!admitted) {
    return fail(admitted.error());
  }
  auto compiled = compiler_.compile(std::move(plan), plan_id);
  if (!compiled) {
    return fail(compiled.error());
  }
  std::lock_guard lock(mutex_);
  plans_by_id_[(*compiled)->plan_id.str()] = *compiled;
  plans_by_digest_[(*compiled)->digest] = *compiled;
  latest_by_workflow_[(*compiled)->workflow_id.str()] = *compiled;
  return ok(std::move(*compiled));
}

auto WorkflowControlPlane::get_latest(const WorkflowId &workflow_id) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  std::lock_guard lock(mutex_);
  const auto it = latest_by_workflow_.find(workflow_id.str());
  if (it == latest_by_workflow_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto WorkflowControlPlane::get_plan(const WorkflowPlanId &plan_id) const
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  std::lock_guard lock(mutex_);
  const auto it = plans_by_id_.find(plan_id.str());
  if (it == plans_by_id_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto WorkflowControlPlane::list_plans() const
    -> std::vector<std::shared_ptr<const ExecutionPlan>> {
  std::vector<std::shared_ptr<const ExecutionPlan>> plans;
  std::lock_guard lock(mutex_);
  plans.reserve(plans_by_id_.size());
  for (const auto &[_, plan] : plans_by_id_) {
    plans.push_back(plan);
  }
  std::ranges::sort(plans, {}, [](const auto &plan) {
    return plan->workflow_id.value();
  });
  return plans;
}

} // namespace dagforge::workflow
