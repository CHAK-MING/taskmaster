#include "dagforge/workflow/workflow_control_plane.hpp"

#include <algorithm>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>

namespace dagforge::workflow {

WorkflowControlPlane::WorkflowControlPlane(const ExecutorRegistry &executors,
                                           PlanValidator validator,
                                           std::shared_ptr<PlanStore> plan_store)
    : compiler_(executors, std::move(validator)),
      plan_store_(std::move(plan_store)) {
  if (!plan_store_) {
    plan_store_ = std::make_shared<PlanStore>();
  }
}

auto WorkflowControlPlane::register_plan(WorkflowPlan plan)
    -> Result<PlanRegistration> {
  auto compiled = compiler_.compile(std::move(plan));
  if (!compiled) {
    return fail(compiled.error());
  }

  std::lock_guard lock(mutex_);
  if (const auto existing = plans_by_digest_.find((*compiled)->digest);
      existing != plans_by_digest_.end()) {
    latest_by_workflow_[existing->second->workflow_id.str()] = existing->second;
    return ok(PlanRegistration{
        .plan = existing->second,
        .durability_deferred =
            durability_deferred_by_plan_id_[existing->second->plan_id.str()],
    });
  }
  auto persisted = plan_store_->save(**compiled);
  if (!persisted) {
    return fail(persisted.error());
  }
  plans_by_id_[(*compiled)->plan_id.str()] = *compiled;
  plans_by_digest_[(*compiled)->digest] = *compiled;
  if (!persisted->durability_deferred) {
    for (auto &[_, deferred] : durability_deferred_by_plan_id_) {
      deferred = false;
    }
  }
  durability_deferred_by_plan_id_[(*compiled)->plan_id.str()] =
      persisted->durability_deferred;
  latest_by_workflow_[(*compiled)->workflow_id.str()] = *compiled;
  return ok(PlanRegistration{
      .plan = std::move(*compiled),
      .durability_deferred = persisted->durability_deferred,
  });
}

auto WorkflowControlPlane::restore_plan(WorkflowPlan plan,
                                        const WorkflowPlanId &plan_id,
                                        std::string_view expected_digest)
    -> Result<std::shared_ptr<const ExecutionPlan>> {
  auto compiled = compiler_.compile(std::move(plan), plan_id);
  if (!compiled) {
    return fail(compiled.error());
  }
  if (!expected_digest.empty() && (*compiled)->digest != expected_digest) {
    return fail(Error::ParseError);
  }
  std::lock_guard lock(mutex_);
  plans_by_id_[(*compiled)->plan_id.str()] = *compiled;
  plans_by_digest_[(*compiled)->digest] = *compiled;
  durability_deferred_by_plan_id_[(*compiled)->plan_id.str()] = false;
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
