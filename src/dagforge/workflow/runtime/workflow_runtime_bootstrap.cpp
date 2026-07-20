#include "dagforge/workflow/workflow_runtime.hpp"

#include "dagforge/core/scope_exit.hpp"
#include "dagforge/util/log.hpp"

#include "dagforge/workflow/plan_compiler.hpp"

#include "../detail/repair_planner.hpp"
#include "../storage/detail/storage_codec.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <memory>
#include <ranges>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow {

struct WorkflowRuntime::RunActivation {
  std::shared_ptr<const ExecutionPlan> plan;
  WorkflowCheckpoint checkpoint;
  WorkflowCallbacks callbacks;
  ActivationKind kind{ActivationKind::NewRun};
  std::vector<RepairNodeDecision> repair_decisions;
};

struct WorkflowRuntime::RunBootstrapRequest {
  std::shared_ptr<const ExecutionPlan> plan;
  TriggerEnvelope trigger;
  WorkflowCallbacks callbacks;
  ActivationKind kind{ActivationKind::NewRun};
  std::optional<WorkflowRunId> parent_run_id;
  std::optional<WorkflowPlanId> parent_plan_id;
  std::uint32_t repair_revision{0};
  std::string repair_reason;
  std::unordered_set<std::string> reused_nodes;
  std::vector<OutputValue> values;
  std::vector<RepairNodeDecision> repair_decisions;
};

namespace {

[[nodiscard]] auto make_initial_checkpoint(
    const ExecutionPlan &plan, TriggerEnvelope trigger, WorkflowRunId run_id,
    std::optional<WorkflowRunId> parent_run_id = std::nullopt,
    std::optional<WorkflowPlanId> parent_plan_id = std::nullopt,
    std::uint32_t repair_revision = 0, std::string repair_reason = {},
    const std::unordered_set<std::string> &reused_nodes = {},
    std::vector<OutputValue> values = {}) -> WorkflowCheckpoint {
  const auto now = std::chrono::system_clock::now();
  RunSnapshot snapshot;
  snapshot.run_id = run_id.clone();
  snapshot.workflow_id = plan.workflow_id.clone();
  snapshot.plan_id = plan.plan_id.clone();
  snapshot.state = RunState::Running;
  snapshot.parent_run_id = std::move(parent_run_id);
  snapshot.parent_plan_id = std::move(parent_plan_id);
  snapshot.repair_revision = repair_revision;
  snapshot.repair_reason = std::move(repair_reason);
  snapshot.created_at = now;
  snapshot.started_at = now;
  snapshot.tasks.reserve(plan.nodes.size());
  for (const auto &compiled : plan.nodes) {
    TaskSnapshot task;
    task.node_id = compiled.plan.node_id.clone();
    if (reused_nodes.contains(compiled.plan.node_id.str())) {
      task.state = TaskState::Succeeded;
      task.reused_from_run_id = snapshot.parent_run_id;
      task.started_at = now;
      task.finished_at = now;
    }
    snapshot.tasks.push_back(std::move(task));
  }
  return WorkflowCheckpoint{
      .plan = source_plan(plan),
      .trigger = std::move(trigger),
      .snapshot = std::move(snapshot),
      .values = std::move(values),
      .created_at = now,
  };
}

} // namespace

auto WorkflowRuntime::bootstrap_run(RunBootstrapRequest request)
    -> Result<RunBootstrapResult> {
  if (!request.plan) {
    return fail(Error::InvalidArgument);
  }

  const auto idempotency_key = request.trigger.idempotency_key;
  std::lock_guard lifecycle_lock(lifecycle_mutex_);
  if (!runtime_.is_running() || quiescing_.load(std::memory_order_acquire)) {
    return fail(Error::SystemNotRunning);
  }

  std::unique_lock idempotency_lock(idempotency_mutex_, std::defer_lock);
  if (!idempotency_key.empty()) {
    idempotency_lock.lock();
    if (const auto existing = idempotency_runs_.find(idempotency_key);
        existing != idempotency_runs_.end()) {
      const auto matches =
          existing->second.workflow_id == request.plan->workflow_id &&
          existing->second.plan_id == request.plan->plan_id &&
          existing->second.parent_run_id == request.parent_run_id;
      if (!matches) {
        return fail(Error::AlreadyExists);
      }
      return ok(RunBootstrapResult{
          .run_id = existing->second.run_id.clone(),
          .existing = true,
      });
    }
  }

  const auto run_id = generate_workflow_run_id(request.plan->workflow_id);
  auto checkpoint = make_initial_checkpoint(
      *request.plan, std::move(request.trigger), run_id.clone(),
      std::move(request.parent_run_id), std::move(request.parent_plan_id),
      request.repair_revision, std::move(request.repair_reason),
      request.reused_nodes, std::move(request.values));
  auto persisted = checkpoint_store_->save(checkpoint);
  if (!persisted) {
    return fail(persisted.error());
  }
  if (persisted->durability_deferred) {
    log::warn("Initial workflow checkpoint {} is visible but directory "
              "durability is deferred",
              run_id);
  }

  if (!idempotency_key.empty()) {
    const auto [_, inserted] = idempotency_runs_.emplace(
        idempotency_key, IdempotencyBinding{
                             .run_id = run_id.clone(),
                             .workflow_id = request.plan->workflow_id.clone(),
                             .plan_id = request.plan->plan_id.clone(),
                             .parent_run_id = checkpoint.snapshot.parent_run_id,
                         });
    assert(inserted);
  }

  schedule_activation(RunActivation{
      .plan = std::move(request.plan),
      .checkpoint = std::move(checkpoint),
      .callbacks = std::move(request.callbacks),
      .kind = request.kind,
      .repair_decisions = std::move(request.repair_decisions),
  });
  return ok(RunBootstrapResult{.run_id = run_id.clone()});
}

auto WorkflowRuntime::schedule_activation(RunActivation activation) -> void {
  assert(activation.plan);
  assert(!activation.checkpoint.snapshot.run_id.empty());
  const auto owner = owner_shard(activation.checkpoint.snapshot.run_id);
  const auto tracker = initialization_tracker_;
  tracker->pending.fetch_add(1, std::memory_order_release);
  runtime_.post_to(owner, [this,
                           weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                           tracker,
                           activation = std::move(activation)]() mutable {
    auto lifetime = weak_lifetime.lock();
    const bool runtime_alive = static_cast<bool>(lifetime);
    const auto initialization_finished =
        dagforge::scope_exit([this, tracker, lifetime = std::move(lifetime)] {
          tracker->pending.fetch_sub(1, std::memory_order_acq_rel);
          tracker->changed.notify_all();
          if (lifetime) {
            notify_lifecycle_changed();
          }
        });
    if (!runtime_alive) {
      return;
    }
    initialize_checkpoint_run(std::move(activation.plan),
                              std::move(activation.checkpoint),
                              std::move(activation.callbacks), activation.kind,
                              std::move(activation.repair_decisions));
  });
}

auto WorkflowRuntime::start(std::shared_ptr<const ExecutionPlan> plan,
                            TriggerEnvelope trigger,
                            WorkflowCallbacks callbacks)
    -> Result<WorkflowRunId> {
  if (!plan || plan->workflow_id.empty() || trigger.workflow_id.empty() ||
      plan->workflow_id != trigger.workflow_id) {
    return fail(Error::InvalidArgument);
  }
  if (trigger.trigger_id.empty()) {
    trigger.trigger_id = generate_workflow_trigger_id();
  }

  auto bootstrapped = bootstrap_run(RunBootstrapRequest{
      .plan = std::move(plan),
      .trigger = std::move(trigger),
      .callbacks = std::move(callbacks),
  });
  if (!bootstrapped) {
    return fail(bootstrapped.error());
  }
  return ok(std::move(bootstrapped->run_id));
}

auto WorkflowRuntime::restore(std::shared_ptr<const ExecutionPlan> plan,
                              WorkflowCheckpoint checkpoint) -> Result<void> {
  if (runtime_.is_running() || !plan || checkpoint.snapshot.run_id.empty() ||
      checkpoint.snapshot.plan_id != plan->plan_id ||
      checkpoint.snapshot.workflow_id != plan->workflow_id) {
    return fail(Error::InvalidState);
  }
  auto validated = storage_detail::validate_checkpoint(checkpoint);
  if (!validated) {
    return fail(validated.error());
  }
  auto checkpoint_digest = PlanCompiler::digest(checkpoint.plan);
  if (!checkpoint_digest) {
    return fail(checkpoint_digest.error());
  }
  auto submitted_plan = source_plan(*plan);
  auto submitted_digest = PlanCompiler::digest(submitted_plan);
  if (!submitted_digest) {
    return fail(submitted_digest.error());
  }
  if (*checkpoint_digest != *submitted_digest) {
    return fail(Error::InvalidState);
  }
  for (const auto &entry : checkpoint.values) {
    const auto *reference = std::get_if<ArtifactRef>(&entry.value);
    if (reference == nullptr) {
      continue;
    }
    auto artifact = artifact_store_->get(reference->artifact_id);
    if (!artifact) {
      return fail(artifact.error());
    }
    if (artifact->ref.media_type != reference->media_type ||
        artifact->ref.size_bytes != reference->size_bytes ||
        artifact->ref.digest != reference->digest) {
      return fail(Error::ParseError);
    }
  }

  auto snapshot = checkpoint.snapshot;
  const auto owner = owner_shard(snapshot.run_id);
  if (shard_states_[owner].completed_runs.contains(snapshot.run_id.str()) ||
      std::ranges::any_of(restored_runs_, [&](const RestoredRun &restored) {
        return restored.checkpoint.snapshot.run_id == snapshot.run_id;
      })) {
    return fail(Error::AlreadyExists);
  }
  if (!checkpoint.trigger.idempotency_key.empty()) {
    std::lock_guard lock(idempotency_mutex_);
    auto [binding, inserted] = idempotency_runs_.emplace(
        checkpoint.trigger.idempotency_key,
        IdempotencyBinding{
            .run_id = snapshot.run_id.clone(),
            .workflow_id = snapshot.workflow_id.clone(),
            .plan_id = snapshot.plan_id.clone(),
            .parent_run_id = snapshot.parent_run_id,
        });
    if (!inserted &&
        (binding->second.run_id != snapshot.run_id ||
         binding->second.workflow_id != snapshot.workflow_id ||
         binding->second.plan_id != snapshot.plan_id ||
         binding->second.parent_run_id != snapshot.parent_run_id)) {
      return fail(Error::AlreadyExists);
    }
  }
  if (!is_terminal(snapshot.state)) {
    restored_runs_.push_back(RestoredRun{.plan = std::move(plan),
                                         .checkpoint = std::move(checkpoint)});
    return ok();
  }

  auto stored = std::make_shared<const RunSnapshot>(snapshot);
  auto &state = shard_states_[owner];
  state.completed_runs[snapshot.run_id.str()] = stored;
  state.completed_values[snapshot.run_id.str()] = std::move(checkpoint.values);
  state.completed_order.push_back(snapshot.run_id.str());
  enforce_completed_retention(state);
  return ok();
}

auto WorkflowRuntime::activate_restored() -> Result<void> {
  if (!runtime_.is_running() || runtime_.is_current_shard() ||
      quiescing_.load(std::memory_order_acquire)) {
    return fail(Error::InvalidState);
  }

  auto restored = std::move(restored_runs_);
  restored_runs_.clear();
  for (auto &run : restored) {
    schedule_activation(RunActivation{
        .plan = std::move(run.plan),
        .checkpoint = std::move(run.checkpoint),
        .kind = ActivationKind::RestartRecovery,
    });
  }
  if (!restored.empty()) {
    std::unique_lock lock(lifecycle_mutex_);
    lifecycle_changed_.wait(lock, [this] {
      return initialization_tracker_->pending.load(std::memory_order_acquire) ==
             0;
    });
  }
  return ok();
}

auto WorkflowRuntime::repair(std::shared_ptr<const ExecutionPlan> plan,
                             const WorkflowRunId &parent_run_id,
                             RepairRequest request, WorkflowCallbacks callbacks)
    -> Result<RepairStartResult> {
  if (!plan || parent_run_id.empty() || request.reason.empty() ||
      !runtime_.is_running() || quiescing_.load(std::memory_order_acquire)) {
    return fail(Error::InvalidState);
  }
  auto parent = checkpoint_store_->load(parent_run_id);
  if (!parent) {
    return fail(parent.error());
  }
  if (!is_terminal(parent->snapshot.state) ||
      parent->snapshot.workflow_id != plan->workflow_id) {
    return fail(Error::InvalidState);
  }
  auto planned = detail::plan_repair(*plan, *parent, *artifact_store_);
  if (!planned) {
    return fail(planned.error());
  }

  auto trigger = parent->trigger;
  trigger.trigger_id = generate_workflow_trigger_id();
  trigger.source = "repair";
  trigger.event_type = "workflow_repair";
  trigger.idempotency_key = request.idempotency_key;
  trigger.occurred_at = std::chrono::system_clock::now();
  const auto revised_plan_id = plan->plan_id.clone();
  auto bootstrapped = bootstrap_run(RunBootstrapRequest{
      .plan = std::move(plan),
      .trigger = std::move(trigger),
      .callbacks = std::move(callbacks),
      .kind = ActivationKind::RepairRun,
      .parent_run_id = parent_run_id.clone(),
      .parent_plan_id = parent->snapshot.plan_id.clone(),
      .repair_revision = parent->snapshot.repair_revision + 1,
      .repair_reason = std::move(request.reason),
      .reused_nodes = std::move(planned->reused_nodes),
      .values = std::move(planned->values),
      .repair_decisions = planned->decisions,
  });
  if (!bootstrapped) {
    return fail(bootstrapped.error());
  }

  if (bootstrapped->existing) {
    auto existing_checkpoint = checkpoint_store_->load(bootstrapped->run_id);
    if (!existing_checkpoint ||
        existing_checkpoint->snapshot.parent_run_id != parent_run_id) {
      return fail(Error::AlreadyExists);
    }
    std::vector<RepairNodeDecision> existing_decisions;
    existing_decisions.reserve(existing_checkpoint->snapshot.tasks.size());
    for (const auto &task : existing_checkpoint->snapshot.tasks) {
      const bool reused = task.reused_from_run_id == parent_run_id;
      existing_decisions.push_back(RepairNodeDecision{
          .node_id = task.node_id.clone(),
          .reused = reused,
          .reason = reused ? "reused" : "existing_repair",
      });
    }
    return ok(RepairStartResult{
        .run_id = bootstrapped->run_id.clone(),
        .parent_run_id = parent_run_id.clone(),
        .plan_id = existing_checkpoint->snapshot.plan_id.clone(),
        .nodes = std::move(existing_decisions),
    });
  }

  return ok(RepairStartResult{
      .run_id = std::move(bootstrapped->run_id),
      .parent_run_id = parent_run_id.clone(),
      .plan_id = revised_plan_id,
      .nodes = std::move(planned->decisions),
  });
}

} // namespace dagforge::workflow
