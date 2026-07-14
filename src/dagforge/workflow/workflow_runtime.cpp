#include "dagforge/workflow/workflow_runtime.hpp"

#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <boost/asio/async_result.hpp>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <experimental/scope>
#include <format>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto is_success(TaskState state) noexcept -> bool {
  return state == TaskState::Succeeded;
}

[[nodiscard]] auto value_to_string(const WorkflowValue &value) -> std::string {
  if (std::holds_alternative<std::monostate>(value)) {
    return {};
  }
  if (const auto *boolean = std::get_if<bool>(&value)) {
    return *boolean ? "true" : "false";
  }
  if (const auto *integer = std::get_if<std::int64_t>(&value)) {
    return std::format("{}", *integer);
  }
  if (const auto *real = std::get_if<double>(&value)) {
    return std::format("{}", *real);
  }
  if (const auto *text = std::get_if<std::string>(&value)) {
    return *text;
  }
  if (const auto *json = std::get_if<JsonValue>(&value)) {
    return dump_json(*json);
  }
  return std::get<ArtifactRef>(value).artifact_id.str();
}

[[nodiscard]] auto value_truthy(const WorkflowValue &value) -> bool {
  if (std::holds_alternative<std::monostate>(value)) {
    return false;
  }
  if (const auto *boolean = std::get_if<bool>(&value)) {
    return *boolean;
  }
  if (const auto *integer = std::get_if<std::int64_t>(&value)) {
    return *integer != 0;
  }
  if (const auto *real = std::get_if<double>(&value)) {
    return *real != 0;
  }
  if (const auto *text = std::get_if<std::string>(&value)) {
    return !text->empty();
  }
  if (const auto *json = std::get_if<JsonValue>(&value)) {
    return dump_json(*json) != "null";
  }
  return !std::get<ArtifactRef>(value).artifact_id.empty();
}

[[nodiscard]] auto outputs_match_contract(
    const NodePlan &node, const ExecutorOutputs &outputs) -> bool {
  std::unordered_set<std::string> seen;
  seen.reserve(outputs.size());
  for (const auto &[port, _] : outputs) {
    const auto declared = std::ranges::find(node.outputs, port);
    if (port.empty() || declared == node.outputs.end() ||
        !seen.emplace(port.str()).second) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] auto make_metadata(
    std::initializer_list<std::pair<std::string, JsonValue>> fields)
    -> JsonValue {
  JsonValue value = JsonValue::object_t{};
  for (auto &[key, field] : fields) {
    value[key] = std::move(field);
  }
  return value;
}

[[nodiscard]] auto instance_id_for(const WorkflowRunId &run_id,
                                   const WorkflowNodeId &node_id,
                                   const AttemptId &attempt_id)
    -> InstanceId {
  return InstanceId{std::format("{}_{}_{}", run_id, node_id, attempt_id)};
}

[[nodiscard]] auto classify_failure(std::error_code error) noexcept
    -> FailureClass {
  if (error == make_error_code(Error::Cancelled) ||
      error == std::errc::operation_canceled) {
    return FailureClass::Cancelled;
  }
  if (error == make_error_code(Error::Timeout) ||
      error == std::errc::timed_out) {
    return FailureClass::Timeout;
  }
  if (error == make_error_code(Error::InvalidArgument) ||
      error == make_error_code(Error::ParseError) ||
      error == make_error_code(Error::FileNotFound) ||
      error == make_error_code(Error::NotFound) ||
      error == make_error_code(Error::AlreadyExists) ||
      error == make_error_code(Error::InvalidUrl) ||
      error == make_error_code(Error::ProtocolError) ||
      error == make_error_code(Error::Unauthorized) ||
      error == make_error_code(Error::Unsupported) ||
      error == make_error_code(Error::InvalidState) ||
      error == make_error_code(Error::ResourceExhausted)) {
    return FailureClass::Permanent;
  }
  if (error == make_error_code(Error::SystemNotRunning) ||
      error == make_error_code(Error::QueueFull) ||
      error == make_error_code(Error::ProcessForkFailed)) {
    return FailureClass::Infrastructure;
  }
  return FailureClass::Retryable;
}

[[nodiscard]] auto retryable(FailureClass failure_class) noexcept -> bool {
  return failure_class == FailureClass::Retryable ||
         failure_class == FailureClass::Timeout ||
         failure_class == FailureClass::Infrastructure;
}

[[nodiscard]] auto retry_delay(const NodePlan &node,
                               std::uint32_t attempt_number)
    -> std::chrono::milliseconds {
  auto delay = node.retry_initial_delay;
  for (std::uint32_t current = 1;
       current < attempt_number && delay < node.retry_max_delay; ++current) {
    delay = std::min(node.retry_max_delay, delay * 2);
  }
  return delay;
}

[[nodiscard]] auto source_plan(const ExecutionPlan &execution)
    -> WorkflowPlan {
  WorkflowPlan plan;
  plan.workflow_id = execution.workflow_id.clone();
  plan.nodes.reserve(execution.nodes.size());
  for (const auto &compiled : execution.nodes) {
    plan.nodes.push_back(compiled.plan);
  }
  plan.edges = execution.edges;
  plan.outputs = execution.outputs;
  plan.policy = execution.policy;
  return plan;
}

[[nodiscard]] auto to_boost_error(std::error_code error)
    -> boost::system::error_code {
  return boost::system::error_code{error};
}

} // namespace

WorkflowRuntime::WorkflowRuntime(
    Runtime &runtime, ExecutorRegistry &executors,
    std::shared_ptr<IArtifactStore> artifact_store,
    std::shared_ptr<EvidenceLedger> evidence_ledger,
    std::shared_ptr<CheckpointStore> checkpoint_store,
    std::size_t max_completed_runs)
    : runtime_(runtime), executors_(executors),
      artifact_store_(std::move(artifact_store)),
      evidence_ledger_(std::move(evidence_ledger)),
      checkpoint_store_(std::move(checkpoint_store)),
      max_completed_runs_(std::max<std::size_t>(1, max_completed_runs)),
      shard_states_(runtime.shard_count()) {
  if (!artifact_store_) {
    artifact_store_ = std::make_shared<InMemoryArtifactStore>();
  }
  if (!evidence_ledger_) {
    evidence_ledger_ = std::make_shared<EvidenceLedger>();
  }
  if (!checkpoint_store_) {
    checkpoint_store_ = std::make_shared<CheckpointStore>();
  }
}

WorkflowRuntime::~WorkflowRuntime() { lifetime_token_.reset(); }

auto WorkflowRuntime::notify_lifecycle_changed() noexcept -> void {
  {
    std::lock_guard lock(lifecycle_mutex_);
  }
  lifecycle_changed_.notify_all();
}

auto WorkflowRuntime::lifecycle_quiesced() const noexcept -> bool {
  return pending_initializations_.load(std::memory_order_acquire) == 0 &&
         active_run_count_.load(std::memory_order_acquire) == 0 &&
         active_task_coroutines_.load(std::memory_order_acquire) == 0;
}

auto WorkflowRuntime::quiesce(std::chrono::milliseconds timeout)
    -> Result<void> {
  if (timeout <= std::chrono::milliseconds::zero()) {
    return fail(Error::InvalidArgument);
  }
  if (runtime_.is_current_shard()) {
    return fail(Error::InvalidState);
  }

  {
    std::lock_guard lock(lifecycle_mutex_);
    quiescing_.store(true, std::memory_order_release);
  }
  if (!runtime_.is_running()) {
    return lifecycle_quiesced() ? ok() : fail(Error::SystemNotRunning);
  }

  constexpr std::string_view kShutdownReason =
      "workflow runtime is shutting down";
  for (shard_id shard = 0; shard < shard_states_.size(); ++shard) {
    runtime_.post_to(
        shard, [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                shard, shutdown_reason = std::string{kShutdownReason}] {
          if (weak_lifetime.expired()) {
            return;
          }
          std::vector<WorkflowRunId> run_ids;
          run_ids.reserve(shard_states_[shard].active_runs.size());
          for (const auto &[_, run] : shard_states_[shard].active_runs) {
            run_ids.push_back(run.snapshot.run_id.clone());
          }
          for (const auto &run_id : run_ids) {
            const auto stopped = request_stop(
                run_id, StopIntent::Cancel, shutdown_reason);
            if (!stopped &&
                stopped.error() != make_error_code(Error::NotFound) &&
                stopped.error() != make_error_code(Error::InvalidState)) {
              log::error("Failed to stop workflow {} during shutdown: {}",
                         run_id, stopped.error().message());
            }
          }
        });
  }

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  {
    std::unique_lock lock(lifecycle_mutex_);
    if (!lifecycle_changed_.wait_until(
            lock, deadline, [this] { return lifecycle_quiesced(); })) {
      return fail(Error::Timeout);
    }
  }

  struct BarrierState {
    explicit BarrierState(std::uint32_t count) : remaining(count) {}
    std::atomic<std::uint32_t> remaining;
    std::mutex mutex;
    std::condition_variable changed;
  };
  auto barrier = std::make_shared<BarrierState>(runtime_.shard_count());
  for (shard_id shard = 0; shard < runtime_.shard_count(); ++shard) {
    runtime_.post_to(shard, [barrier] {
      if (barrier->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        barrier->changed.notify_all();
      }
    });
  }
  std::unique_lock barrier_lock(barrier->mutex);
  if (!barrier->changed.wait_until(barrier_lock, deadline, [barrier] {
        return barrier->remaining.load(std::memory_order_acquire) == 0;
      })) {
    return fail(Error::Timeout);
  }
  return ok();
}

auto WorkflowRuntime::owner_shard(const WorkflowRunId &run_id) const noexcept
    -> shard_id {
  return static_cast<shard_id>(
      std::hash<WorkflowRunId>{}(run_id) % std::max(1U, runtime_.shard_count()));
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

  WorkflowRunId run_id;
  {
    std::lock_guard lifecycle_lock(lifecycle_mutex_);
    if (!runtime_.is_running() ||
        quiescing_.load(std::memory_order_acquire)) {
      return fail(Error::SystemNotRunning);
    }
    if (!trigger.idempotency_key.empty()) {
      std::lock_guard idempotency_lock(idempotency_mutex_);
      if (const auto it = idempotency_runs_.find(trigger.idempotency_key);
          it != idempotency_runs_.end()) {
        return ok(it->second.clone());
      }
    }
    run_id = generate_workflow_run_id(plan->workflow_id);
    if (!trigger.idempotency_key.empty()) {
      std::lock_guard idempotency_lock(idempotency_mutex_);
      idempotency_runs_.emplace(trigger.idempotency_key, run_id.clone());
    }
    pending_initializations_.fetch_add(1, std::memory_order_release);
  }

  const auto target = owner_shard(run_id);
  runtime_.post_to(
      target,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), plan = std::move(plan),
       trigger = std::move(trigger), callbacks = std::move(callbacks)]() mutable {
        if (weak_lifetime.expired()) {
          return;
        }
        const auto initialization_finished = std::experimental::scope_exit(
            [this] {
              pending_initializations_.fetch_sub(1,
                                                 std::memory_order_acq_rel);
              notify_lifecycle_changed();
            });
        initialize_run(std::move(run_id), std::move(plan), std::move(trigger),
                       std::move(callbacks));
      });
  return ok(std::move(run_id));
}

auto WorkflowRuntime::restore(std::shared_ptr<const ExecutionPlan> plan,
                              WorkflowCheckpoint checkpoint) -> Result<void> {
  if (runtime_.is_running() || !plan || checkpoint.snapshot.run_id.empty() ||
      checkpoint.snapshot.plan_id != plan->plan_id ||
      checkpoint.snapshot.workflow_id != plan->workflow_id) {
    return fail(Error::InvalidState);
  }

  auto snapshot = std::move(checkpoint.snapshot);
  if (!is_terminal(snapshot.state)) {
    const auto now = std::chrono::system_clock::now();
    constexpr std::string_view kRestartError =
        "runtime restarted before workflow completion";
    for (auto &task : snapshot.tasks) {
      if (is_terminal(task.state)) {
        continue;
      }
      if (task.active_attempt_id && !task.attempts.empty()) {
        auto &attempt = task.attempts.back();
        if (attempt.attempt_id == *task.active_attempt_id &&
            !is_terminal(attempt.state)) {
          attempt.state = AttemptState::Failed;
          attempt.failure_class = FailureClass::Infrastructure;
          attempt.termination_reason = TerminationReason::RunFailed;
          attempt.error = kRestartError;
          attempt.finished_at = now;
        }
      }
      task.active_attempt_id.reset();
      task.next_attempt_at.reset();
      task.last_error = kRestartError;
      task.state = TaskState::Failed;
      task.finished_at = now;
    }
    snapshot.state = RunState::Failed;
    snapshot.stop_intent = StopIntent::Fail;
    snapshot.stop_reason = kRestartError;
    snapshot.error = kRestartError;
    snapshot.finished_at = now;
    checkpoint.snapshot = snapshot;
    (void)checkpoint_store_->save(checkpoint);
  }

  const auto owner = owner_shard(snapshot.run_id);
  auto stored = std::make_shared<const RunSnapshot>(snapshot);
  auto &state = shard_states_[owner];
  state.completed_runs[snapshot.run_id.str()] = stored;
  state.completed_values[snapshot.run_id.str()] =
      std::move(checkpoint.values);
  state.completed_order.push_back(snapshot.run_id.str());
  while (state.completed_order.size() > max_completed_runs_) {
    auto expired = std::move(state.completed_order.front());
    state.completed_order.pop_front();
    state.completed_runs.erase(expired);
    state.completed_values.erase(expired);
    (void)checkpoint_store_->erase(WorkflowRunId{expired});
    std::lock_guard lock(idempotency_mutex_);
    std::erase_if(idempotency_runs_, [&](const auto &entry) {
      return entry.second.str() == expired;
    });
  }
  if (!checkpoint.trigger.idempotency_key.empty()) {
    std::lock_guard lock(idempotency_mutex_);
    idempotency_runs_[checkpoint.trigger.idempotency_key] =
        snapshot.run_id.clone();
  }
  return ok();
}

auto WorkflowRuntime::initialize_run(
    WorkflowRunId run_id, std::shared_ptr<const ExecutionPlan> plan,
    TriggerEnvelope trigger, WorkflowCallbacks callbacks) -> void {
  const auto owner = runtime_.current_shard();
  auto &state = shard_states_[owner];

  ActiveRun run;
  run.plan = std::move(plan);
  run.trigger = std::move(trigger);
  run.snapshot.run_id = run_id.clone();
  run.snapshot.workflow_id = run.plan->workflow_id.clone();
  run.snapshot.plan_id = run.plan->plan_id.clone();
  run.snapshot.state = RunState::Running;
  run.snapshot.created_at = std::chrono::system_clock::now();
  run.snapshot.started_at = run.snapshot.created_at;
  run.callbacks = std::move(callbacks);
  run.values = std::make_unique<RunValueStore>(
      runtime_, owner, *artifact_store_,
      run.plan->policy.budget.max_total_output_bytes);

  run.tasks.reserve(run.plan->nodes.size());
  run.snapshot.tasks.reserve(run.plan->nodes.size());
  for (const auto &compiled : run.plan->nodes) {
    TaskRuntimeState task;
    task.snapshot.node_id = compiled.plan.node_id.clone();
    if (compiled.dependencies.empty()) {
      task.snapshot.state = TaskState::Ready;
      run.ready.push_back(compiled.index);
    }
    run.snapshot.tasks.push_back(task.snapshot);
    run.tasks.push_back(std::move(task));
  }
  assert(invariants_hold(run));

  auto [it, inserted] = state.active_runs.emplace(run_id.str(), std::move(run));
  if (!inserted) {
    return;
  }
  it->second.deadline_handle = runtime_.schedule_after_on(
      owner, it->second.plan->policy.budget.max_run_duration,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone()] {
        if (weak_lifetime.expired()) {
          return;
        }
        auto &owner_state = shard_states_[owner_shard(run_id)];
        const auto active = owner_state.active_runs.find(run_id.str());
        if (active == owner_state.active_runs.end()) {
          return;
        }
        (void)request_stop(run_id, StopIntent::Fail,
                           "workflow run deadline exceeded");
      });
  active_run_count_.fetch_add(1, std::memory_order_release);
  append_evidence(it->second, it->second.tasks.size(),
                  EvidenceType::TriggerReceived,
                  make_metadata({{"source", it->second.trigger.source},
                                 {"event_type", it->second.trigger.event_type},
                                 {"plan_digest", it->second.plan->digest}}));
  append_evidence(it->second, it->second.tasks.size(),
                  EvidenceType::PlanCompiled,
                  make_metadata({{"plan_id", it->second.plan->plan_id.str()},
                                 {"digest", it->second.plan->digest}}));
  emit_run_state(it->second);
  if (quiescing_.load(std::memory_order_acquire)) {
    (void)request_stop(run_id, StopIntent::Cancel,
                       "workflow runtime is shutting down");
  } else {
    dispatch(run_id);
  }
}

auto WorkflowRuntime::transition_run(ActiveRun &run, RunState state)
    -> Result<void> {
  if (!can_transition(run.snapshot.state, state)) {
    return fail(Error::InvalidState);
  }
  run.snapshot.state = state;
  if (is_terminal(state)) {
    run.snapshot.finished_at = std::chrono::system_clock::now();
  }
  emit_run_state(run);
  return ok();
}

auto WorkflowRuntime::transition_task(ActiveRun &run, std::size_t task_index,
                                      TaskState state) -> Result<void> {
  if (task_index >= run.tasks.size()) {
    return fail(Error::InvalidArgument);
  }
  auto &task = run.tasks[task_index].snapshot;
  if (!can_transition(task.state, state)) {
    return fail(Error::InvalidState);
  }
  task.state = state;
  if (state == TaskState::Running &&
      task.started_at == std::chrono::system_clock::time_point{}) {
    task.started_at = std::chrono::system_clock::now();
  }
  if (state == TaskState::Ready) {
    task.next_attempt_at.reset();
  }
  if (is_terminal(state)) {
    task.active_attempt_id.reset();
    task.next_attempt_at.reset();
    task.finished_at = std::chrono::system_clock::now();
  }
  emit_task_state(run, task_index);
  return ok();
}

auto WorkflowRuntime::transition_attempt(AttemptSnapshot &attempt,
                                         AttemptState state) -> Result<void> {
  if (!can_transition(attempt.state, state)) {
    return fail(Error::InvalidState);
  }
  attempt.state = state;
  if (state == AttemptState::Running &&
      attempt.started_at == std::chrono::system_clock::time_point{}) {
    attempt.started_at = std::chrono::system_clock::now();
  }
  if (is_terminal(state)) {
    attempt.finished_at = std::chrono::system_clock::now();
  }
  return ok();
}

auto WorkflowRuntime::active_attempt(TaskRuntimeState &task,
                                     const AttemptId &attempt_id)
    -> AttemptSnapshot * {
  if (!task.snapshot.active_attempt_id ||
      *task.snapshot.active_attempt_id != attempt_id ||
      task.snapshot.attempts.empty()) {
    return nullptr;
  }
  auto &attempt = task.snapshot.attempts.back();
  return attempt.attempt_id == attempt_id ? &attempt : nullptr;
}

auto WorkflowRuntime::invariants_hold(const ActiveRun &run) const noexcept
    -> bool {
  if (run.snapshot.tasks.size() != run.tasks.size()) {
    return false;
  }
  std::size_t active_attempts = 0;
  for (std::size_t index = 0; index < run.tasks.size(); ++index) {
    const auto &task = run.tasks[index];
    const auto &published = run.snapshot.tasks[index];
    if (published.state != task.snapshot.state ||
        published.attempt_count != task.snapshot.attempt_count ||
        published.active_attempt_id != task.snapshot.active_attempt_id) {
      return false;
    }
    if (task.snapshot.attempt_count != task.snapshot.attempts.size()) {
      return false;
    }

    const AttemptSnapshot *active = nullptr;
    if (task.snapshot.active_attempt_id) {
      if (task.snapshot.attempts.empty()) {
        return false;
      }
      const auto &candidate = task.snapshot.attempts.back();
      if (candidate.attempt_id != *task.snapshot.active_attempt_id ||
          is_terminal(candidate.state)) {
        return false;
      }
      active = std::addressof(candidate);
      ++active_attempts;
    }

    if ((task.snapshot.state == TaskState::Running) != (active != nullptr)) {
      return false;
    }
    if (task.snapshot.state == TaskState::RetryWaiting) {
      if (task.snapshot.attempts.empty() ||
          !is_terminal(task.snapshot.attempts.back().state) ||
          !task.snapshot.next_attempt_at) {
        return false;
      }
    }
    if (is_terminal(task.snapshot.state) &&
        task.snapshot.active_attempt_id) {
      return false;
    }
  }

  if (active_attempts != run.active_attempts) {
    return false;
  }
  if (is_terminal(run.snapshot.state)) {
    if (active_attempts != 0 ||
        !std::ranges::all_of(run.tasks, [](const auto &task) {
          return is_terminal(task.snapshot.state);
        })) {
      return false;
    }
  }
  return true;
}

auto WorkflowRuntime::begin_attempt(ActiveRun &run, std::size_t task_index)
    -> AttemptId {
  auto &task = run.tasks[task_index];
  auto attempt_id = generate_attempt_id();
  task.snapshot.attempt_count += 1;
  task.snapshot.active_attempt_id = attempt_id.clone();
  task.snapshot.next_attempt_at.reset();
  task.snapshot.skip_reason.reset();
  task.snapshot.attempts.push_back(AttemptSnapshot{
      .attempt_id = attempt_id.clone(),
      .number = task.snapshot.attempt_count,
      .state = AttemptState::Starting,
      .created_at = std::chrono::system_clock::now(),
  });
  run.active_attempts += 1;
  (void)transition_task(run, task_index, TaskState::Running);
  append_evidence(run, task_index, EvidenceType::AttemptStarted,
                  make_metadata({{"attempt_id", attempt_id.str()},
                                 {"number", task.snapshot.attempt_count}}));
  append_evidence(run, task_index, EvidenceType::TaskStarted,
                  make_metadata({{"attempt", task.snapshot.attempt_count},
                                 {"executor", run.plan->nodes[task_index]
                                                   .plan.executor}}));
  assert(invariants_hold(run));
  return attempt_id;
}

auto WorkflowRuntime::mark_attempt_running(ActiveRun &run,
                                           std::size_t task_index,
                                           const AttemptId &attempt_id)
    -> void {
  if (task_index >= run.tasks.size()) {
    return;
  }
  auto *attempt = active_attempt(run.tasks[task_index], attempt_id);
  if (attempt == nullptr || is_terminal(attempt->state)) {
    return;
  }
  if (transition_attempt(*attempt, AttemptState::Running)) {
    emit_task_state(run, task_index);
    assert(invariants_hold(run));
  }
}

auto WorkflowRuntime::dispatch(const WorkflowRunId &run_id) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner, [this, run_id = run_id.clone()] {
      dispatch(run_id);
    });
    return;
  }

  auto &state = shard_states_[owner];
  auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end() || it->second.dispatching) {
    return;
  }
  if (it->second.snapshot.state != RunState::Running) {
    settle_control_state(run_id);
    return;
  }
  it->second.dispatching = true;

  while (true) {
    it = state.active_runs.find(run_id.str());
    if (it == state.active_runs.end()) {
      return;
    }
    auto &run = it->second;
    if (run.snapshot.state != RunState::Running) {
      break;
    }

    const auto limit = std::max<std::size_t>(
        1, run.plan->policy.budget.max_parallel_nodes);
    if (run.ready.empty() || run.active_attempts >= limit) {
      break;
    }

    const auto node_index = run.ready.front();
    run.ready.pop_front();
    if (node_index >= run.tasks.size() ||
        run.tasks[node_index].snapshot.state != TaskState::Ready) {
      continue;
    }

    auto passes = conditions_pass(run, node_index);
    if (!passes) {
      (void)request_stop(run_id, StopIntent::Fail, passes.error().message());
      break;
    }
    if (!*passes) {
      run.tasks[node_index].snapshot.skip_reason = SkipReason::ConditionFalse;
      (void)transition_task(run, node_index, TaskState::Skipped);
      update_dependents(run_id, node_index);
      continue;
    }

    start_task(run_id, node_index);
  }

  it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return;
  }
  it->second.dispatching = false;
  settle_control_state(run_id);
}

auto WorkflowRuntime::start_task(const WorkflowRunId &run_id,
                                 std::size_t task_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  const auto attempt_id = begin_attempt(run, task_index);
  active_task_coroutines_.fetch_add(1, std::memory_order_release);
  runtime_.spawn_on(owner_shard(run_id),
                    start_async_task(run_id.clone(), task_index,
                                     attempt_id.clone()));
}

auto WorkflowRuntime::start_async_task(WorkflowRunId run_id,
                                       std::size_t task_index,
                                       AttemptId attempt_id) -> spawn_task {
  const auto task_finished = std::experimental::scope_exit([this] {
    active_task_coroutines_.fetch_sub(1, std::memory_order_acq_rel);
    notify_lifecycle_changed();
  });
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end()) {
    co_return;
  }

  auto node = run_it->second.plan->nodes[task_index].plan;
  run_it->second.tasks[task_index].instance_id =
      instance_id_for(run_id, node.node_id, attempt_id);
  auto inputs = input_values(run_it->second, task_index);
  if (!inputs) {
    complete_task(run_id, task_index, attempt_id, fail(inputs.error()));
    co_return;
  }
  auto result = co_await execute_task(
      run_id.clone(), task_index, attempt_id.clone(), std::move(node),
      std::move(*inputs));

  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), task_index,
                      attempt_id = attempt_id.clone(),
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_task(run_id, task_index, attempt_id,
                                       std::move(result));
                       }
                     });
    co_return;
  }
  complete_task(run_id, task_index, attempt_id, std::move(result));
}

auto WorkflowRuntime::complete_task(const WorkflowRunId &run_id,
                                    std::size_t task_index,
                                    const AttemptId &attempt_id,
                                    Result<ExecutorOutputs> result) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner,
                     [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
                      run_id = run_id.clone(), task_index,
                      attempt_id = attempt_id.clone(),
                      result = std::move(result)]() mutable {
                       if (!weak_lifetime.expired()) {
                         complete_task(run_id, task_index, attempt_id,
                                       std::move(result));
                       }
                     });
    return;
  }

  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end() ||
      task_index >= run_it->second.tasks.size()) {
    return;
  }
  auto &run = run_it->second;
  auto &task = run.tasks[task_index];
  auto *attempt = active_attempt(task, attempt_id);
  if (attempt == nullptr || is_terminal(attempt->state) ||
      is_terminal(task.snapshot.state)) {
    return;
  }

  task.instance_id.reset();
  if (run.active_attempts > 0) {
    run.active_attempts -= 1;
  }

  auto finish_failure = [&](std::error_code error) {
    const auto failure_class = classify_failure(error);
    attempt->failure_class = failure_class;
    attempt->error = error.message();
    task.snapshot.last_error = attempt->error;
    if (failure_class == FailureClass::Timeout) {
      attempt->termination_reason = TerminationReason::AttemptTimeout;
      (void)transition_attempt(*attempt, AttemptState::TimedOut);
    } else if (failure_class == FailureClass::Cancelled) {
      (void)transition_attempt(*attempt, AttemptState::Cancelled);
    } else {
      (void)transition_attempt(*attempt, AttemptState::Failed);
    }
    task.snapshot.active_attempt_id.reset();
    append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                    make_metadata({{"attempt_id", attempt_id.str()},
                                   {"state", std::string{
                                                 to_string_view(attempt->state)}},
                                   {"error", attempt->error}}));

    const auto &plan = run.plan->nodes[task_index].plan;
    if (retryable(failure_class) &&
        task.snapshot.attempt_count <=
            static_cast<std::uint32_t>(plan.max_retries) &&
        run.snapshot.state != RunState::Stopping) {
      const auto delay = retry_delay(plan, task.snapshot.attempt_count);
      task.snapshot.next_attempt_at =
          std::chrono::system_clock::now() + delay;
      (void)transition_task(run, task_index, TaskState::RetryWaiting);
      schedule_retry(run_id, task_index);
      assert(invariants_hold(run));
      return;
    }

    const auto final_state = failure_class == FailureClass::Cancelled
                                 ? TaskState::Cancelled
                                 : TaskState::Failed;
    (void)transition_task(run, task_index, final_state);
    append_evidence(run, task_index, EvidenceType::TaskFailed,
                    make_metadata({{"error", task.snapshot.last_error}}));
    assert(invariants_hold(run));
    if (final_state == TaskState::Failed &&
        run.plan->policy.failure_policy == FailurePolicy::FailFast) {
      (void)request_stop(run_id, StopIntent::Fail, task.snapshot.last_error);
      return;
    }
    update_dependents(run_id, task_index);
  };

  if (run.snapshot.state == RunState::Stopping) {
    if (attempt->state != AttemptState::Terminating) {
      (void)transition_attempt(*attempt, AttemptState::Terminating);
    }
    attempt->termination_reason =
        run.snapshot.stop_intent == StopIntent::Cancel
            ? TerminationReason::RunCancelled
            : TerminationReason::RunFailed;
    (void)transition_attempt(*attempt, AttemptState::Cancelled);
    task.snapshot.active_attempt_id.reset();
    task.snapshot.last_error = run.snapshot.stop_reason;
    (void)transition_task(run, task_index, TaskState::Cancelled);
    append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                    make_metadata({{"attempt_id", attempt_id.str()},
                                   {"state", "cancelled"}}));
    assert(invariants_hold(run));
    settle_control_state(run_id);
    return;
  }

  if (!result) {
    finish_failure(result.error());
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  if (attempt->state == AttemptState::Starting) {
    (void)transition_attempt(*attempt, AttemptState::Running);
  }

  const auto &node = run.plan->nodes[task_index].plan;
  if (!outputs_match_contract(node, *result)) {
    finish_failure(make_error_code(Error::ProtocolError));
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  std::optional<std::error_code> output_error;
  for (auto &[port, value] : *result) {
    if (port == "exit_code") {
      if (const auto *exit_code = std::get_if<std::int64_t>(&value)) {
        attempt->exit_code = static_cast<int>(*exit_code);
      }
    }
    auto stored = run.values->put(
        OutputRef{.node_id = run.plan->nodes[task_index].plan.node_id.clone(),
                  .port = port.clone()},
        std::move(value));
    if (!stored) {
      output_error = stored.error();
      break;
    }
  }

  if (output_error) {
    finish_failure(*output_error);
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  (void)transition_attempt(*attempt, AttemptState::Succeeded);
  task.snapshot.active_attempt_id.reset();
  task.snapshot.last_error.clear();
  (void)transition_task(run, task_index, TaskState::Succeeded);
  append_evidence(run, task_index, EvidenceType::AttemptCompleted,
                  make_metadata({{"attempt_id", attempt_id.str()},
                                 {"state", "succeeded"}}));
  append_evidence(run, task_index, EvidenceType::TaskCompleted);

  if (run.plan->nodes[task_index].plan.checkpoint) {
    checkpoint(run);
    append_evidence(run, task_index, EvidenceType::Checkpoint);
  }
  assert(invariants_hold(run));
  update_dependents(run_id, task_index);
  settle_control_state(run_id);
  dispatch(run_id);
}

auto WorkflowRuntime::schedule_retry(const WorkflowRunId &run_id,
                                     std::size_t task_index) -> void {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end() ||
      task_index >= run_it->second.tasks.size()) {
    return;
  }
  auto &run = run_it->second;
  auto &task = run.tasks[task_index];
  const auto delay =
      retry_delay(run.plan->nodes[task_index].plan,
                  task.snapshot.attempt_count);
  task.retry_handle = runtime_.schedule_after_on(
      owner, delay,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), task_index] {
        if (weak_lifetime.expired()) {
          return;
        }
        auto &owner_state = shard_states_[owner_shard(run_id)];
        const auto active = owner_state.active_runs.find(run_id.str());
        if (active == owner_state.active_runs.end() ||
            task_index >= active->second.tasks.size()) {
          return;
        }
        auto &run = active->second;
        auto &task = run.tasks[task_index];
        task.retry_handle = {};
        if (task.snapshot.state != TaskState::RetryWaiting ||
            run.snapshot.state == RunState::Stopping ||
            is_terminal(run.snapshot.state)) {
          return;
        }
        (void)transition_task(run, task_index, TaskState::Ready);
        run.ready.push_back(task_index);
        assert(invariants_hold(run));
        dispatch(run_id);
      });
}

auto WorkflowRuntime::update_dependents(const WorkflowRunId &run_id,
                                        std::size_t completed_index) -> void {
  auto &run = shard_states_[owner_shard(run_id)].active_runs.at(run_id.str());
  for (const auto dependent : run.plan->nodes[completed_index].dependents) {
    if (dependent >= run.tasks.size() ||
        run.tasks[dependent].snapshot.state != TaskState::Pending) {
      continue;
    }

    bool all_terminal = true;
    bool all_success = true;
    for (const auto dependency : run.plan->nodes[dependent].dependencies) {
      const auto state = run.tasks[dependency].snapshot.state;
      all_terminal = all_terminal && is_terminal(state);
      all_success = all_success && is_success(state);
    }
    if (!all_terminal) {
      continue;
    }

    if (!all_success) {
      auto reason = SkipReason::BranchNotSelected;
      for (const auto dependency : run.plan->nodes[dependent].dependencies) {
        const auto state = run.tasks[dependency].snapshot.state;
        if (state == TaskState::Failed) {
          reason = SkipReason::UpstreamFailed;
          break;
        }
        if (state == TaskState::Cancelled) {
          reason = SkipReason::UpstreamCancelled;
        }
      }
      run.tasks[dependent].snapshot.skip_reason = reason;
      (void)transition_task(run, dependent, TaskState::Skipped);
      update_dependents(run_id, dependent);
      continue;
    }

    auto passes = conditions_pass(run, dependent);
    if (!passes) {
      (void)request_stop(run_id, StopIntent::Fail, passes.error().message());
      return;
    }
    if (!*passes) {
      run.tasks[dependent].snapshot.skip_reason = SkipReason::ConditionFalse;
      (void)transition_task(run, dependent, TaskState::Skipped);
      update_dependents(run_id, dependent);
      continue;
    }

    (void)transition_task(run, dependent, TaskState::Ready);
    run.ready.push_back(dependent);
  }
}

auto WorkflowRuntime::conditions_pass(const ActiveRun &run,
                                      std::size_t node_index) const
    -> Result<bool> {
  const auto &node_id = run.plan->nodes[node_index].plan.node_id;
  for (const auto &edge : run.plan->edges) {
    if (edge.target != node_id || edge.condition.kind == ConditionKind::Always) {
      continue;
    }
    auto value = run.values->get(edge.source);
    if (!value) {
      return fail(value.error());
    }

    bool passed = false;
    switch (edge.condition.kind) {
    case ConditionKind::Always:
      passed = true;
      break;
    case ConditionKind::BoolEquals:
      passed = value_truthy(**value) == edge.condition.expected_bool;
      break;
    case ConditionKind::StringEquals:
      passed = value_to_string(**value) == edge.condition.expected_string;
      break;
    }
    if (!passed) {
      return ok(false);
    }
  }
  return ok(true);
}

auto WorkflowRuntime::input_values(const ActiveRun &run,
                                   std::size_t node_index) const
    -> Result<ExecutorInputs> {
  ExecutorInputs inputs;
  for (const auto &binding : run.plan->nodes[node_index].plan.inputs) {
    auto value = run.values->get(binding.source);
    if (!value) {
      return fail(value.error());
    }
    inputs.emplace(binding.input.str(), std::move(*value));
  }
  return ok(std::move(inputs));
}

auto WorkflowRuntime::request_stop(const WorkflowRunId &run_id,
                                   StopIntent intent, std::string reason)
    -> Result<void> {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    return fail(Error::InvalidState);
  }
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return fail(Error::NotFound);
  }
  auto &run = it->second;
  if (is_terminal(run.snapshot.state)) {
    return fail(Error::InvalidState);
  }
  if (run.snapshot.state == RunState::Stopping) {
    return ok();
  }

  run.snapshot.stop_intent = intent;
  run.snapshot.stop_reason = std::move(reason);
  if (intent == StopIntent::Fail) {
    run.snapshot.error = run.snapshot.stop_reason;
  }
  auto transitioned = transition_run(run, RunState::Stopping);
  if (!transitioned) {
    return transitioned;
  }
  append_evidence(run, run.tasks.size(), EvidenceType::RunStopRequested,
                  make_metadata({{"intent", std::string{to_string_view(intent)}},
                                 {"reason", run.snapshot.stop_reason}}));
  run.ready.clear();
  std::vector<std::pair<std::string, InstanceId>> instances_to_cancel;

  for (std::size_t index = 0; index < run.tasks.size(); ++index) {
    auto &task = run.tasks[index];
    if (task.retry_handle.valid()) {
      runtime_.cancel_after_on(owner, task.retry_handle);
      task.retry_handle = {};
    }
    if (task.snapshot.state == TaskState::Pending ||
        task.snapshot.state == TaskState::Ready ||
        task.snapshot.state == TaskState::RetryWaiting) {
      task.snapshot.last_error = run.snapshot.stop_reason;
      (void)transition_task(run, index, TaskState::Cancelled);
      continue;
    }
    if (task.snapshot.state != TaskState::Running ||
        !task.snapshot.active_attempt_id) {
      continue;
    }
    auto *attempt = active_attempt(task, *task.snapshot.active_attempt_id);
    if (attempt != nullptr && !is_terminal(attempt->state)) {
      attempt->termination_reason =
          intent == StopIntent::Cancel ? TerminationReason::RunCancelled
                                       : TerminationReason::RunFailed;
      (void)transition_attempt(*attempt, AttemptState::Terminating);
      emit_task_state(run, index);
    }
    if (task.instance_id) {
      instances_to_cancel.emplace_back(
          run.plan->nodes[index].plan.executor, task.instance_id->clone());
    }
  }
  assert(invariants_hold(run));

  // Executor callbacks may complete synchronously. Do not retain or access
  // ActiveRun references after crossing this external boundary.
  for (const auto &[executor, instance_id] : instances_to_cancel) {
    executors_.cancel(executor, instance_id);
  }
  (void)finalize_run_if_ready(run_id);
  return ok();
}

auto WorkflowRuntime::settle_control_state(const WorkflowRunId &run_id)
    -> void {
  if (finalize_run_if_ready(run_id)) {
    return;
  }
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return;
  }
  auto &run = it->second;
  if (run.snapshot.state == RunState::Pausing && run.active_attempts == 0) {
    if (transition_run(run, RunState::Paused)) {
      append_evidence(run, run.tasks.size(), EvidenceType::RunPaused);
      assert(invariants_hold(run));
    }
  }
}

auto WorkflowRuntime::finalize_run_if_ready(const WorkflowRunId &run_id)
    -> bool {
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end()) {
    return true;
  }
  auto &run = it->second;

  bool all_terminal = true;
  bool any_failed = false;
  bool any_cancelled = false;
  for (const auto &task : run.tasks) {
    all_terminal = all_terminal && is_terminal(task.snapshot.state);
    any_failed = any_failed || task.snapshot.state == TaskState::Failed;
    any_cancelled =
        any_cancelled || task.snapshot.state == TaskState::Cancelled;
  }

  if (!all_terminal || run.active_attempts != 0) {
    return false;
  }

  // A task-level cancellation is also a run-level cancellation, but the run
  // state machine deliberately requires cancellation to pass through
  // Stopping. Without this bridge, an executor that rejects start with
  // Error::Cancelled leaves the run permanently active because
  // Running -> Cancelled is not a legal transition.
  if (any_cancelled && run.snapshot.state != RunState::Stopping) {
    run.snapshot.stop_intent = StopIntent::Cancel;
    if (run.snapshot.stop_reason.empty()) {
      const auto cancelled_task = std::ranges::find_if(
          run.tasks, [](const auto &task) {
            return task.snapshot.state == TaskState::Cancelled;
          });
      if (cancelled_task != run.tasks.end()) {
        run.snapshot.stop_reason = cancelled_task->snapshot.last_error;
      }
    }
    if (!transition_run(run, RunState::Stopping)) {
      return false;
    }
    append_evidence(
        run, run.tasks.size(), EvidenceType::RunStopRequested,
        make_metadata({{"intent", std::string{to_string_view(StopIntent::Cancel)}},
                       {"reason", run.snapshot.stop_reason}}));
  }

  RunState terminal_state = RunState::Succeeded;
  if (run.snapshot.state == RunState::Stopping) {
    switch (run.snapshot.stop_intent.value_or(StopIntent::Fail)) {
    case StopIntent::Succeed:
      terminal_state = RunState::Succeeded;
      break;
    case StopIntent::Fail:
      terminal_state = RunState::Failed;
      break;
    case StopIntent::Cancel:
      terminal_state = RunState::Cancelled;
      break;
    }
  } else {
    terminal_state = any_cancelled ? RunState::Cancelled
                                   : (any_failed ? RunState::Failed
                                                 : RunState::Succeeded);
  }
  if (terminal_state == RunState::Succeeded) {
    for (const auto &published : run.plan->outputs) {
      if (run.values->contains(published)) {
        continue;
      }
      terminal_state = RunState::Failed;
      run.snapshot.error = std::format(
          "required workflow output is missing: {}.{}",
          published.node_id, published.port);
      break;
    }
  }
  if (run.snapshot.error.empty() && terminal_state != RunState::Succeeded) {
    const auto failed_task = std::ranges::find_if(
        run.tasks, [terminal_state](const auto &task) {
          return terminal_state == RunState::Failed
                     ? task.snapshot.state == TaskState::Failed
                     : task.snapshot.state == TaskState::Cancelled;
        });
    if (failed_task != run.tasks.end()) {
      run.snapshot.error = failed_task->snapshot.last_error;
    }
  }
  if (!transition_run(run, terminal_state)) {
    return false;
  }
  assert(invariants_hold(run));
  const auto evidence_type =
      run.snapshot.state == RunState::Succeeded
          ? EvidenceType::RunCompleted
          : (run.snapshot.state == RunState::Cancelled
                 ? EvidenceType::RunCancelled
                 : EvidenceType::RunFailed);
  append_evidence(run, run.tasks.size(), evidence_type,
                  make_metadata({{"state", std::string{
                                              to_string_view(run.snapshot.state)}}}));
  checkpoint(run);

  if (run.deadline_handle.valid()) {
    runtime_.cancel_after_on(owner, run.deadline_handle);
    run.deadline_handle = {};
  }

  auto snapshot = make_snapshot(run);
  auto values = run.values->snapshot();
  if (values) {
    state.completed_values[run_id.str()] = std::move(*values);
  }
  state.completed_runs[run_id.str()] = snapshot;
  state.completed_order.push_back(run_id.str());
  if (run.callbacks.on_complete) {
    run.callbacks.on_complete(run_id, snapshot);
  }
  state.active_runs.erase(it);
  active_run_count_.fetch_sub(1, std::memory_order_release);
  notify_lifecycle_changed();
  while (state.completed_order.size() > max_completed_runs_) {
    auto expired = std::move(state.completed_order.front());
    state.completed_order.pop_front();
    state.completed_runs.erase(expired);
    state.completed_values.erase(expired);
    (void)checkpoint_store_->erase(WorkflowRunId{expired});
    std::lock_guard lock(idempotency_mutex_);
    std::erase_if(idempotency_runs_, [&](const auto &entry) {
      return entry.second.str() == expired;
    });
  }
  return true;
}

auto WorkflowRuntime::make_snapshot(const ActiveRun &run) const
    -> std::shared_ptr<const RunSnapshot> {
  auto snapshot = run.snapshot;
  snapshot.tasks.clear();
  snapshot.tasks.reserve(run.tasks.size());
  for (const auto &task : run.tasks) {
    snapshot.tasks.push_back(task.snapshot);
  }
  return std::make_shared<const RunSnapshot>(std::move(snapshot));
}

auto WorkflowRuntime::emit_run_state(ActiveRun &run) -> void {
  if (run.callbacks.on_run_state) {
    run.callbacks.on_run_state(run.snapshot);
  }
}

auto WorkflowRuntime::emit_task_state(ActiveRun &run, std::size_t task_index)
    -> void {
  run.snapshot.tasks[task_index] = run.tasks[task_index].snapshot;
  if (run.callbacks.on_task_state) {
    run.callbacks.on_task_state(run.snapshot.run_id,
                                run.tasks[task_index].snapshot);
  }
}

auto WorkflowRuntime::append_evidence(const ActiveRun &run,
                                      std::size_t node_index, EvidenceType type,
                                      JsonValue metadata) -> void {
  EvidenceRecord record;
  record.run_id = run.snapshot.run_id.clone();
  if (node_index < run.plan->nodes.size()) {
    record.node_id = run.plan->nodes[node_index].plan.node_id.clone();
  }
  record.type = type;
  record.actor = run.trigger.principal;
  record.metadata = std::move(metadata);
  (void)evidence_ledger_->append(std::move(record));
}

auto WorkflowRuntime::checkpoint(ActiveRun &run) -> void {
  auto values = run.values->snapshot();
  if (!values) {
    return;
  }
  WorkflowCheckpoint checkpoint{
      .plan = source_plan(*run.plan),
      .trigger = run.trigger,
      .snapshot = *make_snapshot(run),
      .values = std::move(*values),
      .created_at = std::chrono::system_clock::now(),
  };
  (void)checkpoint_store_->save(std::move(checkpoint));
}

auto WorkflowRuntime::execute_task(WorkflowRunId run_id,
                                   std::size_t task_index,
                                   AttemptId attempt_id, NodePlan node,
                                   ExecutorInputs inputs)
    -> task<Result<ExecutorOutputs>> {
  if (node.executor.empty()) {
    co_return fail(Error::InvalidArgument);
  }

  const auto instance_id = instance_id_for(run_id, node.node_id, attempt_id);
  auto result = co_await execute_task_async(
      runtime_, owner_shard(run_id), executors_, node.executor,
      TaskExecutionRequest{
          .instance_id = instance_id.clone(),
          .config = std::move(node.config),
          .inputs = std::move(inputs),
          .outputs = node.outputs,
          .timeout = node.timeout,
      },
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), task_index,
       attempt_id = attempt_id.clone()](std::string_view state) mutable {
        if (state != "running" || weak_lifetime.expired()) {
          return;
        }
        const auto owner = owner_shard(run_id);
        auto mark_running =
            [this, weak_lifetime, run_id = run_id.clone(), task_index,
             attempt_id = attempt_id.clone()]() mutable {
              if (weak_lifetime.expired()) {
                return;
              }
              auto &owner_state = shard_states_[owner_shard(run_id)];
              const auto active = owner_state.active_runs.find(run_id.str());
              if (active == owner_state.active_runs.end()) {
                return;
              }
              mark_attempt_running(active->second, task_index, attempt_id);
            };
        if (runtime_.is_current_shard() && runtime_.current_shard() == owner) {
          mark_running();
        } else {
          runtime_.post_to(owner, std::move(mark_running));
        }
      });
  co_return result;
}

auto WorkflowRuntime::snapshot(const WorkflowRunId &run_id) const
    -> task<Result<std::shared_ptr<const RunSnapshot>>> {
  const auto target = owner_shard(run_id);
  auto copy_snapshot = [this, &run_id, target]()
      -> Result<std::shared_ptr<const RunSnapshot>> {
    const auto &state = shard_states_[target];
    if (const auto active = state.active_runs.find(run_id.str());
        active != state.active_runs.end()) {
      return ok(make_snapshot(active->second));
    }
    if (const auto completed = state.completed_runs.find(run_id.str());
        completed != state.completed_runs.end()) {
      return ok(completed->second);
    }
    return fail(Error::NotFound);
  };

  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    co_return copy_snapshot();
  }

  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::shared_ptr<const RunSnapshot>)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)),
                        std::shared_ptr<const RunSnapshot>{});
                return;
              }
              const auto &state = shard_states_[target];
              if (const auto active = state.active_runs.find(run_id.str());
                  active != state.active_runs.end()) {
                handler(boost::system::error_code{},
                        make_snapshot(active->second));
                return;
              }
              if (const auto completed =
                      state.completed_runs.find(run_id.str());
                  completed != state.completed_runs.end()) {
                handler(boost::system::error_code{}, completed->second);
                return;
              }
              handler(to_boost_error(make_error_code(Error::NotFound)),
                      std::shared_ptr<const RunSnapshot>{});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok(std::move(*result));
}

auto WorkflowRuntime::output(const WorkflowRunId &run_id,
                             const OutputRef &output_ref) const
    -> task<Result<std::shared_ptr<const WorkflowValue>>> {
  const auto target = owner_shard(run_id);
  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::shared_ptr<const WorkflowValue>)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), output_ref, target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id),
                     output_ref = std::move(output_ref), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)),
                        std::shared_ptr<const WorkflowValue>{});
                return;
              }
              auto &state = shard_states_[target];
              if (auto active = state.active_runs.find(run_id.str());
                  active != state.active_runs.end()) {
                auto result = active->second.values->get(output_ref);
                if (!result) {
                  handler(to_boost_error(result.error()),
                          std::shared_ptr<const WorkflowValue>{});
                  return;
                }
                handler(boost::system::error_code{}, std::move(*result));
                return;
              }
              if (const auto completed =
                      state.completed_values.find(run_id.str());
                  completed != state.completed_values.end()) {
                for (const auto &[ref, stored] : completed->second) {
                  if (ref == output_ref) {
                    handler(boost::system::error_code{},
                            std::make_shared<const WorkflowValue>(stored));
                    return;
                  }
                }
              }
              handler(to_boost_error(make_error_code(Error::NotFound)),
                      std::shared_ptr<const WorkflowValue>{});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok(std::move(*result));
}

auto WorkflowRuntime::pause(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)));
                return;
              }
              auto &state = shard_states_[target];
              const auto it = state.active_runs.find(run_id.str());
              if (it == state.active_runs.end()) {
                handler(to_boost_error(make_error_code(Error::NotFound)));
                return;
              }
              auto &run = it->second;
              if (run.snapshot.state == RunState::Pausing ||
                  run.snapshot.state == RunState::Paused) {
                handler(boost::system::error_code{});
                return;
              }
              auto transitioned = transition_run(run, RunState::Pausing);
              if (!transitioned) {
                handler(to_boost_error(transitioned.error()));
                return;
              }
              append_evidence(run, run.tasks.size(),
                              EvidenceType::RunPauseRequested);
              settle_control_state(run_id);
              handler(boost::system::error_code{});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

auto WorkflowRuntime::resume(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id),
                     target, handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)));
                return;
              }
              auto &state = shard_states_[target];
              const auto run_it = state.active_runs.find(run_id.str());
              if (run_it == state.active_runs.end()) {
                handler(to_boost_error(make_error_code(Error::NotFound)));
                return;
              }
              auto &run = run_it->second;
              if (run.snapshot.state == RunState::Running) {
                handler(boost::system::error_code{});
                return;
              }
              auto transitioned = transition_run(run, RunState::Running);
              if (!transitioned) {
                handler(to_boost_error(transitioned.error()));
                return;
              }
              append_evidence(run, run.tasks.size(), EvidenceType::RunResumed);
              dispatch(run_id);
              handler(boost::system::error_code{});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

auto WorkflowRuntime::cancel(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  auto result = co_await co_as_result(boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow), void(boost::system::error_code)>(
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, weak_lifetime, run_id = std::move(run_id), target,
                     handler = std::move(handler)]() mutable {
              if (weak_lifetime.expired()) {
                handler(to_boost_error(make_error_code(Error::Cancelled)));
                return;
              }
              auto stopped =
                  request_stop(run_id, StopIntent::Cancel, "cancel requested");
              if (!stopped) {
                handler(to_boost_error(stopped.error()));
                return;
              }
              handler(boost::system::error_code{});
            });
      },
      dagforge::use_nothrow));
  if (!result) {
    co_return fail(result.error());
  }
  co_return ok();
}

auto WorkflowRuntime::evidence(const WorkflowRunId &run_id) const
    -> std::vector<EvidenceRecord> {
  return evidence_ledger_->records(run_id);
}

} // namespace dagforge::workflow
