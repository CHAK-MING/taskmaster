#include "dagforge/workflow/workflow_runtime.hpp"

#include "dagforge/core/scope_exit.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include "../detail/retry_policy.hpp"
#include "../detail/shard_request.hpp"
#include "../detail/state_machine.hpp"

#include <boost/asio/async_result.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <map>
#include <memory>
#include <mutex>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto is_success(TaskState state) noexcept -> bool {
  return state == TaskState::Succeeded;
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
  if (const auto *json = std::get_if<JsonPayload>(&value)) {
    return !json->is_null();
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

[[nodiscard]] auto instance_id_for(const WorkflowRunId &run_id,
                                   const WorkflowNodeId &node_id,
                                   const AttemptId &attempt_id)
    -> InstanceId {
  return InstanceId{std::format("{}_{}_{}", run_id, node_id, attempt_id)};
}

[[nodiscard]] auto failure_attempt_state(Error error) noexcept
    -> AttemptState {
  if (error == Error::Timeout) {
    return AttemptState::TimedOut;
  }
  if (error == Error::Cancelled) {
    return AttemptState::Cancelled;
  }
  return AttemptState::Failed;
}

[[nodiscard]] auto persistence_failure(std::error_code cause,
                                       std::string code,
                                       std::string message,
                                       std::string component)
    -> ExecutionFailure {
  auto details = JsonPayload::from(
      glz::obj{"cause",
               FailureCause{
                   .category = cause.category().name(),
                   .value = cause.value(),
                   .message = cause.message(),
               },
               "component", component});
  return make_execution_failure(
      Error::PersistenceError, std::move(code), std::move(message),
      details ? std::move(*details) : JsonPayload{});
}

[[nodiscard]] constexpr auto executor_class(std::string_view executor) noexcept
    -> std::string_view {
  if (executor == "command") {
    return "command";
  }
  if (executor == "http") {
    return "http";
  }
  return "other";
}

[[nodiscard]] auto
metric_error_type(const std::optional<ExecutionFailure> &failure)
    -> std::string {
  return failure ? std::string{to_string_view(failure->kind)} : std::string{};
}

[[nodiscard]] auto metric_error_type(std::error_code error) -> std::string {
  if (error.category() == error_category() && error.value() >= 0 &&
      error.value() <= std::to_underlying(Error::Unknown)) {
    return std::string{to_string_view(static_cast<Error>(error.value()))};
  }
  return std::string{to_string_view(Error::Unknown)};
}

template <typename Clock>
[[nodiscard]] auto elapsed_ns(std::chrono::time_point<Clock> started,
                              std::chrono::time_point<Clock> finished) noexcept
    -> std::uint64_t {
  if (started == std::chrono::time_point<Clock>{} || finished < started) {
    return 0;
  }
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started)
          .count();
  return elapsed > 0 ? static_cast<std::uint64_t>(elapsed) : 0;
}

} // namespace

struct WorkflowRuntime::MetricsState {
  static constexpr std::array<std::uint64_t, 15> kDurationBoundsNs{
      5'000'000ULL,      10'000'000ULL,      25'000'000ULL,
      50'000'000ULL,     100'000'000ULL,     250'000'000ULL,
      500'000'000ULL,    1'000'000'000ULL,   2'500'000'000ULL,
      5'000'000'000ULL,  10'000'000'000ULL,  30'000'000'000ULL,
      60'000'000'000ULL, 120'000'000'000ULL, 300'000'000'000ULL,
  };

  struct SeriesKey {
    std::string executor;
    std::string result;
    std::string error_type;

    auto operator<=>(const SeriesKey &) const = default;
  };

  struct PersistenceKey {
    std::string store;
    std::string operation;
    std::string result;
    std::string error_type;

    auto operator<=>(const PersistenceKey &) const = default;
  };

  struct Aggregate {
    std::uint64_t total{0};
    std::uint64_t sum_ns{0};
    std::array<std::uint64_t, kDurationBoundsNs.size() + 1> buckets{};

    auto observe(std::uint64_t duration_ns) noexcept -> void {
      const auto bucket =
          std::lower_bound(kDurationBoundsNs.begin(), kDurationBoundsNs.end(),
                           duration_ns) -
          kDurationBoundsNs.begin();
      ++buckets[static_cast<std::size_t>(bucket)];
      ++total;
      sum_ns += duration_ns;
    }

    [[nodiscard]] auto duration_snapshot() const
        -> WorkflowDurationMetricSnapshot {
      return WorkflowDurationMetricSnapshot{
          .bounds_ns = {kDurationBoundsNs.begin(), kDurationBoundsNs.end()},
          .bucket_counts = {buckets.begin(), buckets.end()},
          .count = total,
          .sum_ns = sum_ns,
      };
    }
  };

  mutable std::mutex mutex;
  std::uint64_t runs_paused{0};
  std::uint64_t runs_stopping{0};
  std::uint64_t tasks_ready{0};
  std::uint64_t tasks_retry_waiting{0};
  std::map<std::string, std::uint64_t> tasks_active;
  std::map<std::string, std::uint64_t> attempts_active;
  std::map<std::string, std::uint64_t> retries;
  std::map<SeriesKey, Aggregate> runs;
  std::map<SeriesKey, Aggregate> tasks;
  std::map<SeriesKey, Aggregate> attempts;
  std::map<SeriesKey, Aggregate> task_queue;
  std::map<SeriesKey, Aggregate> repair_runs;
  std::uint64_t repair_nodes_reused{0};
  std::uint64_t repair_nodes_invalidated{0};
  std::map<PersistenceKey, Aggregate> persistence;

  static auto adjust(std::uint64_t &value, int delta) noexcept -> void {
    if (delta > 0) {
      value += static_cast<std::uint64_t>(delta);
    } else if (value > 0) {
      --value;
    }
  }

  auto activate_run(RunState state) -> void {
    std::lock_guard lock(mutex);
    if (state == RunState::Paused) {
      ++runs_paused;
    } else if (state == RunState::Stopping) {
      ++runs_stopping;
    }
  }

  auto transition_run(RunState from, RunState to) -> void {
    std::lock_guard lock(mutex);
    if (from == RunState::Paused) {
      adjust(runs_paused, -1);
    } else if (from == RunState::Stopping) {
      adjust(runs_stopping, -1);
    }
    if (to == RunState::Paused) {
      adjust(runs_paused, 1);
    } else if (to == RunState::Stopping) {
      adjust(runs_stopping, 1);
    }
  }

  auto activate_task(std::string_view executor, TaskState state) -> void {
    const auto classification = std::string{executor_class(executor)};
    std::lock_guard lock(mutex);
    if (state == TaskState::Running) {
      ++tasks_active[classification];
    } else if (state == TaskState::Ready) {
      ++tasks_ready;
    } else if (state == TaskState::RetryWaiting) {
      ++tasks_retry_waiting;
    }
  }

  auto transition_task(std::string_view executor, TaskState from, TaskState to,
                       const TaskSnapshot &snapshot,
                       std::optional<std::uint64_t> queue_ns) -> void {
    const auto classification = std::string{executor_class(executor)};
    std::lock_guard lock(mutex);
    if (from == TaskState::Running) {
      adjust(tasks_active[classification], -1);
    } else if (from == TaskState::Ready) {
      adjust(tasks_ready, -1);
    } else if (from == TaskState::RetryWaiting) {
      adjust(tasks_retry_waiting, -1);
    }
    if (to == TaskState::Running) {
      adjust(tasks_active[classification], 1);
    } else if (to == TaskState::Ready) {
      adjust(tasks_ready, 1);
    } else if (to == TaskState::RetryWaiting) {
      adjust(tasks_retry_waiting, 1);
    }
    if (queue_ns) {
      task_queue[SeriesKey{.executor = classification}].observe(*queue_ns);
    }
    if (!is_terminal(to)) {
      return;
    }
    tasks[SeriesKey{
              .executor = classification,
              .result = std::string{to_string_view(to)},
              .error_type = metric_error_type(snapshot.failure),
          }]
        .observe(elapsed_ns(snapshot.started_at, snapshot.finished_at));
  }

  auto attempt_started(std::string_view executor, std::uint32_t number)
      -> void {
    const auto classification = std::string{executor_class(executor)};
    std::lock_guard lock(mutex);
    ++attempts_active[classification];
    if (number > 1) {
      ++retries[classification];
    }
  }

  auto attempt_completed(std::string_view executor,
                         const AttemptSnapshot &snapshot) -> void {
    const auto classification = std::string{executor_class(executor)};
    std::lock_guard lock(mutex);
    adjust(attempts_active[classification], -1);
    attempts[SeriesKey{
                 .executor = classification,
                 .result = std::string{to_string_view(snapshot.state)},
                 .error_type = metric_error_type(snapshot.failure),
             }]
        .observe(elapsed_ns(snapshot.created_at, snapshot.finished_at));
  }

  auto run_completed(const RunSnapshot &snapshot) -> void {
    const auto duration_ns =
        elapsed_ns(snapshot.started_at, snapshot.finished_at);
    const auto result = std::string{to_string_view(snapshot.state)};
    const auto error_type = metric_error_type(snapshot.failure);
    std::lock_guard lock(mutex);
    runs[SeriesKey{.result = result, .error_type = error_type}].observe(
        duration_ns);
    if (snapshot.parent_run_id) {
      repair_runs[SeriesKey{.result = result, .error_type = error_type}]
          .observe(duration_ns);
    }
  }

  auto repair_decision(bool reused) -> void {
    std::lock_guard lock(mutex);
    if (reused) {
      ++repair_nodes_reused;
    } else {
      ++repair_nodes_invalidated;
    }
  }

  auto persistence_operation(std::string_view store, std::string_view operation,
                             std::string_view result, std::string error_type,
                             std::uint64_t duration_ns) -> void {
    std::lock_guard lock(mutex);
    persistence[PersistenceKey{
                    .store = std::string{store},
                    .operation = std::string{operation},
                    .result = std::string{result},
                    .error_type = std::move(error_type),
                }]
        .observe(duration_ns);
  }

  [[nodiscard]] auto snapshot() const -> WorkflowMetricsSnapshot {
    std::lock_guard lock(mutex);
    WorkflowMetricsSnapshot out{
        .runs_paused = runs_paused,
        .runs_stopping = runs_stopping,
        .tasks_ready = tasks_ready,
        .tasks_retry_waiting = tasks_retry_waiting,
        .repair_nodes_reused = repair_nodes_reused,
        .repair_nodes_invalidated = repair_nodes_invalidated,
    };
    for (const auto &[executor, value] : tasks_active) {
      out.tasks_active.emplace_back(executor, value);
    }
    for (const auto &[executor, value] : attempts_active) {
      out.attempts_active.emplace_back(executor, value);
    }
    for (const auto &[executor, value] : retries) {
      out.retries.emplace_back(executor, value);
    }
    const auto copy_series = [](const auto &source, auto &target) {
      for (const auto &[key, aggregate] : source) {
        target.push_back(WorkflowMetricSeriesSnapshot{
            .executor_class = key.executor,
            .result = key.result,
            .error_type = key.error_type,
            .total = aggregate.total,
            .duration = aggregate.duration_snapshot(),
        });
      }
    };
    copy_series(runs, out.runs);
    copy_series(tasks, out.tasks);
    copy_series(attempts, out.attempts);
    copy_series(task_queue, out.task_queue);
    copy_series(repair_runs, out.repair_runs);
    for (const auto &[key, aggregate] : persistence) {
      out.persistence.push_back(WorkflowPersistenceMetricSnapshot{
          .store = key.store,
          .operation = key.operation,
          .result = key.result,
          .error_type = key.error_type,
          .total = aggregate.total,
          .duration = aggregate.duration_snapshot(),
      });
    }
    return out;
  }
};

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
      shard_states_(runtime.shard_count()),
      metrics_(std::make_unique<MetricsState>()) {
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

WorkflowRuntime::~WorkflowRuntime() {
  lifetime_token_.reset();
  const auto tracker = initialization_tracker_;
  if (!tracker || tracker->pending.load(std::memory_order_acquire) == 0) {
    return;
  }
  if (!runtime_.is_running()) {
    return;
  }
  assert(!runtime_.is_current_shard() &&
         "WorkflowRuntime must not be destroyed on a shard while Run "
         "activation is pending");
  if (runtime_.is_current_shard()) {
    return;
  }
  std::unique_lock lock(tracker->mutex);
  tracker->changed.wait(lock, [tracker] {
    return tracker->pending.load(std::memory_order_acquire) == 0;
  });
}

template <typename Metadata>
auto WorkflowRuntime::append_typed_evidence(ActiveRun &run,
                                            std::size_t node_index,
                                            EvidenceType type,
                                            const Metadata &metadata) -> void {
  auto encoded = JsonPayload::from(metadata);
  if (!encoded) {
    record_persistence_failure(
        run, make_execution_failure(
                 Error::ProtocolError, "evidence_metadata_encode_failed",
                 "Workflow Evidence metadata could not be encoded"));
    return;
  }
  append_evidence(run, node_index, type, std::move(*encoded));
}

auto WorkflowRuntime::notify_lifecycle_changed() noexcept -> void {
  {
    std::lock_guard lock(lifecycle_mutex_);
  }
  lifecycle_changed_.notify_all();
}

auto WorkflowRuntime::lifecycle_quiesced() const noexcept -> bool {
  return initialization_tracker_->pending.load(std::memory_order_acquire) == 0 &&
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

auto WorkflowRuntime::initialize_checkpoint_run(
    std::shared_ptr<const ExecutionPlan> plan,
    WorkflowCheckpoint restored_checkpoint, WorkflowCallbacks callbacks,
    ActivationKind activation,
    std::vector<RepairNodeDecision> repair_decisions) -> void {
  if (!plan ||
      restored_checkpoint.snapshot.tasks.size() != plan->nodes.size()) {
    log::error("Cannot activate workflow checkpoint with mismatched Plan");
    return;
  }

  const auto owner = runtime_.current_shard();
  auto &state = shard_states_[owner];
  const auto run_id = restored_checkpoint.snapshot.run_id.clone();
  const auto now = std::chrono::system_clock::now();

  ActiveRun run;
  run.plan = std::move(plan);
  run.trigger = std::move(restored_checkpoint.trigger);
  run.snapshot = std::move(restored_checkpoint.snapshot);
  run.callbacks = std::move(callbacks);
  run.values = std::make_unique<RunValueStore>(
      runtime_, owner, *artifact_store_,
      run.plan->policy.budget.max_total_output_bytes);

  detail::RestartPreparation restart_preparation;
  if (activation == ActivationKind::RestartRecovery) {
    restart_preparation = detail::rehydrate_for_restart(run.snapshot, now);
  }

  run.tasks.reserve(run.snapshot.tasks.size());
  for (std::size_t index = 0; index < run.snapshot.tasks.size(); ++index) {
    if (run.snapshot.tasks[index].node_id !=
        run.plan->nodes[index].plan.node_id) {
      log::error("Cannot activate workflow {} with reordered checkpoint Tasks",
                 run_id);
      return;
    }
    run.tasks.push_back(
        TaskRuntimeState{.snapshot = run.snapshot.tasks[index]});
  }
  for (auto &entry : restored_checkpoint.values) {
    auto restored =
        run.values->put(std::move(entry.output), std::move(entry.value));
    if (!restored) {
      const auto failure = make_execution_failure(
          restored.error(), "recovery_value_restore_failed",
          "A retained workflow value could not be restored");
      run.snapshot.state = RunState::Failed;
      run.snapshot.stop_intent = StopIntent::Fail;
      run.snapshot.stop_reason = failure.message;
      run.snapshot.failure = failure;
      run.snapshot.finished_at = now;
      for (auto &task : run.tasks) {
        if (!is_terminal(task.snapshot.state)) {
          task.snapshot.state = TaskState::Failed;
          task.snapshot.failure = failure;
          task.snapshot.finished_at = now;
        }
      }
      run.snapshot.tasks.clear();
      for (const auto &task : run.tasks) {
        run.snapshot.tasks.push_back(task.snapshot);
      }
      auto failed_checkpoint = WorkflowCheckpoint{
          .plan = source_plan(*run.plan),
          .trigger = run.trigger,
          .snapshot = run.snapshot,
          .values = {},
          .created_at = now,
      };
      (void)checkpoint_store_->save(std::move(failed_checkpoint));
      state.completed_runs[run_id.str()] =
          std::make_shared<const RunSnapshot>(run.snapshot);
      state.completed_values[run_id.str()] = {};
      state.completed_order.push_back(run_id.str());
      return;
    }
  }

  run.snapshot.tasks.clear();
  run.snapshot.tasks.reserve(run.tasks.size());
  for (const auto &task : run.tasks) {
    run.snapshot.tasks.push_back(task.snapshot);
  }
  auto [it, inserted] = state.active_runs.emplace(run_id.str(), std::move(run));
  if (!inserted) {
    return;
  }
  auto &active = it->second;
  active_run_count_.fetch_add(1, std::memory_order_release);
  metrics_->activate_run(active.snapshot.state);
  for (std::size_t index = 0; index < active.tasks.size(); ++index) {
    auto &task = active.tasks[index];
    const auto &executor = active.plan->nodes[index].plan.executor;
    if (task.snapshot.state == TaskState::Ready) {
      task.ready_at = now;
    }
    metrics_->activate_task(executor, task.snapshot.state);
  }

  auto primed = prime_ready_tasks(active);
  if (!primed) {
    (void)request_stop(
        run_id, StopIntent::Fail,
        "workflow dependencies could not be restored",
        make_execution_failure(primed.error(), "recovery_prime_failed",
                               "Workflow dependencies could not be restored"));
    return;
  }
  schedule_run_deadline(active);

  if (activation == ActivationKind::RestartRecovery) {
    append_typed_evidence(
        active, active.tasks.size(), EvidenceType::RunRecoveryResumed,
        glz::obj{"plan_id", active.plan->plan_id});
    for (const auto index : restart_preparation.finalized_attempts) {
      const auto &task = active.tasks[index].snapshot;
      const auto &attempt = task.attempts.back();
      append_typed_evidence(
          active, index, EvidenceType::AttemptCompleted,
          glz::obj{"attempt_id", attempt.attempt_id, "state", attempt.state,
                   "recovered", true, "failure", attempt.failure});
    }
    for (const auto index : restart_preparation.failed_tasks) {
      const auto &task = active.tasks[index].snapshot;
      append_typed_evidence(
          active, index, EvidenceType::TaskFailed,
          glz::obj{"recovered", true, "failure", *task.failure});
    }
  } else {
    append_typed_evidence(
        active, active.tasks.size(), EvidenceType::TriggerReceived,
        glz::obj{"source", active.trigger.source, "event_type",
                 active.trigger.event_type, "plan_digest", active.plan->digest,
                 "trace_id", active.trigger.trace.trace_id, "parent_span_id",
                 active.trigger.trace.parent_span_id});
    append_typed_evidence(
        active, active.tasks.size(), EvidenceType::PlanCompiled,
        glz::obj{"plan_id", active.plan->plan_id, "digest",
                 active.plan->digest});
  }
  if (activation == ActivationKind::RepairRun) {
    append_typed_evidence(
        active, active.tasks.size(), EvidenceType::RepairRunStarted,
        glz::obj{
            "parent_run_id",
            active.snapshot.parent_run_id
                ? active.snapshot.parent_run_id->value()
                : std::string_view{},
            "parent_plan_id",
            active.snapshot.parent_plan_id
                ? active.snapshot.parent_plan_id->value()
                : std::string_view{},
            "revision", active.snapshot.repair_revision, "reason",
            active.snapshot.repair_reason});
    for (const auto &decision : repair_decisions) {
      metrics_->repair_decision(decision.reused);
      const auto node = std::ranges::find_if(
          active.plan->nodes, [&](const auto &compiled) {
            return compiled.plan.node_id == decision.node_id;
          });
      if (node == active.plan->nodes.end()) {
        continue;
      }
      append_typed_evidence(
          active, node->index,
          decision.reused ? EvidenceType::TaskReused
                          : EvidenceType::TaskInvalidated,
          glz::obj{
              "reason", decision.reason, "parent_run_id",
              active.snapshot.parent_run_id
                  ? active.snapshot.parent_run_id->value()
                  : std::string_view{}});
    }
  }

  emit_run_state(active);
  if (active.snapshot.state == RunState::Stopping) {
    (void)finalize_run_if_ready(run_id);
    return;
  }
  if (active.snapshot.state == RunState::Paused) {
    return;
  }
  if (quiescing_.load(std::memory_order_acquire)) {
    (void)request_stop(run_id, StopIntent::Cancel,
                       "workflow runtime is shutting down");
    return;
  }
  dispatch(run_id);
}

auto WorkflowRuntime::prime_ready_tasks(ActiveRun &run) -> Result<void> {
  run.ready.clear();
  const auto now = std::chrono::system_clock::now();
  for (const auto index : run.plan->topological_order) {
    auto &task = run.tasks[index];
    if (task.snapshot.state == TaskState::RetryWaiting) {
      if (!task.snapshot.next_attempt_at ||
          *task.snapshot.next_attempt_at <= now) {
        task.snapshot.state = TaskState::Ready;
        task.snapshot.next_attempt_at.reset();
      } else {
        schedule_retry(run.snapshot.run_id, index);
        continue;
      }
    }
    if (task.snapshot.state == TaskState::Pending) {
      const auto &dependencies = run.plan->nodes[index].dependencies;
      const auto all_terminal = std::ranges::all_of(
          dependencies, [&](std::size_t dependency) {
            return is_terminal(run.tasks[dependency].snapshot.state);
          });
      if (!all_terminal) {
        continue;
      }
      const auto all_success = std::ranges::all_of(
          dependencies, [&](std::size_t dependency) {
            return is_success(run.tasks[dependency].snapshot.state);
          });
      if (!all_success) {
        task.snapshot.state = TaskState::Skipped;
        task.snapshot.skip_reason = SkipReason::BranchNotSelected;
        for (const auto dependency : dependencies) {
          const auto dependency_state =
              run.tasks[dependency].snapshot.state;
          if (dependency_state == TaskState::Failed) {
            task.snapshot.skip_reason = SkipReason::UpstreamFailed;
            break;
          }
          if (dependency_state == TaskState::Cancelled) {
            task.snapshot.skip_reason = SkipReason::UpstreamCancelled;
          }
        }
        task.snapshot.finished_at = now;
      } else {
        auto passes = conditions_pass(run, index);
        if (!passes) {
          return fail(passes.error());
        }
        if (*passes) {
          task.snapshot.state = TaskState::Ready;
        } else {
          task.snapshot.state = TaskState::Skipped;
          task.snapshot.skip_reason = SkipReason::ConditionFalse;
          task.snapshot.finished_at = now;
        }
      }
    }
    if (task.snapshot.state == TaskState::Ready) {
      run.ready.push_back(index);
    }
  }
  run.snapshot.tasks.clear();
  run.snapshot.tasks.reserve(run.tasks.size());
  for (const auto &task : run.tasks) {
    run.snapshot.tasks.push_back(task.snapshot);
  }
  return ok();
}

auto WorkflowRuntime::schedule_run_deadline(ActiveRun &run) -> void {
  const auto now = std::chrono::system_clock::now();
  auto started = run.snapshot.started_at;
  if (started == std::chrono::system_clock::time_point{}) {
    started = run.snapshot.created_at;
  }
  if (started == std::chrono::system_clock::time_point{}) {
    started = now;
    run.snapshot.created_at = now;
    run.snapshot.started_at = now;
  }
  const auto deadline = started + run.plan->policy.budget.max_run_duration;
  const auto run_id = run.snapshot.run_id.clone();
  const auto delay = deadline <= now
                         ? std::chrono::milliseconds::zero()
                         : std::chrono::duration_cast<
                               std::chrono::milliseconds>(deadline - now);
  run.deadline_handle = runtime_.schedule_after_on(
      runtime_.current_shard(), delay,
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = std::move(run_id)] {
        if (weak_lifetime.expired()) {
          return;
        }
        auto &owner_state = shard_states_[owner_shard(run_id)];
        if (!owner_state.active_runs.contains(run_id.str())) {
          return;
        }
        (void)request_stop(run_id, StopIntent::Fail,
                           "workflow run deadline exceeded",
                           make_execution_failure(
                               Error::Timeout, "run_deadline_exceeded",
                               "Workflow run deadline exceeded"));
      });
}

auto WorkflowRuntime::transition_run(ActiveRun &run, RunState state)
    -> Result<void> {
  const auto previous = run.snapshot.state;
  auto transitioned =
      detail::transition(run.snapshot, state, std::chrono::system_clock::now());
  if (!transitioned) {
    return transitioned;
  }
  metrics_->transition_run(previous, state);
  emit_run_state(run);
  return ok();
}

auto WorkflowRuntime::transition_task(ActiveRun &run, std::size_t task_index,
                                      TaskState state) -> Result<void> {
  if (task_index >= run.tasks.size()) {
    return fail(Error::InvalidArgument);
  }
  auto &task_runtime = run.tasks[task_index];
  auto &task = task_runtime.snapshot;
  const auto previous = task.state;
  const auto previous_ready_at = task_runtime.ready_at;
  auto transitioned =
      detail::transition(task, state, std::chrono::system_clock::now());
  if (!transitioned) {
    return transitioned;
  }
  std::optional<std::uint64_t> queue_ns;
  if (previous == TaskState::Ready && state == TaskState::Running &&
      previous_ready_at) {
    queue_ns = elapsed_ns(*previous_ready_at, task.started_at);
  }
  if (state == TaskState::Ready) {
    task_runtime.ready_at = std::chrono::system_clock::now();
  } else if (previous == TaskState::Ready) {
    task_runtime.ready_at.reset();
  }
  metrics_->transition_task(run.plan->nodes[task_index].plan.executor, previous,
                            state, task, queue_ns);
  emit_task_state(run, task_index);
  return ok();
}

auto WorkflowRuntime::transition_attempt(AttemptSnapshot &attempt,
                                         AttemptState state) -> Result<void> {
  return detail::transition(attempt, state,
                            std::chrono::system_clock::now());
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

auto WorkflowRuntime::invariants_hold(const ActiveRun &run) const -> bool {
  if (run.snapshot.tasks.size() != run.tasks.size()) {
    return false;
  }
  std::vector<TaskSnapshot> task_snapshots;
  task_snapshots.reserve(run.tasks.size());
  std::size_t active_attempts = 0;
  for (std::size_t index = 0; index < run.tasks.size(); ++index) {
    const auto &task = run.tasks[index];
    const auto &published = run.snapshot.tasks[index];
    if (published.state != task.snapshot.state ||
        published.attempt_count != task.snapshot.attempt_count ||
        published.active_attempt_id != task.snapshot.active_attempt_id) {
      return false;
    }
    if (!detail::task_snapshot_is_valid(task.snapshot)) {
      return false;
    }
    active_attempts += task.snapshot.active_attempt_id.has_value() ? 1U : 0U;
    task_snapshots.push_back(task.snapshot);
  }
  return active_attempts == run.active_attempts &&
         detail::runtime_projection_is_valid(run.snapshot, task_snapshots,
                                             run.active_attempts);
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
  append_typed_evidence(
      run, task_index, EvidenceType::AttemptStarted,
      glz::obj{"attempt_id", attempt_id, "number",
               task.snapshot.attempt_count});
  append_typed_evidence(
      run, task_index, EvidenceType::TaskStarted,
      glz::obj{"attempt", task.snapshot.attempt_count, "executor",
               run.plan->nodes[task_index].plan.executor});
  metrics_->attempt_started(run.plan->nodes[task_index].plan.executor,
                            task.snapshot.attempt_count);
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
  if (it->second.persistence_failure) {
    handle_persistence_failure(run_id);
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
    if (run.persistence_failure) {
      run.dispatching = false;
      handle_persistence_failure(run_id);
      return;
    }
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
      auto failure = make_execution_failure(
          passes.error(), "condition_evaluation_failed",
          "Workflow condition could not be evaluated");
      (void)request_stop(run_id, StopIntent::Fail, failure.message,
                         std::move(failure));
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
  if (run.persistence_failure) {
    complete_task(run_id, task_index, attempt_id,
                  task_failed(*run.persistence_failure));
    return;
  }
  active_task_coroutines_.fetch_add(1, std::memory_order_release);
  runtime_.spawn_on(owner_shard(run_id),
                    start_async_task(run_id.clone(), task_index,
                                     attempt_id.clone()));
}

auto WorkflowRuntime::start_async_task(WorkflowRunId run_id,
                                       std::size_t task_index,
                                       AttemptId attempt_id) -> spawn_task {
  const auto task_finished = dagforge::scope_exit([this] {
    active_task_coroutines_.fetch_sub(1, std::memory_order_acq_rel);
    notify_lifecycle_changed();
  });
  const auto owner = owner_shard(run_id);
  auto &state = shard_states_[owner];
  const auto run_it = state.active_runs.find(run_id.str());
  if (run_it == state.active_runs.end()) {
    co_return;
  }

  const auto &compiled_node = run_it->second.plan->nodes[task_index];
  auto node = compiled_node.plan;
  auto executor_config = compiled_node.executor_config;
  auto principal = run_it->second.trigger.principal;
  auto trace = run_it->second.trigger.trace;
  run_it->second.tasks[task_index].instance_id =
      instance_id_for(run_id, node.node_id, attempt_id);
  auto inputs = input_values(run_it->second, task_index);
  if (!inputs) {
    complete_task(
        run_id, task_index, attempt_id,
        task_failed(make_execution_failure(
            inputs.error(), "input_resolution_failed",
            "Task inputs could not be resolved")));
    co_return;
  }
  auto result = co_await execute_task(
      run_id.clone(), task_index, attempt_id.clone(), std::move(node),
      std::move(executor_config), std::move(*inputs), std::move(principal),
      std::move(trace));

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
                                    TaskExecutionResult result) -> void {
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

  auto finish_failure = [&](ExecutionFailure failure) {
    failure = retain_failure_details(std::move(failure));
    attempt->failure = failure;
    task.snapshot.failure = failure;
    (void)transition_attempt(*attempt, failure_attempt_state(failure.kind));
    metrics_->attempt_completed(run.plan->nodes[task_index].plan.executor,
                                *attempt);
    task.snapshot.active_attempt_id.reset();
    append_typed_evidence(
        run, task_index, EvidenceType::AttemptCompleted,
        glz::obj{"attempt_id", attempt_id, "state", attempt->state, "failure",
                 failure});

    const auto &plan = run.plan->nodes[task_index].plan;
    const auto delay = detail::next_retry_delay(
        plan, failure.kind, task.snapshot.attempt_count, run_id,
        plan.node_id);
    if (delay && run.snapshot.state != RunState::Stopping) {
      task.snapshot.next_attempt_at =
          std::chrono::system_clock::now() + *delay;
      (void)transition_task(run, task_index, TaskState::RetryWaiting);
      checkpoint(run);
      if (run.persistence_failure) {
        assert(invariants_hold(run));
        return;
      }
      schedule_retry(run_id, task_index);
      assert(invariants_hold(run));
      return;
    }

    const auto final_state = failure.kind == Error::Cancelled
                                 ? TaskState::Cancelled
                                 : TaskState::Failed;
    (void)transition_task(run, task_index, final_state);
    append_typed_evidence(
        run, task_index, EvidenceType::TaskFailed,
        glz::obj{"failure", failure});
    assert(invariants_hold(run));
    if (final_state == TaskState::Failed &&
        run.plan->policy.failure_policy == FailurePolicy::FailFast) {
      (void)request_stop(run_id, StopIntent::Fail, failure.message, failure);
      return;
    }
    update_dependents(run_id, task_index);
  };

  if (run.snapshot.state == RunState::Stopping) {
    attempt->termination_reason =
        run.snapshot.stop_intent == StopIntent::Cancel
            ? TerminationReason::RunCancelled
            : TerminationReason::RunFailed;
    if (attempt->state != AttemptState::Terminating) {
      (void)transition_attempt(*attempt, AttemptState::Terminating);
    }
    (void)transition_attempt(*attempt, AttemptState::Cancelled);
    metrics_->attempt_completed(run.plan->nodes[task_index].plan.executor,
                                *attempt);
    task.snapshot.active_attempt_id.reset();
    const auto cancellation = make_execution_failure(
        Error::Cancelled, "run_stopped_task_cancelled",
        run.snapshot.stop_reason.empty() ? "Task cancelled while stopping run"
                                         : run.snapshot.stop_reason);
    attempt->failure = cancellation;
    task.snapshot.failure = cancellation;
    (void)transition_task(run, task_index, TaskState::Cancelled);
    append_typed_evidence(
        run, task_index, EvidenceType::AttemptCompleted,
        glz::obj{"attempt_id", attempt_id, "state", AttemptState::Cancelled,
                 "failure", cancellation});
    assert(invariants_hold(run));
    settle_control_state(run_id);
    return;
  }

  if (!result) {
    finish_failure(std::move(result.error()));
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  if (attempt->state == AttemptState::Starting) {
    (void)transition_attempt(*attempt, AttemptState::Running);
  }

  const auto &node = run.plan->nodes[task_index].plan;
  if (!outputs_match_contract(node, *result)) {
    finish_failure(make_execution_failure(
        Error::ProtocolError, "output_contract_violation",
        "Task executor returned outputs outside the compiled contract"));
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
    auto rolled_back = run.values->erase_node(
        run.plan->nodes[task_index].plan.node_id);
    if (!rolled_back) {
      output_error = rolled_back.error();
    }
    finish_failure(make_execution_failure(
        *output_error, "output_storage_failed",
        "Task output could not be stored"));
    settle_control_state(run_id);
    dispatch(run_id);
    return;
  }

  (void)transition_attempt(*attempt, AttemptState::Succeeded);
  metrics_->attempt_completed(run.plan->nodes[task_index].plan.executor,
                              *attempt);
  task.snapshot.active_attempt_id.reset();
  task.snapshot.failure.reset();
  (void)transition_task(run, task_index, TaskState::Succeeded);
  append_typed_evidence(
      run, task_index, EvidenceType::AttemptCompleted,
      glz::obj{"attempt_id", attempt_id, "state", AttemptState::Succeeded});
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
  assert(task.snapshot.next_attempt_at.has_value());
  if (!task.snapshot.next_attempt_at) {
    return;
  }
  const auto now = std::chrono::system_clock::now();
  const auto delay = *task.snapshot.next_attempt_at > now
                         ? std::chrono::duration_cast<std::chrono::milliseconds>(
                               *task.snapshot.next_attempt_at - now)
                         : std::chrono::milliseconds::zero();
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
      auto failure = make_execution_failure(
          passes.error(), "condition_evaluation_failed",
          "Workflow condition could not be evaluated");
      (void)request_stop(run_id, StopIntent::Fail, failure.message,
                         std::move(failure));
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
      passed = workflow_value_text(**value) == edge.condition.expected_string;
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
                                   StopIntent intent, std::string reason,
                                   std::optional<ExecutionFailure> failure)
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
  if (failure) {
    run.snapshot.failure = std::move(*failure);
  } else if (intent == StopIntent::Fail) {
    run.snapshot.failure = make_execution_failure(
        Error::Unknown, "run_stop_failed", run.snapshot.stop_reason);
  } else if (intent == StopIntent::Cancel) {
    run.snapshot.failure = make_execution_failure(
        Error::Cancelled, "run_cancelled", run.snapshot.stop_reason);
  }
  auto transitioned = transition_run(run, RunState::Stopping);
  if (!transitioned) {
    return transitioned;
  }
  append_typed_evidence(
      run, run.tasks.size(), EvidenceType::RunStopRequested,
      glz::obj{"intent", intent, "reason", run.snapshot.stop_reason});
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
      task.snapshot.failure = make_execution_failure(
          Error::Cancelled, "run_stopped_task_cancelled",
          run.snapshot.stop_reason);
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
  checkpoint(run);

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
      checkpoint(run);
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
  if (run.persistence_failure) {
    run.snapshot.stop_intent = StopIntent::Fail;
    run.snapshot.stop_reason = run.persistence_failure->message;
    run.snapshot.failure = run.persistence_failure;
  } else if (any_cancelled && run.snapshot.state != RunState::Stopping) {
    run.snapshot.stop_intent = StopIntent::Cancel;
    if (run.snapshot.stop_reason.empty()) {
      const auto cancelled_task = std::ranges::find_if(
          run.tasks, [](const auto &task) {
            return task.snapshot.state == TaskState::Cancelled;
          });
      if (cancelled_task != run.tasks.end()) {
        if (cancelled_task->snapshot.failure) {
          run.snapshot.stop_reason =
              cancelled_task->snapshot.failure->message;
          run.snapshot.failure = cancelled_task->snapshot.failure;
        }
      }
    }
    if (!transition_run(run, RunState::Stopping)) {
      return false;
    }
    append_typed_evidence(
        run, run.tasks.size(), EvidenceType::RunStopRequested,
        glz::obj{"intent", StopIntent::Cancel, "reason",
                 run.snapshot.stop_reason});
  }

  RunState terminal_state = RunState::Succeeded;
  if (run.persistence_failure) {
    terminal_state = RunState::Failed;
  } else if (run.snapshot.state == RunState::Stopping) {
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
      auto message = std::format(
          "Required workflow output is missing: {}.{}",
          published.node_id, published.port);
      auto details = JsonPayload::from(
          glz::obj{"node_id", published.node_id, "port", published.port});
      run.snapshot.failure = make_execution_failure(
          Error::Incomplete, "required_output_missing", std::move(message),
          details ? std::move(*details) : JsonPayload{});
      break;
    }
  }
  if (!run.snapshot.failure && terminal_state != RunState::Succeeded) {
    const auto failed_task = std::ranges::find_if(
        run.tasks, [terminal_state](const auto &task) {
          return terminal_state == RunState::Failed
                     ? task.snapshot.state == TaskState::Failed
                     : task.snapshot.state == TaskState::Cancelled;
        });
    if (failed_task != run.tasks.end()) {
      run.snapshot.failure = failed_task->snapshot.failure;
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
  append_typed_evidence(
      run, run.tasks.size(), evidence_type,
      glz::obj{"state", run.snapshot.state, "failure",
               run.snapshot.failure});
  checkpoint(run);
  metrics_->run_completed(run.snapshot);

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
  enforce_completed_retention(state);
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

auto WorkflowRuntime::enforce_completed_retention(ShardState &state) -> void {
  while (state.completed_order.size() > max_completed_runs_) {
    const auto &expired = state.completed_order.front();
    auto erased = checkpoint_store_->erase(WorkflowRunId{expired});
    if (!erased && erased.error() != make_error_code(Error::NotFound)) {
      log::error("Failed to evict workflow checkpoint {}: {}", expired,
                 erased.error().message());
      return;
    }
    if (erased && erased->durability_deferred) {
      log::warn(
          "Workflow checkpoint {} was removed but directory durability is deferred",
          expired);
    }
    state.completed_runs.erase(expired);
    state.completed_values.erase(expired);
    {
      std::lock_guard lock(idempotency_mutex_);
      std::erase_if(idempotency_runs_, [&](const auto &entry) {
        return entry.second.run_id.str() == expired;
      });
    }
    state.completed_order.pop_front();
  }
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

auto WorkflowRuntime::append_evidence(ActiveRun &run,
                                      std::size_t node_index, EvidenceType type,
                                      JsonPayload metadata) -> void {
  if (run.persistence_failure) {
    return;
  }
  EvidenceRecord record;
  record.run_id = run.snapshot.run_id.clone();
  if (node_index < run.plan->nodes.size()) {
    record.node_id = run.plan->nodes[node_index].plan.node_id.clone();
  }
  record.type = type;
  record.actor = run.trigger.principal;
  record.metadata = std::move(metadata);
  const auto started = std::chrono::steady_clock::now();
  auto appended = evidence_ledger_->append(std::move(record));
  const auto duration = elapsed_ns(started, std::chrono::steady_clock::now());
  if (!appended) {
    metrics_->persistence_operation("evidence", "append", "failed",
                                    metric_error_type(appended.error()),
                                    duration);
    log::error("Failed to append workflow Evidence for {}: {}",
               run.snapshot.run_id, appended.error().message());
    record_persistence_failure(
        run, persistence_failure(appended.error(), "evidence_persist_failed",
                                 "Workflow Evidence could not be persisted",
                                 "evidence"));
    return;
  }
  if (appended->durability_deferred) {
    metrics_->persistence_operation(
        "evidence", "append", "deferred",
        std::string{to_string_view(Error::PersistenceError)}, duration);
    log::error(
        "Workflow Evidence {} is visible but directory durability is deferred",
        appended->evidence_id);
    record_persistence_failure(
        run, persistence_failure(make_error_code(Error::PersistenceError),
                                 "evidence_durability_deferred",
                                 "Workflow Evidence durability was not confirmed",
                                 "evidence"));
    return;
  }
  metrics_->persistence_operation("evidence", "append", "succeeded", {},
                                  duration);
}

auto WorkflowRuntime::checkpoint(ActiveRun &run) -> void {
  if (run.persistence_failure) {
    return;
  }
  const auto started = std::chrono::steady_clock::now();
  auto values = run.values->snapshot();
  if (!values) {
    metrics_->persistence_operation(
        "checkpoint", "write", "failed", metric_error_type(values.error()),
        elapsed_ns(started, std::chrono::steady_clock::now()));
    record_persistence_failure(
        run, persistence_failure(values.error(), "checkpoint_persist_failed",
                                 "Workflow checkpoint could not be persisted",
                                 "checkpoint"));
    return;
  }
  WorkflowCheckpoint checkpoint{
      .plan = source_plan(*run.plan),
      .trigger = run.trigger,
      .snapshot = *make_snapshot(run),
      .values = std::move(*values),
      .created_at = std::chrono::system_clock::now(),
  };
  auto saved = checkpoint_store_->save(std::move(checkpoint));
  const auto duration = elapsed_ns(started, std::chrono::steady_clock::now());
  if (!saved) {
    metrics_->persistence_operation("checkpoint", "write", "failed",
                                    metric_error_type(saved.error()), duration);
    record_persistence_failure(
        run, persistence_failure(saved.error(), "checkpoint_persist_failed",
                                 "Workflow checkpoint could not be persisted",
                                 "checkpoint"));
    return;
  }
  if (saved->durability_deferred) {
    metrics_->persistence_operation(
        "checkpoint", "write", "deferred",
        std::string{to_string_view(Error::PersistenceError)}, duration);
    log::warn(
        "Workflow checkpoint {} is visible but directory durability is deferred",
        run.snapshot.run_id);
    return;
  }
  metrics_->persistence_operation("checkpoint", "write", "succeeded", {},
                                  duration);
}

auto WorkflowRuntime::record_persistence_failure(ActiveRun &run,
                                                 ExecutionFailure failure)
    -> void {
  if (run.persistence_failure) {
    return;
  }
  run.persistence_failure = std::move(failure);
  if (run.snapshot.state == RunState::Stopping ||
      is_terminal(run.snapshot.state)) {
    detail::apply_persistence_failure(run.snapshot,
                                      *run.persistence_failure);
  }
  log::error("Workflow persistence failed for {}: {}",
             run.snapshot.run_id, run.persistence_failure->message);
  const auto run_id = run.snapshot.run_id.clone();
  runtime_.post_to(
      runtime_.current_shard(),
      [this, weak_lifetime = std::weak_ptr<int>(lifetime_token_),
       run_id = std::move(run_id)] {
        if (!weak_lifetime.expired()) {
          handle_persistence_failure(run_id);
        }
      });
}

auto WorkflowRuntime::handle_persistence_failure(
    const WorkflowRunId &run_id) -> void {
  const auto owner = owner_shard(run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != owner) {
    runtime_.post_to(owner, [this, run_id = run_id.clone()] {
      handle_persistence_failure(run_id);
    });
    return;
  }
  auto &state = shard_states_[owner];
  const auto it = state.active_runs.find(run_id.str());
  if (it == state.active_runs.end() || !it->second.persistence_failure ||
      is_terminal(it->second.snapshot.state)) {
    return;
  }
  auto &run = it->second;
  const auto failure = *run.persistence_failure;
  if (run.snapshot.state == RunState::Stopping) {
    run.snapshot.stop_intent = StopIntent::Fail;
    run.snapshot.stop_reason = failure.message;
    run.snapshot.failure = failure;
    return;
  }
  (void)request_stop(run_id, StopIntent::Fail, failure.message, failure);
}

auto WorkflowRuntime::retain_failure_details(ExecutionFailure failure)
    -> ExecutionFailure {
  constexpr std::size_t kInlineFailureDetailsBytes = 64 * 1024;
  const auto encoded = failure.details.encoded();
  if (encoded.size() <= kInlineFailureDetailsBytes ||
      std::ranges::any_of(failure.artifacts, [](const auto &artifact) {
        return artifact.name == "details";
      })) {
    return failure;
  }
  const auto started = std::chrono::steady_clock::now();
  auto stored = artifact_store_->put(
      std::as_bytes(std::span{encoded.data(), encoded.size()}),
      "application/json");
  const auto duration = elapsed_ns(started, std::chrono::steady_clock::now());
  if (!stored) {
    metrics_->persistence_operation("artifact", "write", "failed",
                                    metric_error_type(stored.error()),
                                    duration);
    log::error("Failed to retain oversized failure details: {}",
               stored.error().message());
    auto summary = JsonPayload::from(
        glz::obj{"externalization_failed", true, "size_bytes", encoded.size(),
                 "storage_error", stored.error().message()});
    failure.details = summary ? std::move(*summary) : JsonPayload{};
    return failure;
  }
  if (stored->durability_deferred) {
    metrics_->persistence_operation(
        "artifact", "write", "deferred",
        std::string{to_string_view(Error::PersistenceError)}, duration);
    const auto artifact_id = stored->artifact_id.clone();
    (void)artifact_store_->erase(artifact_id);
    auto summary = JsonPayload::from(
        glz::obj{"externalization_failed", true,
                 "size_bytes", encoded.size(),
                 "storage_error", "artifact durability was not confirmed"});
    failure.details = summary ? std::move(*summary) : JsonPayload{};
    return failure;
  }
  metrics_->persistence_operation("artifact", "write", "succeeded", {},
                                  duration);
  failure.artifacts.push_back(
      FailureArtifact{.name = "details",
                      .artifact = std::move(*stored).take_ref()});
  auto summary = JsonPayload::from(
      glz::obj{"externalized", true, "artifact_id", stored->artifact_id,
               "size_bytes", stored->size_bytes});
  failure.details = summary ? std::move(*summary) : JsonPayload{};
  return failure;
}

auto WorkflowRuntime::execute_task(WorkflowRunId run_id,
                                   std::size_t task_index,
                                   AttemptId attempt_id, NodePlan node,
                                   CompiledExecutorConfig executor_config,
                                   ExecutorInputs inputs, Principal principal,
                                   TraceContext trace)
    -> task<TaskExecutionResult> {
  if (node.executor.empty()) {
    co_return task_failed(make_execution_failure(
        Error::InvalidArgument, "executor_type_missing",
        "Task node does not name an executor"));
  }

  const auto instance_id = instance_id_for(run_id, node.node_id, attempt_id);
  auto result = co_await execute_task_async(
      runtime_, owner_shard(run_id), executors_, node.executor,
      TaskExecutionRequest{
          .instance_id = instance_id.clone(),
          .principal = std::move(principal),
          .trace = std::move(trace),
          .config = std::move(executor_config),
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
  co_return co_await detail::request_value_on_shard<
      std::shared_ptr<const RunSnapshot>>(
      runtime_, target, std::weak_ptr<int>(lifetime_token_),
      [this, run_id = run_id.clone(), target]()
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
      });
}

auto WorkflowRuntime::failure_report(const WorkflowRunId &run_id) const
    -> task<Result<RunFailureReport>> {
  auto current = co_await snapshot(run_id);
  if (!current) {
    co_return fail(current.error());
  }

  RunFailureReport report{
      .run_id = run_id.clone(),
      .workflow_id = (*current)->workflow_id.clone(),
      .plan_id = (*current)->plan_id.clone(),
      .state = (*current)->state,
      .parent_run_id = (*current)->parent_run_id,
      .parent_plan_id = (*current)->parent_plan_id,
      .repair_revision = (*current)->repair_revision,
      .failure = (*current)->failure,
  };
  for (const auto &task : (*current)->tasks) {
    TaskFailureReport task_report{
        .node_id = task.node_id.clone(),
        .state = task.state,
        .reused_from_run_id = task.reused_from_run_id,
        .failure = task.failure,
    };
    for (const auto &attempt : task.attempts) {
      if (!attempt.failure) {
        continue;
      }
      task_report.attempts.push_back(AttemptFailureReport{
          .attempt_id = attempt.attempt_id.clone(),
          .number = attempt.number,
          .state = attempt.state,
          .termination_reason = attempt.termination_reason,
          .failure = *attempt.failure,
      });
    }
    if (task_report.failure || !task_report.attempts.empty()) {
      report.tasks.push_back(std::move(task_report));
    }
  }
  co_return ok(std::move(report));
}

auto WorkflowRuntime::output(const WorkflowRunId &run_id,
                             const OutputRef &output_ref) const
    -> task<Result<std::shared_ptr<const WorkflowValue>>> {
  const auto target = owner_shard(run_id);
  co_return co_await detail::request_value_on_shard<
      std::shared_ptr<const WorkflowValue>>(
      runtime_, target, std::weak_ptr<int>(lifetime_token_),
      [this, run_id = run_id.clone(), output_ref, target]()
          -> Result<std::shared_ptr<const WorkflowValue>> {
        auto &state = shard_states_[target];
        if (auto active = state.active_runs.find(run_id.str());
            active != state.active_runs.end()) {
          return active->second.values->get(output_ref);
        }
        if (const auto completed =
                state.completed_values.find(run_id.str());
            completed != state.completed_values.end()) {
          const auto value = std::ranges::find(
              completed->second, output_ref, &OutputValue::output);
          if (value != completed->second.end()) {
            return ok(std::make_shared<const WorkflowValue>(value->value));
          }
        }
        return fail(Error::NotFound);
      });
}

auto WorkflowRuntime::pause(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  co_return co_await detail::request_void_on_shard(
      runtime_, target, std::weak_ptr<int>(lifetime_token_),
      [this, run_id = run_id.clone(), target]() -> Result<void> {
        auto &state = shard_states_[target];
        const auto it = state.active_runs.find(run_id.str());
        if (it == state.active_runs.end()) {
          return fail(Error::NotFound);
        }
        auto &run = it->second;
        if (run.snapshot.state == RunState::Pausing ||
            run.snapshot.state == RunState::Paused) {
          return ok();
        }
        auto transitioned = transition_run(run, RunState::Pausing);
        if (!transitioned) {
          return transitioned;
        }
        append_evidence(run, run.tasks.size(),
                        EvidenceType::RunPauseRequested);
        settle_control_state(run_id);
        const auto current = state.active_runs.find(run_id.str());
        if (current != state.active_runs.end() &&
            current->second.snapshot.state == RunState::Pausing) {
          checkpoint(current->second);
        }
        return ok();
      });
}

auto WorkflowRuntime::resume(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  co_return co_await detail::request_void_on_shard(
      runtime_, target, std::weak_ptr<int>(lifetime_token_),
      [this, run_id = run_id.clone(), target]() -> Result<void> {
        auto &state = shard_states_[target];
        const auto run_it = state.active_runs.find(run_id.str());
        if (run_it == state.active_runs.end()) {
          return fail(Error::NotFound);
        }
        auto &run = run_it->second;
        if (run.snapshot.state == RunState::Running) {
          return ok();
        }
        auto transitioned = transition_run(run, RunState::Running);
        if (!transitioned) {
          return transitioned;
        }
        append_evidence(run, run.tasks.size(), EvidenceType::RunResumed);
        checkpoint(run);
        if (run.persistence_failure) {
          return fail(Error::PersistenceError);
        }
        dispatch(run_id);
        return ok();
      });
}

auto WorkflowRuntime::cancel(const WorkflowRunId &run_id)
    -> task<Result<void>> {
  const auto target = owner_shard(run_id);
  co_return co_await detail::request_void_on_shard(
      runtime_, target, std::weak_ptr<int>(lifetime_token_),
      [this, run_id = run_id.clone()]() -> Result<void> {
        return request_stop(run_id, StopIntent::Cancel, "cancel requested");
      });
}

auto WorkflowRuntime::evidence(const WorkflowRunId &run_id) const
    -> std::vector<EvidenceRecord> {
  return evidence_ledger_->records(run_id);
}

auto WorkflowRuntime::metrics_snapshot() const -> WorkflowMetricsSnapshot {
  return metrics_->snapshot();
}

} // namespace dagforge::workflow
