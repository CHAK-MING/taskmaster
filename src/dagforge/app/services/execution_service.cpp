#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/scheduler/retry_policy.hpp"
#include "dagforge/util/hash.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/string_hash.hpp"

#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/error_code.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <experimental/scope>
#include <functional>
#include <memory_resource>
#include <ranges>
#include <span>
#include <unordered_set>

namespace dagforge {

namespace {

[[nodiscard]] auto executor_type_index(ExecutorType type) -> std::size_t {
  switch (type) {
  case ExecutorType::Shell:
    return 0;
  case ExecutorType::Docker:
    return 1;
  case ExecutorType::Sensor:
    return 2;
  case ExecutorType::Noop:
    return 3;
  case ExecutorType::Lua:
    return 4;
  }
  return 0;
}

constexpr std::size_t kCompletedRunCacheLimit = 8;
constexpr std::array<std::uint64_t, 21> kDispatchLatencyBucketsNs{
    1'000ULL,         5'000ULL,         10'000ULL,        25'000ULL,
    50'000ULL,        100'000ULL,       250'000ULL,       500'000ULL,
    1'000'000ULL,     2'500'000ULL,     5'000'000ULL,     10'000'000ULL,
    25'000'000ULL,    50'000'000ULL,    100'000'000ULL,   250'000'000ULL,
    500'000'000ULL,   1'000'000'000ULL, 2'500'000'000ULL, 5'000'000'000ULL,
    10'000'000'000ULL};
constexpr std::array<std::string_view, 9> kTaskFailureErrorTypeLabels{
    "timeout",        "dependency_blocked", "invalid_config",
    "shell_exit_126", "shell_exit_127",     "immediate_fail",
    "exit_error",     "executor_error",     "unknown",
};

[[nodiscard]] auto task_failure_error_type_index(std::string_view label)
    -> std::size_t {
  for (std::size_t i = 0; i < kTaskFailureErrorTypeLabels.size(); ++i) {
    if (kTaskFailureErrorTypeLabels[i] == label) {
      return i;
    }
  }
  return kTaskFailureErrorTypeLabels.size() - 1;
}

[[nodiscard]] auto configured_retry_policy(ExecutionCallbacks &callbacks,
                                           const DAGRunId &dag_run_id,
                                           NodeIndex idx) -> RetryPolicy {
  RetryPolicy policy{
      .max_retries = task_defaults::kMaxRetries,
      .retry_interval = task_defaults::kRetryInterval,
  };
  if (callbacks.get_max_retries) {
    policy.max_retries = callbacks.get_max_retries(dag_run_id, idx);
  }
  if (callbacks.get_retry_interval) {
    policy.retry_interval = callbacks.get_retry_interval(dag_run_id, idx);
  }
  return policy;
}

[[nodiscard]] auto effective_retry_policy(
    ExecutionCallbacks &callbacks, const TaskConfig::Compiled *task_cfg,
    const DAGRunId &dag_run_id, NodeIndex idx, std::string_view error,
    int exit_code) -> RetryPolicy {
  auto policy = configured_retry_policy(callbacks, dag_run_id, idx);
  if (task_cfg == nullptr) {
    return policy;
  }

  return resolve_retry_policy({
      .configured_policy = policy,
      .executor = task_cfg->executor,
      .failure_category = classify_task_failure_category(error, exit_code),
  });
}

class ScopedHistogramTimer {
public:
  explicit ScopedHistogramTimer(metrics::Histogram &histogram)
      : histogram_(histogram), started_at_(std::chrono::steady_clock::now()) {}

  ~ScopedHistogramTimer() {
    const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at_)
            .count();
    histogram_.observe_ns(
        static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
  }

  ScopedHistogramTimer(const ScopedHistogramTimer &) = delete;
  auto operator=(const ScopedHistogramTimer &)
      -> ScopedHistogramTimer & = delete;

private:
  metrics::Histogram &histogram_;
  std::chrono::steady_clock::time_point started_at_;
};

} // namespace

ExecutionService::ExecutionService(Runtime &runtime, IExecutor &executor)
    : runtime_(runtime), executor_(executor),
      shard_states_(runtime_.shard_count()),
      ready_run_queue_sizes_(runtime_.shard_count()),
      dispatch_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_scan_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_ready_snapshot_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_candidate_materialize_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_dependency_check_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_launch_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)),
      dispatch_spawn_duration_histogram_(
          std::span<const std::uint64_t>(kDispatchLatencyBucketsNs)) {}

ExecutionService::~ExecutionService() { lifetime_token_.reset(); }

auto ExecutionService::set_max_concurrency(int max_concurrency) -> void {
  max_concurrency_ = max_concurrency;
}

auto ExecutionService::owner_shard(const DAGRunId &dag_run_id) const noexcept
    -> shard_id {
  const auto shard_count = runtime_.shard_count();
  if (shard_count == 0) {
    return 0;
  }
  return static_cast<shard_id>(
      util::shard_of(dag_run_id.value(), shard_count));
}

auto ExecutionService::post_to_owner(const DAGRunId &dag_run_id,
                                     std::move_only_function<void()> fn)
    -> void {
  runtime_.post_to(owner_shard(dag_run_id), std::move(fn));
}

auto ExecutionService::copy_run_snapshot(const ShardState &state,
                                         const DAGRunId &dag_run_id) const
    -> Result<std::shared_ptr<const DAGRun>> {
  if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
    if (it->second.run) {
      return ok(std::make_shared<DAGRun>(*it->second.run));
    }
  }
  if (auto it = state.completed_runs.find(dag_run_id);
      it != state.completed_runs.end() && it->second) {
    return ok(it->second);
  }
  return fail(Error::NotFound);
}

auto ExecutionService::remember_completed_run(
    const DAGRunId &dag_run_id, std::shared_ptr<const DAGRun> run) -> void {
  auto &state = shard_state(dag_run_id);
  state.completed_runs.insert_or_assign(dag_run_id.clone(), std::move(run));
  state.completed_run_order.push_back(dag_run_id.clone());
  while (state.completed_run_order.size() > kCompletedRunCacheLimit) {
    auto oldest = std::move(state.completed_run_order.front());
    state.completed_run_order.pop_front();
    state.completed_runs.erase(oldest);
  }
}

auto ExecutionService::schedule_dispatch_on_owner(
    ActiveRunState &run_state, const DAGRunId &dag_run_id) -> void {
  switch (run_state.dispatch_state) {
  case DispatchState::Idle:
  case DispatchState::Queued:
    run_state.dispatch_state = DispatchState::Dispatching;
    runtime_.spawn_on(owner_shard(dag_run_id), dispatch(dag_run_id.clone()));
    return;
  case DispatchState::Dispatching:
    run_state.dispatch_state = DispatchState::DispatchingQueued;
    return;
  case DispatchState::DispatchingQueued:
    return;
  }
}

auto ExecutionService::schedule_dispatch(const DAGRunId &dag_run_id) -> void {
  const auto target = owner_shard(dag_run_id);
  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    auto &state = shard_states_[target];
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end() || !it->second.run ||
        it->second.run->is_complete()) {
      return;
    }
    if (it->second.run->ready_count() == 0) {
      return;
    }
    auto &run_state = it->second;
    const auto has_capacity =
        running_tasks_.load(std::memory_order_relaxed) < max_concurrency_;
    switch (run_state.dispatch_state) {
    case DispatchState::Idle:
      if (has_capacity) {
        schedule_dispatch_on_owner(run_state, dag_run_id);
        return;
      }
      break;
    case DispatchState::Queued:
      if (has_capacity) {
        schedule_dispatch_scan(target);
      }
      return;
    case DispatchState::Dispatching:
      schedule_dispatch_on_owner(run_state, dag_run_id);
      return;
    case DispatchState::DispatchingQueued:
      return;
    }
    if (enqueue_ready_run(state, run_state, dag_run_id)) {
      schedule_dispatch_scan(target);
    }
    return;
  }

  post_to_owner(dag_run_id, [this, id = dag_run_id.clone()]() mutable {
    auto &state = shard_state(id);
    auto it = state.runs.find(id);
    if (it == state.runs.end() || !it->second.run ||
        it->second.run->is_complete()) {
      return;
    }
    if (enqueue_ready_run(state, it->second, id)) {
      schedule_dispatch_scan(owner_shard(id));
    }
  });
}

auto ExecutionService::schedule_dispatch_scan(shard_id sid) -> void {
  if (runtime_.is_current_shard() && runtime_.current_shard() == sid) {
    auto &state = shard_states_[sid];
    if (state.dispatch_scan_state != DispatchScanState::Idle) {
      state.dispatch_scan_state = DispatchScanState::Rearmed;
      return;
    }
    state.dispatch_scan_state = DispatchScanState::Active;
    dispatch_pending_on_shard(sid);
    return;
  }

  runtime_.post_to(sid, [this, sid]() {
    auto &state = shard_states_[sid];
    if (state.dispatch_scan_state != DispatchScanState::Idle) {
      state.dispatch_scan_state = DispatchScanState::Rearmed;
      return;
    }
    state.dispatch_scan_state = DispatchScanState::Active;
    dispatch_pending_on_shard(sid);
  });
}

auto ExecutionService::enqueue_ready_run(ShardState &state,
                                         ActiveRunState &run_state,
                                         const DAGRunId &dag_run_id) -> bool {
  if (run_state.run->ready_count() == 0) {
    return false;
  }
  switch (run_state.dispatch_state) {
  case DispatchState::Idle:
    run_state.dispatch_state = DispatchState::Queued;
    state.ready_run_queue.push_back(dag_run_id.clone());
    ready_run_queue_sizes_[owner_shard(dag_run_id)].value.fetch_add(
        1, std::memory_order_relaxed);
    return true;
  case DispatchState::Queued:
  case DispatchState::DispatchingQueued:
    return false;
  case DispatchState::Dispatching:
    run_state.dispatch_state = DispatchState::DispatchingQueued;
    return false;
  }
  return false;
}

auto ExecutionService::finish_dispatch_cycle(shard_id sid,
                                             const DAGRunId &dag_run_id)
    -> void {
  auto &state = shard_states_[sid];
  auto it = state.runs.find(dag_run_id);
  if (it == state.runs.end()) {
    return;
  }

  if (it->second.dispatch_state != DispatchState::Dispatching &&
      it->second.dispatch_state != DispatchState::DispatchingQueued) {
    return;
  }

  if (it->second.run && !it->second.run->is_complete() &&
      it->second.run->ready_count() > 0) {
    it->second.dispatch_state = DispatchState::Queued;
    state.ready_run_queue.push_back(dag_run_id.clone());
    ready_run_queue_sizes_[sid].value.fetch_add(1, std::memory_order_relaxed);
    schedule_dispatch_scan(sid);
    return;
  }

  it->second.dispatch_state = DispatchState::Idle;
}

auto ExecutionService::dispatch_pending_on_shard(shard_id sid) -> void {
  const auto started_at = std::chrono::steady_clock::now();
  const auto dispatch_scan_metrics_guard =
      std::experimental::scope_exit([this, started_at] {
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        dispatch_scan_duration_histogram_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
      });

  dispatch_scan_invocations_.fetch_add(1, std::memory_order_relaxed);
  auto &state = shard_states_[sid];
  while (running_tasks_ < max_concurrency_ && !state.ready_run_queue.empty()) {
    auto dag_run_id = std::move(state.ready_run_queue.front());
    state.ready_run_queue.pop_front();
    ready_run_queue_sizes_[sid].value.fetch_sub(1, std::memory_order_relaxed);

    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) {
      continue;
    }
    if (!it->second.run || it->second.run->is_complete()) {
      it->second.dispatch_state = DispatchState::Idle;
      continue;
    }
    schedule_dispatch_on_owner(it->second, dag_run_id);
  }

  const auto rearm = state.dispatch_scan_state == DispatchScanState::Rearmed;
  state.dispatch_scan_state = DispatchScanState::Idle;
  if (rearm) {
    schedule_dispatch_scan(sid);
  }
}

auto ExecutionService::dispatch_pending() -> void {
  // Called on owner shard — only dispatch local runs
  if (runtime_.is_current_shard()) {
    dispatch_pending_on_shard(runtime_.current_shard());
  }
}

auto ExecutionService::notify_capacity_available() -> void {
  const auto shard_count = runtime_.shard_count();
  if (shard_count == 0) {
    return;
  }

  const auto start =
      notify_rr_.fetch_add(1, std::memory_order_relaxed) % shard_count;
  for (unsigned offset = 0; offset < shard_count; ++offset) {
    const auto sid = static_cast<shard_id>((start + offset) % shard_count);
    if (ready_run_queue_sizes_[sid].value.load(std::memory_order_relaxed) ==
        0) {
      continue;
    }
    schedule_dispatch_scan(sid);
    break;
  }
}

auto ExecutionService::set_callbacks(ExecutionCallbacks callbacks) -> void {
  callbacks_ = std::move(callbacks);
}

auto ExecutionService::set_local_task_lease_timeout(
    std::chrono::milliseconds timeout) -> void {
  local_task_lease_timeout_ =
      std::max(timeout, std::chrono::milliseconds::zero());
}

auto ExecutionService::start_run(const DAGRunId &dag_run_id, RunContext ctx)
    -> void {
  ++active_run_count_;
  post_to_owner(dag_run_id, [this, id = dag_run_id.clone(),
                             ctx = std::move(ctx)]() mutable {
    auto &state = shard_state(id);
    auto it = state.runs.insert_or_assign(
        id.clone(),
        ActiveRunState{.run = std::move(ctx.run),
                       .executor_configs = std::move(ctx.executor_configs),
                       .task_configs = std::move(ctx.task_configs),
                       .dag_id = std::move(ctx.dag_id)})
                  .first;
    if (!it->second.run || it->second.run->is_complete()) {
      return;
    }

    // Fast path for root-task release: avoid queue+scan if we can dispatch now.
    if (it->second.run->ready_count() > 0 &&
        running_tasks_.load(std::memory_order_relaxed) < max_concurrency_) {
      schedule_dispatch_on_owner(it->second, id);
      return;
    }

    if (enqueue_ready_run(state, it->second, id)) {
      schedule_dispatch_scan(owner_shard(id));
    }
  });
}

auto ExecutionService::get_cached_dag_id(const DAGRunId &dag_run_id) const
    -> std::optional<DAGId> {
  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    return std::nullopt;
  }

  const auto &state = shard_states_[target];
  if (auto it = state.runs.find(dag_run_id); it != state.runs.end()) {
    if (it->second.dag_id) {
      return it->second.dag_id->clone();
    }
  }
  return std::nullopt;
}

auto ExecutionService::get_run_snapshot(const DAGRunId &dag_run_id) const
    -> task<Result<std::shared_ptr<const DAGRun>>> {
  const auto target = owner_shard(dag_run_id);
  if (runtime_.is_current_shard() && runtime_.current_shard() == target) {
    co_return copy_run_snapshot(shard_states_[target], dag_run_id);
  }

  auto [ec, snapshot] = co_await boost::asio::async_initiate<
      const decltype(dagforge::use_nothrow),
      void(boost::system::error_code, std::shared_ptr<const DAGRun>)>(
      [this, dag_run_id = dag_run_id.clone(), target](auto handler) mutable {
        runtime_.post_to(
            target, [this, dag_run_id = std::move(dag_run_id), target,
                     handler = std::move(handler)]() mutable {
              auto result =
                  copy_run_snapshot(shard_states_[target], dag_run_id);
              if (!result) {
                handler(result.error(), std::shared_ptr<const DAGRun>{});
                return;
              }
              handler(boost::system::error_code{}, std::move(*result));
            });
      },
      dagforge::use_nothrow);

  if (ec) {
    co_return fail(ec);
  }
  co_return ok(std::move(snapshot));
}

auto ExecutionService::has_active_runs() const -> bool {
  return active_run_count_.load(std::memory_order_acquire) > 0;
}

auto ExecutionService::wait_for_completion_async(int timeout_ms)
    -> task<Result<void>> {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (has_active_runs() && std::chrono::steady_clock::now() < deadline) {
    co_await async_sleep(std::chrono::milliseconds(50));
  }
  if (has_active_runs()) {
    co_return fail(Error::Timeout);
  }
  co_return ok();
}

auto ExecutionService::coro_count() const -> int { return coro_count_.load(); }

auto ExecutionService::emit_log_chunk(const DAGRunId &dag_run_id,
                                      const TaskId &task_id, int attempt,
                                      std::string_view stream,
                                      std::string_view msg) -> void {
  if (callbacks_.on_log) {
    callbacks_.on_log(dag_run_id, task_id, attempt, stream, msg);
  }
}

auto ExecutionService::on_executor_heartbeat(const DAGRunId &dag_run_id,
                                             const InstanceId &instance_id)
    -> void {
  if (auto key = refresh_local_task_lease(dag_run_id, instance_id);
      key && callbacks_.on_task_heartbeat) {
    callbacks_.on_task_heartbeat(*key);
  }
}

auto ExecutionService::begin_task_execution(const DAGRunId &dag_run_id,
                                            const TaskJob &job)
    -> std::chrono::steady_clock::time_point {
  const auto task_started_at = std::chrono::steady_clock::now();
  const auto &task_id = job.task_id();
  const auto &cfg = job.cfg();
  task_runs_total_.fetch_add(1, std::memory_order_relaxed);
  executor_start_totals_[executor_type_index(cfg.type())].fetch_add(
      1, std::memory_order_relaxed);
  executor_active_counts_[executor_type_index(cfg.type())].fetch_add(
      1, std::memory_order_relaxed);

  log::debug("run_task: starting {} (idx={}, attempt={})", task_id, job.idx,
             job.attempt);
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(dag_run_id, task_id, TaskState::Running);
  }

  return task_started_at;
}

auto ExecutionService::finalize_task_execution(
    const DAGRunId &dag_run_id, TaskJob &job, ExecutorResult result,
    std::chrono::steady_clock::time_point task_started_at, shard_id target)
    -> task<void> {
  if (!consume_local_task_lease(dag_run_id, job.inst_id)) {
    log::debug("run_task: ignoring late completion for dag_run_id={} task={} "
               "inst_id={}",
               dag_run_id, job.task_id(), job.inst_id);
    co_return;
  }

  const auto &task_id = job.task_id();
  const auto &cfg = job.cfg();
  running_tasks_--;
  log::debug(
      "run_task: completed dag_run_id={} task={} inst_id={} exit_code={} "
      "err='{}'",
      dag_run_id, task_id, job.inst_id, result.exit_code, result.error);

  const auto task_finished_at = std::chrono::steady_clock::now();
  const auto task_duration_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(task_finished_at -
                                                           task_started_at)
          .count());
  task_duration_histogram_.observe_ns(task_duration_ns);
  executor_active_counts_[executor_type_index(cfg.type())].fetch_sub(
      1, std::memory_order_relaxed);

  if (!result.stdout_output.empty() && !result.stdout_streamed) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, task_id, job.attempt, "stdout",
                        result.stdout_output);
    }
  }
  if (!result.stderr_output.empty() && !result.stderr_streamed) {
    if (callbacks_.on_log) {
      callbacks_.on_log(dag_run_id, task_id, job.attempt, "stderr",
                        result.stderr_output);
    }
  }

  if (result.exit_code == 0 && result.error.empty()) {
    task_successes_total_.fetch_add(1, std::memory_order_relaxed);
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, task_id, TaskState::Success);
    }
    log::debug("run_task: calling on_task_success for {} (idx={})", task_id,
               job.idx);
    if (auto r = co_await on_task_success(dag_run_id, job.idx); !r) {
      log::error("run_task: on_task_success failed: {}", r.error().message());
    }
    co_return;
  }

  if (result.exit_code == kExitCodeSkip) {
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, task_id, TaskState::Skipped);
    }
    task_skipped_total_.fetch_add(1, std::memory_order_relaxed);
    on_task_skipped(dag_run_id, job.idx).or_else([](std::error_code ec) {
      log::error("run_task: on_task_skipped failed: {}", ec.message());
      return ok();
    });
    co_return;
  }

  if (result.exit_code == kExitCodeImmediateFail) {
    if (callbacks_.on_task_status) {
      callbacks_.on_task_status(dag_run_id, task_id, TaskState::Failed);
    }
    task_failures_total_.fetch_add(1, std::memory_order_relaxed);
    on_task_fail_immediately(dag_run_id, job.idx, result.error,
                             result.exit_code)
        .or_else([](std::error_code ec) {
          log::error("run_task: on_task_fail_immediately failed: {}",
                     ec.message());
          return ok();
        });
    co_return;
  }

  if (callbacks_.on_log) {
    auto error_msg =
        std::format("Task '{}' failed: {}", task_id.value(), result.error);
    callbacks_.on_log(dag_run_id, task_id, job.attempt, "stderr", error_msg);
  }
  auto retry_result =
      on_task_failure(dag_run_id, job.idx, result.error, result.exit_code,
                      result.timed_out)
          .or_else([](std::error_code ec) {
            log::error("run_task: on_task_failure failed: {}", ec.message());
            return ok(false);
          });
  const bool will_retry = *retry_result;
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(dag_run_id, task_id,
                              will_retry ? TaskState::Retrying
                                         : TaskState::Failed);
  }
  task_failures_total_.fetch_add(1, std::memory_order_relaxed);
  if (will_retry) {
    const auto retry_interval =
        effective_retry_policy(callbacks_, job.task_config, dag_run_id, job.idx,
                               result.error, result.exit_code)
            .retry_interval;
    auto retry = dispatch_after_delay(dag_run_id, job.idx, retry_interval);
    runtime_.spawn_on(target, std::move(retry));
  }
}

auto ExecutionService::run_noop_task_inline(const DAGRunId &dag_run_id,
                                            TaskJob &job) -> task<void> {
  const auto task_started_at = begin_task_execution(dag_run_id, job);
  auto *noop = job.cfg().as<NoopExecutorConfig>();
  auto *resource = current_memory_resource_or_default();
  ExecutorResult result = make_executor_result(resource);
  if (noop == nullptr) {
    result.exit_code = 1;
    result.error =
        pmr::string("Invalid configuration for noop executor", resource);
  } else {
    result.exit_code = noop->exit_code;
  }
  co_await finalize_task_execution(dag_run_id, job, std::move(result),
                                   task_started_at, owner_shard(dag_run_id));
}

auto ExecutionService::dispatch_invocations() const -> std::uint64_t {
  return dispatch_invocations_.load(std::memory_order_relaxed);
}

auto ExecutionService::dispatch_scan_invocations() const -> std::uint64_t {
  return dispatch_scan_invocations_.load(std::memory_order_relaxed);
}

auto ExecutionService::dispatch_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_scan_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_scan_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_ready_snapshot_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_ready_snapshot_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_candidate_materialize_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_candidate_materialize_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_dependency_check_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_dependency_check_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_launch_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_launch_duration_histogram_.snapshot();
}

auto ExecutionService::dispatch_spawn_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return dispatch_spawn_duration_histogram_.snapshot();
}

auto ExecutionService::task_runs_total() const -> std::uint64_t {
  return task_runs_total_.load(std::memory_order_relaxed);
}

auto ExecutionService::task_successes_total() const -> std::uint64_t {
  return task_successes_total_.load(std::memory_order_relaxed);
}

auto ExecutionService::task_failures_total() const -> std::uint64_t {
  return task_failures_total_.load(std::memory_order_relaxed);
}

auto ExecutionService::task_failure_error_type_totals() const
    -> std::array<std::uint64_t, 9> {
  std::array<std::uint64_t, 9> out{};
  for (std::size_t i = 0; i < out.size(); ++i) {
    out[i] = task_failure_error_type_totals_[i].load(std::memory_order_relaxed);
  }
  return out;
}

auto ExecutionService::task_skipped_total() const -> std::uint64_t {
  return task_skipped_total_.load(std::memory_order_relaxed);
}

auto ExecutionService::queue_depth_total() const -> std::size_t {
  std::size_t total = 0;
  for (const auto &counter : ready_run_queue_sizes_) {
    total += counter.value.load(std::memory_order_relaxed);
  }
  return total;
}

auto ExecutionService::executor_active_count(ExecutorType type) const
    -> std::uint64_t {
  return executor_active_counts_[executor_type_index(type)].load(
      std::memory_order_relaxed);
}

auto ExecutionService::executor_start_total(ExecutorType type) const
    -> std::uint64_t {
  return executor_start_totals_[executor_type_index(type)].load(
      std::memory_order_relaxed);
}

auto ExecutionService::task_duration_histogram() const
    -> metrics::Histogram::Snapshot {
  return task_duration_histogram_.snapshot();
}

auto ExecutionService::record_task_failure_error_type(
    std::string_view error_type) -> void {
  const auto idx = task_failure_error_type_index(error_type);
  task_failure_error_type_totals_[idx].fetch_add(1, std::memory_order_relaxed);
}

auto ExecutionService::maybe_persist_task(const DAGRunId &dag_run_id,
                                          const TaskInstanceInfo &info)
    -> void {
  if (callbacks_.on_persist_task) {
    callbacks_.on_persist_task(dag_run_id, info);
  }
}

auto ExecutionService::maybe_persist_tasks(
    const DAGRunId &dag_run_id, std::span<const TaskInstanceInfo> infos)
    -> void {
  if (!callbacks_.on_persist_task) {
    return;
  }
  for (const auto &info : infos) {
    callbacks_.on_persist_task(dag_run_id, info);
  }
}

auto ExecutionService::local_task_lease_enabled() const noexcept -> bool {
  return local_task_lease_timeout_ > std::chrono::milliseconds::zero();
}

auto ExecutionService::arm_local_task_lease(
    const DAGRunId &dag_run_id, const TaskJob &job,
    std::chrono::steady_clock::time_point task_started_at) -> void {
  if (!local_task_lease_enabled()) {
    return;
  }

  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    post_to_owner(dag_run_id, [this, id = dag_run_id.clone(), job,
                               task_started_at]() mutable {
      arm_local_task_lease(id, job, task_started_at);
    });
    return;
  }

  auto &state = shard_states_[target];
  auto key = std::string(job.inst_id.value());
  if (auto existing = state.local_task_leases.find(key);
      existing != state.local_task_leases.end()) {
    if (existing->second.handle.valid()) {
      runtime_.cancel_after_on(target, existing->second.handle);
    }
    state.local_task_leases.erase(existing);
  }

  ShardState::ActiveLocalTaskLease lease{
      .dag_run_id = dag_run_id.clone(),
      .task_id = job.task_id().clone(),
      .instance_id = job.inst_id.clone(),
      .task_instance_key = job.task_instance_key,
      .idx = job.idx,
      .executor_type = job.cfg().type(),
      .attempt = job.attempt,
      .task_started_at = task_started_at,
  };
  lease.handle = runtime_.schedule_after_on(
      target, local_task_lease_timeout_,
      [this, id = dag_run_id.clone(), instance_id = job.inst_id.clone()]() {
        expire_local_task_lease(id, instance_id);
      });
  state.local_task_leases.emplace(std::move(key), std::move(lease));
}

auto ExecutionService::refresh_local_task_lease(const DAGRunId &dag_run_id,
                                                const InstanceId &instance_id)
    -> std::optional<TaskInstanceKey> {
  if (!local_task_lease_enabled()) {
    return std::nullopt;
  }

  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    post_to_owner(dag_run_id, [this, id = dag_run_id.clone(),
                               instance_id = instance_id.clone()]() mutable {
      (void)refresh_local_task_lease(id, instance_id);
    });
    return std::nullopt;
  }

  auto &state = shard_states_[target];
  auto it = state.local_task_leases.find(std::string(instance_id.value()));
  if (it == state.local_task_leases.end()) {
    return std::nullopt;
  }

  if (it->second.handle.valid()) {
    runtime_.cancel_after_on(target, it->second.handle);
  }
  it->second.handle = runtime_.schedule_after_on(
      target, local_task_lease_timeout_,
      [this, id = dag_run_id.clone(), instance_id = instance_id.clone()]() {
        expire_local_task_lease(id, instance_id);
      });
  return it->second.task_instance_key;
}

auto ExecutionService::consume_local_task_lease(const DAGRunId &dag_run_id,
                                                const InstanceId &instance_id)
    -> bool {
  if (!local_task_lease_enabled()) {
    return true;
  }

  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    return false;
  }

  auto &state = shard_states_[target];
  auto it = state.local_task_leases.find(std::string(instance_id.value()));
  if (it == state.local_task_leases.end()) {
    return false;
  }

  if (it->second.handle.valid()) {
    runtime_.cancel_after_on(target, it->second.handle);
  }
  state.local_task_leases.erase(it);
  return true;
}

auto ExecutionService::expire_local_task_lease(const DAGRunId &dag_run_id,
                                               const InstanceId &instance_id)
    -> void {
  if (!local_task_lease_enabled()) {
    return;
  }

  const auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    post_to_owner(dag_run_id, [this, id = dag_run_id.clone(),
                               instance_id = instance_id.clone()]() mutable {
      expire_local_task_lease(id, instance_id);
    });
    return;
  }

  auto &state = shard_states_[target];
  auto it = state.local_task_leases.find(std::string(instance_id.value()));
  if (it == state.local_task_leases.end()) {
    return;
  }

  auto lease = std::move(it->second);
  state.local_task_leases.erase(it);

  running_tasks_.fetch_sub(1, std::memory_order_relaxed);
  const auto task_duration_ns = static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - lease.task_started_at)
          .count());
  task_duration_histogram_.observe_ns(task_duration_ns);
  executor_active_counts_[executor_type_index(lease.executor_type)].fetch_sub(
      1, std::memory_order_relaxed);

  executor_.cancel(lease.instance_id);

  constexpr std::string_view kLeaseTimeoutMessage =
      "Local worker heartbeat timeout";
  if (callbacks_.on_log) {
    callbacks_.on_log(dag_run_id, lease.task_id, lease.attempt, "stderr",
                      kLeaseTimeoutMessage);
  }

  auto retry_result =
      on_task_failure(dag_run_id, lease.idx, kLeaseTimeoutMessage,
                      kExitCodeTimeout, true)
          .or_else([](std::error_code ec) {
            log::error("expire_local_task_lease: on_task_failure failed: {}",
                       ec.message());
            return ok(false);
          });
  const bool will_retry = *retry_result;
  if (callbacks_.on_task_status) {
    callbacks_.on_task_status(dag_run_id, lease.task_id,
                              will_retry ? TaskState::Retrying
                                         : TaskState::Failed);
  }
  task_failures_total_.fetch_add(1, std::memory_order_relaxed);
  log::warn("Local lease expired for dag_run_id={} task={} inst_id={}",
            dag_run_id, lease.task_id, lease.instance_id);
}

auto ExecutionService::dispatch(DAGRunId dag_run_id) -> task<Result<void>> {
  const auto target = owner_shard(dag_run_id);
  auto lifetime = std::weak_ptr<int>{lifetime_token_};

  // If not on owner shard, schedule a coroutine on the owner and return.
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    schedule_dispatch(dag_run_id);
    co_return ok();
  }

  // --- We are on the owner shard; direct access, no locks ---
  const auto started_at = std::chrono::steady_clock::now();
  const auto dispatch_metrics_guard =
      std::experimental::scope_exit([lifetime, this, started_at] {
        if (lifetime.expired()) {
          return;
        }
        const auto elapsed_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at)
                .count();
        dispatch_duration_histogram_.observe_ns(
            static_cast<std::uint64_t>(elapsed_ns > 0 ? elapsed_ns : 0));
      });

  dispatch_invocations_.fetch_add(1, std::memory_order_relaxed);
  const auto dispatch_cycle_guard =
      std::experimental::scope_exit([lifetime, this, target,
                                     dag_run_id = dag_run_id.clone()] {
        if (!lifetime.expired()) {
          finish_dispatch_cycle(target, dag_run_id);
        }
      });

  auto &state = shard_states_[target];

  struct ReadyCandidate {
    NodeIndex idx{kInvalidNode};
    std::int64_t task_rowid{0};
    int attempt{1};
    bool depends_on_past{false};
    std::chrono::system_clock::time_point execution_date;
  };

  std::array<std::byte, 8192> dispatch_storage{};
  pmr::monotonic_buffer_resource dispatch_resource(
      dispatch_storage.data(), dispatch_storage.size(),
      current_memory_resource_or_default());

  pmr::vector<TaskJob> jobs{&dispatch_resource};
  pmr::vector<NodeIndex> depends_on_past_blocked{&dispatch_resource};
  pmr::vector<ReadyCandidate> candidates{&dispatch_resource};
  pmr::vector<TaskInstanceInfo> blocked_to_persist{&dispatch_resource};
  pmr::vector<std::pair<TaskId, TaskState>> blocked_status_updates{
      &dispatch_resource};
  std::shared_ptr<const DAGRun> completed_snapshot;
  std::shared_ptr<const std::vector<ExecutorConfig>> executor_configs_snapshot;
  std::shared_ptr<const std::vector<TaskConfig::Compiled>>
      task_configs_snapshot;
  bool completed{false};

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) [[unlikely]] {
      log::debug("dispatch: run {} not found", dag_run_id);
      co_return ok();
    }

    auto &run_state = it->second;
    if (!run_state.run) [[unlikely]] {
      log::debug("dispatch: run state for {} is empty", dag_run_id);
      co_return ok();
    }

    auto &run = *run_state.run;
    if (!run_state.executor_configs || !run_state.task_configs) [[unlikely]] {
      log::error("dispatch: missing configs for run {}", dag_run_id);
      co_return fail(Error::InvalidState);
    }
    executor_configs_snapshot = run_state.executor_configs;
    task_configs_snapshot = run_state.task_configs;
    const auto &executor_configs = *executor_configs_snapshot;
    const auto &task_configs = *task_configs_snapshot;

    {
      ScopedHistogramTimer ready_snapshot_timer{
          dispatch_ready_snapshot_duration_histogram_};
      ScopedHistogramTimer candidate_materialize_timer{
          dispatch_candidate_materialize_duration_histogram_};
      pmr::vector<NodeIndex> ready_indices{&dispatch_resource};
      run.copy_ready_tasks(ready_indices);
      for (const auto idx : ready_indices) {
        if (static_cast<int>(candidates.size()) + running_tasks_.load() >=
            max_concurrency_) {
          log::debug("Max concurrency reached ({}/{})", running_tasks_.load(),
                     max_concurrency_);
          break;
        }

        if (idx >= executor_configs.size()) [[unlikely]] {
          log::error("Config not found for task {}", idx);
          continue;
        }

        bool depends_on_past = false;
        if (idx < task_configs.size()) {
          depends_on_past = task_configs[idx].depends_on_past;
        }

        int attempt = 1;
        if (auto info = run.get_task_info(idx)) {
          attempt = info->attempt + 1;
        }

        candidates.emplace_back(
            ReadyCandidate{.idx = idx,
                           .task_rowid = task_configs[idx].task_rowid,
                           .attempt = attempt,
                           .depends_on_past = depends_on_past,
                           .execution_date = run.execution_date()});
      }
    }
  }

  {
    ScopedHistogramTimer dependency_check_timer{
        dispatch_dependency_check_duration_histogram_};
    for (const auto &candidate : candidates) {
      if (!candidate.depends_on_past) {
        continue;
      }
      if (!callbacks_.check_previous_task_state) {
        log::error("dispatch: task {} has depends_on_past but no callback "
                   "configured",
                   candidate.idx);
        depends_on_past_blocked.emplace_back(candidate.idx);
        continue;
      }

      if (candidate.task_rowid <= 0) {
        log::error("dispatch: task {} has depends_on_past but unresolved "
                   "task_rowid",
                   candidate.idx);
        depends_on_past_blocked.emplace_back(candidate.idx);
        continue;
      }

      auto state_res = co_await callbacks_.check_previous_task_state(
          candidate.task_rowid, candidate.execution_date, dag_run_id);
      auto blocked_res =
          state_res
              .and_then([&](auto state_val) -> Result<bool> {
                if (state_val != TaskState::Success &&
                    state_val != TaskState::Skipped) {
                  log::debug("dispatch: task {} blocked by depends_on_past "
                             "(previous state: {})",
                             candidate.idx, to_string_view(state_val));
                  return ok(true);
                }
                return ok(false);
              })
              .or_else([&](std::error_code ec) -> Result<bool> {
                if (ec == make_error_code(Error::NotFound)) {
                  return ok(false);
                }
                log::error("dispatch: task {} blocked by depends_on_past "
                           "(persistence error: {})",
                           candidate.idx, ec.message());
                return ok(true);
              });
      const bool blocked = *blocked_res;
      if (blocked) {
        depends_on_past_blocked.emplace_back(candidate.idx);
      }
    }
  }

  {
    ScopedHistogramTimer launch_timer{dispatch_launch_duration_histogram_};

    // Reacquire after the depends_on_past callback loop above. That loop may
    // suspend, so iterators/references into state.runs cannot be carried over.
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end()) [[unlikely]] {
      co_return ok();
    }
    auto &run_state = it->second;
    auto &run = *run_state.run;
    const auto &executor_configs = *executor_configs_snapshot;
    const auto &task_configs = *task_configs_snapshot;
    pmr::unordered_set<NodeIndex> blocked_set(&dispatch_resource);
    blocked_set.reserve(depends_on_past_blocked.size());
    for (const auto idx : depends_on_past_blocked) {
      blocked_set.insert(idx);
    }
    const auto dispatch_started_at = std::chrono::system_clock::now();

    for (auto &candidate : candidates | std::views::take_while([&](auto &) {
                             return running_tasks_ < max_concurrency_;
                           }) | std::views::filter([&](auto &cand) {
                             return run.is_task_ready(cand.idx) &&
                                    !blocked_set.contains(cand.idx);
                           })) {

      TaskId task_id = run.dag().get_key(candidate.idx);
      auto inst_id = generate_instance_id(dag_run_id, task_id);
      if (auto r = run.mark_task_started(candidate.idx, inst_id,
                                         dispatch_started_at);
          !r) {
        log::error("dispatch: failed to mark task {} started: {}",
                   candidate.idx, r.error().message());
        co_return r;
      }

      jobs.emplace_back(TaskJob{
          .idx = candidate.idx,
          .inst_id = std::move(inst_id),
          .task_config = &task_configs[candidate.idx],
          .executor_config = &executor_configs[candidate.idx],
          .task_instance_key =
              TaskInstanceKey{.run_rowid = run.run_rowid(),
                              .task_rowid =
                                  task_configs[candidate.idx].task_rowid,
                              .attempt = candidate.attempt},
          .attempt = candidate.attempt,
          .execution_date = run.execution_date(),
          .data_interval_start = run.data_interval_start(),
          .data_interval_end = run.data_interval_end(),
      });
      running_tasks_++;
      const auto &scheduled = jobs.back();
      log::debug("dispatch: scheduled task {} (idx={}, attempt={})",
                 scheduled.task_id(), scheduled.idx, scheduled.attempt);
    }

    for (NodeIndex idx : depends_on_past_blocked) {
      if (!run.is_task_ready(idx)) {
        continue;
      }
      if (idx >= executor_configs.size()) [[unlikely]] {
        log::error("dispatch: blocked task {} missing executor config", idx);
        continue;
      }

      TaskId task_id = run.dag().get_key(idx);
      auto inst_id = generate_instance_id(dag_run_id, task_id);
      if (auto r = run.mark_task_started(idx, inst_id, dispatch_started_at);
          !r) {
        log::error("dispatch: failed to mark blocked task {} started: {}", idx,
                   r.error().message());
        co_return r;
      }
      auto delta = run.mark_task_failed(idx, "depends_on_past blocked", 0,
                                        kExitCodeImmediateFail);
      if (!delta) {
        log::error("dispatch: failed to fail blocked task {}: {}", idx,
                   delta.error().message());
        co_return fail(delta.error());
      }
      if (auto info = run.get_task_info(idx); info) {
        blocked_to_persist.emplace_back(*info);
        blocked_status_updates.emplace_back(task_id, info->state);
        record_task_failure_error_type(info->error_type);
      }
      for (NodeIndex terminal_idx : delta->terminal_tasks) {
        auto info = run.get_task_info(terminal_idx);
        if (!info) {
          continue;
        }
        blocked_to_persist.emplace_back(*info);
        if (info->state == TaskState::Skipped ||
            info->state == TaskState::UpstreamFailed) {
          blocked_status_updates.emplace_back(run.dag().get_key(info->task_idx),
                                              info->state);
        }
      }
      task_failures_total_.fetch_add(1, std::memory_order_relaxed);
      if (run.is_complete()) {
        completed_snapshot = std::make_shared<DAGRun>(run);
        state.runs.erase(it);
        completed = true;
        break;
      }
    }
  }

  maybe_persist_tasks(dag_run_id, blocked_to_persist);
  if (!blocked_status_updates.empty() && callbacks_.on_task_status) {
    for (const auto &[task_id, task_state] : blocked_status_updates) {
      callbacks_.on_task_status(dag_run_id, task_id, task_state);
    }
  }
  if (completed_snapshot) {
    remember_completed_run(dag_run_id, completed_snapshot);
    on_run_complete(std::move(completed_snapshot), dag_run_id)
        .or_else([](std::error_code ec) {
          log::error("dispatch: on_run_complete failed: {}", ec.message());
          return ok();
        });
  }
  if (completed) {
    --active_run_count_;
  }

  pmr::vector<TaskJob> inline_noop_jobs(&dispatch_resource);
  pmr::vector<TaskJob> async_jobs(&dispatch_resource);
  inline_noop_jobs.reserve(jobs.size());
  async_jobs.reserve(jobs.size());
  for (auto &job : jobs) {
    if (job.cfg().type() == ExecutorType::Noop) {
      inline_noop_jobs.emplace_back(std::move(job));
    } else {
      async_jobs.emplace_back(std::move(job));
    }
  }

  {
    ScopedHistogramTimer spawn_timer{dispatch_spawn_duration_histogram_};

    for (auto &job : async_jobs) {
      log::debug("Executing task {} (idx={}, inst_id={})", job.task_id(),
                 job.idx, job.inst_id);
      auto coro = run_task(dag_run_id, std::move(job));
      runtime_.spawn_on(target, std::move(coro));
    }
  }

  for (auto &job : inline_noop_jobs) {
    co_await run_noop_task_inline(dag_run_id, job);
  }

  co_return ok();
}

namespace {

// dag_id is pre-fetched asynchronously by run_task before invoking the visitor,
// so all methods here are synchronous — no DB calls or async lookups needed.
struct ExecutionVisitor {
  ExecutionService &service;
  const DAGRunId &dag_run_id;
  ExecutionService::TaskJob &job;
  std::shared_ptr<pmr::memory_resource> task_resource_owner;
  pmr::memory_resource *task_resource;

  auto operator()() const -> task<Result<ExecutorResult>> {
    switch (job.cfg().type()) {
    case ExecutorType::Shell:
      return execute_shell();
    case ExecutorType::Docker:
      return execute_docker();
    case ExecutorType::Sensor:
      return execute_sensor();
    case ExecutorType::Noop:
      return execute_noop();
    case ExecutorType::Lua:
      return invalid_config_task("lua");
    }
    return invalid_config_task("unknown");
  }

private:
  auto refresh_heartbeat() const -> void {
    service.on_executor_heartbeat(dag_run_id, job.inst_id);
  }

  auto execute_shell() const -> task<Result<ExecutorResult>> {
    if (job.cfg().as<ShellExecutorConfig>() == nullptr) {
      return invalid_config_task("shell");
    }
    return execute_async(
        service.runtime(), service.executor(),
        ExecutorRequest{.instance_id = job.inst_id.clone(),
                        .command = job.task_config->command,
                        .working_dir = job.task_config->working_dir,
                        .execution_timeout = job.task_config->execution_timeout,
                        .config = job.cfg(),
                        .memory_resource = {}},
        task_resource_owner,
        [this](std::string_view chunk) {
          service.emit_log_chunk(dag_run_id, job.task_id(), job.attempt,
                                 "stdout", chunk);
        },
        [this](std::string_view chunk) {
          service.emit_log_chunk(dag_run_id, job.task_id(), job.attempt,
                                 "stderr", chunk);
        },
        [this](const InstanceId &) { refresh_heartbeat(); });
  }

  auto execute_docker() const -> task<Result<ExecutorResult>> {
    auto *cfg = job.cfg().as<DockerExecutorConfig>();
    if (cfg == nullptr) {
      return invalid_config_task("docker");
    }
    return execute_async(
        service.runtime(), service.executor(),
        ExecutorRequest{.instance_id = job.inst_id.clone(),
                        .command = job.task_config->command,
                        .working_dir = job.task_config->working_dir,
                        .execution_timeout = job.task_config->execution_timeout,
                        .config = job.cfg(),
                        .memory_resource = {}},
        task_resource_owner, {}, {},
        [this](const InstanceId &) { refresh_heartbeat(); });
  }

  auto execute_sensor() const -> task<Result<ExecutorResult>> {
    auto *cfg = job.cfg().as<SensorExecutorConfig>();
    if (cfg == nullptr) {
      return invalid_config_task("sensor");
    }
    return execute_async(
        service.runtime(), service.executor(),
        ExecutorRequest{.instance_id = job.inst_id.clone(),
                        .command = job.task_config->command,
                        .working_dir = job.task_config->working_dir,
                        .execution_timeout = job.task_config->execution_timeout,
                        .config = job.cfg(),
                        .memory_resource = {}},
        task_resource_owner, {}, {},
        [this](const InstanceId &) { refresh_heartbeat(); });
  }

  auto execute_noop() const -> task<Result<ExecutorResult>> {
    auto *cfg = job.cfg().as<NoopExecutorConfig>();
    if (cfg == nullptr) {
      return invalid_config_task("noop");
    }
    ExecutorResult result = make_executor_result(task_resource);
    result.exit_code = cfg->exit_code;
    return ready_result(std::move(result));
  }

  [[nodiscard]] auto invalid_config(std::string_view executor_name) const
      -> ExecutorResult {
    ExecutorResult result = make_executor_result(task_resource);
    result.exit_code = 1;
    result.error = pmr::string(
        std::format("Invalid configuration for {} executor", executor_name),
        task_resource);
    return result;
  }

  auto ready_result(ExecutorResult result) const
      -> task<Result<ExecutorResult>> {
    co_return ok(std::move(result));
  }

  auto invalid_config_task(std::string executor_name) const
      -> task<Result<ExecutorResult>> {
    co_return ok(invalid_config(executor_name));
  }
};

} // namespace

auto ExecutionService::run_task(DAGRunId dag_run_id, TaskJob job)
    -> spawn_task {
  auto lifetime = std::weak_ptr<int>{lifetime_token_};
  ++coro_count_;
  const auto coro_count_guard = std::experimental::scope_exit([lifetime, this] {
    if (!lifetime.expired()) {
      --coro_count_;
    }
  });

  const auto task_started_at = begin_task_execution(dag_run_id, job);

  const auto target = owner_shard(dag_run_id);
  arm_local_task_lease(dag_run_id, job, task_started_at);
  auto task_resource = std::make_shared<pmr::monotonic_buffer_resource>(
      current_memory_resource_or_default());
  auto result_res =
      co_await ExecutionVisitor{.service = *this,
                                .dag_run_id = dag_run_id,
                                .job = job,
                                .task_resource_owner = task_resource,
                                .task_resource = task_resource.get()}();
  ExecutorResult result = make_executor_result(task_resource.get());
  if (!result_res) {
    result.exit_code = 1;
    result.error = pmr::string(result_res.error().message(),
                               task_resource.get());
  } else {
    result = std::move(*result_res);
  }
  co_await finalize_task_execution(dag_run_id, job, std::move(result),
                                   task_started_at, target);
}

auto ExecutionService::dispatch_after_delay(DAGRunId dag_run_id,
                                            NodeIndex retry_idx,
                                            std::chrono::seconds delay)
    -> spawn_task {
  co_await async_sleep_on_timing_wheel(delay);
  auto target = owner_shard(dag_run_id);
  if (!runtime_.is_current_shard() || runtime_.current_shard() != target) {
    auto repost = dispatch_after_delay(std::move(dag_run_id), retry_idx,
                                       std::chrono::seconds{0});
    runtime_.spawn_on(target, std::move(repost));
    co_return;
  }

  auto &state = shard_states_[target];
  if (auto it = state.runs.find(dag_run_id); it == state.runs.end()) {
    co_return;
  } else if (auto delta = it->second.run->mark_task_retry_ready(retry_idx);
             !delta) {
    log::debug("dispatch_after_delay: skip retry arm for {} idx={} ({})",
               dag_run_id, retry_idx, delta.error().message());
    co_return;
  } else if (!delta->has_ready_tasks()) {
    co_return;
  }

  schedule_dispatch(dag_run_id);
}

auto ExecutionService::dispatch_after_yield(DAGRunId dag_run_id) -> spawn_task {
  auto executor = co_await boost::asio::this_coro::executor;
  auto post_res =
      co_await co_as_result(boost::asio::post(executor, dagforge::use_nothrow));
  if (!post_res) {
    log::error("dispatch_after_yield: post failed: {}",
               post_res.error().message());
    co_return;
  }
  if (auto result = co_await dispatch(std::move(dag_run_id)); !result) {
    log::error("dispatch_after_yield: dispatch failed: {}",
               result.error().message());
  }
}

auto ExecutionService::record_task_snapshot(const DAGRun &run, NodeIndex idx,
                                            TransitionEffects &effects) const
    -> void {
  if (auto info = run.get_task_info(idx); info) {
    effects.persisted_task_info = *info;
  }
}

auto ExecutionService::collect_transition_delta(
    const DAGRun &run, const DAGRun::TransitionDelta &delta,
    TransitionEffects &effects) const -> void {
  effects.ready_tasks_available =
      effects.ready_tasks_available || delta.has_ready_tasks();
  for (NodeIndex idx : delta.terminal_tasks) {
    auto info = run.get_task_info(idx);
    if (!info) {
      continue;
    }
    effects.persisted_infos.emplace_back(*info);
    if (info->state == TaskState::Skipped ||
        info->state == TaskState::UpstreamFailed) {
      effects.status_updates.emplace_back(run.dag().get_key(info->task_idx),
                                          info->state);
    }
  }
}

auto ExecutionService::complete_transition_if_needed(ShardState &state,
                                                     RunMap::iterator it,
                                                     TransitionEffects &effects)
    -> void {
  if (!it->second.run->is_complete()) {
    return;
  }
  effects.completed_snapshot = std::make_shared<DAGRun>(*it->second.run);
  state.runs.erase(it);
}

auto ExecutionService::on_task_success(const DAGRunId &dag_run_id,
                                       NodeIndex idx) -> task<Result<void>> {
  TransitionEffects effects;
  auto &state = shard_state(dag_run_id);
  log::debug("on_task_success: dag_run_id={} idx={}", dag_run_id, idx);

  auto it = state.runs.find(dag_run_id);
  if (it == state.runs.end()) {
    co_return ok();
  }

  auto &run = *it->second.run;
  auto delta = run.mark_task_completed(idx, 0);
  if (!delta) {
    log::error("on_task_success: failed to mark task {}: {}", idx,
               delta.error().message());
    co_return fail(delta.error());
  }

  record_task_snapshot(run, idx, effects);
  collect_transition_delta(run, *delta, effects);
  complete_transition_if_needed(state, it, effects);
  finalize_task_transition(dag_run_id, std::move(effects));
  co_return ok();
}

auto ExecutionService::on_task_failure(const DAGRunId &dag_run_id,
                                       NodeIndex idx, std::string_view error,
                                       int exit_code, bool timed_out)
    -> Result<bool> {
  (void)timed_out;

  log::debug("on_task_failure: dag_run_id={} idx={} err='{}'", dag_run_id, idx,
             error);

  bool needs_retry = false;
  TransitionEffects effects;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok(false);

    auto &run = *it->second.run;
    DAGRun::TransitionDelta delta;

    const TaskConfig::Compiled *task_cfg = nullptr;
    if (it->second.task_configs &&
        static_cast<std::size_t>(idx) < it->second.task_configs->size()) {
      task_cfg = &(*it->second.task_configs)[idx];
    }
    const auto retry_policy = effective_retry_policy(
        callbacks_, task_cfg, dag_run_id, idx, error, exit_code);

    auto delta_result =
        run.mark_task_failed(idx, error, retry_policy.max_retries, exit_code)
            .or_else(
                [&](std::error_code ec) -> Result<DAGRun::TransitionDelta> {
                  log::error("on_task_failure: failed to mark task {}: {}", idx,
                             ec.message());
                  return fail(ec);
                });
    if (!delta_result) {
      return fail(delta_result.error());
    }
    delta = std::move(*delta_result);

    if (auto info = run.get_task_info(idx)) {
      needs_retry = (info->state == TaskState::Retrying);
    }

    record_task_snapshot(run, idx, effects);
    if (auto info = run.get_task_info(idx); info) {
      record_task_failure_error_type(info->error_type);
    }
    collect_transition_delta(run, delta, effects);
    complete_transition_if_needed(state, it, effects);

    if (effects.completed_snapshot) {
      needs_retry = false;
    }
  }

  const bool run_complete = static_cast<bool>(effects.completed_snapshot);
  finalize_task_transition(dag_run_id, std::move(effects));
  return ok(run_complete ? false : needs_retry);
}

auto ExecutionService::on_run_complete(std::shared_ptr<const DAGRun> run,
                                       const DAGRunId &dag_run_id)
    -> Result<void> {
  const auto run_state = run->state();
  if (callbacks_.on_run_completed) {
    callbacks_.on_run_completed(dag_run_id, *run);
  }
  if (callbacks_.on_persist_run) {
    callbacks_.on_persist_run(std::move(run));
  }
  if (callbacks_.on_run_status) {
    callbacks_.on_run_status(dag_run_id, run_state);
  }
  log::info("DAG run {} {}", dag_run_id, to_string_view(run_state));
  return ok();
}

auto ExecutionService::on_task_skipped(const DAGRunId &dag_run_id,
                                       NodeIndex idx) -> Result<void> {
  log::debug("on_task_skipped: dag_run_id={} idx={}", dag_run_id, idx);

  TransitionEffects effects;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok();

    auto &run = *it->second.run;
    auto delta = run.mark_task_skipped(idx).or_else(
        [&](std::error_code ec) -> Result<DAGRun::TransitionDelta> {
          log::error("on_task_skipped: failed to mark task {}: {}", idx,
                     ec.message());
          return fail(ec);
        });
    if (!delta) {
      return fail(delta.error());
    }

    record_task_snapshot(run, idx, effects);
    collect_transition_delta(run, *delta, effects);
    complete_transition_if_needed(state, it, effects);
  }

  finalize_task_transition(dag_run_id, std::move(effects));

  return ok();
}

auto ExecutionService::on_task_fail_immediately(const DAGRunId &dag_run_id,
                                                NodeIndex idx,
                                                std::string_view error,
                                                int exit_code) -> Result<void> {

  log::debug("on_task_fail_immediately: dag_run_id={} idx={} err='{}'",
             dag_run_id, idx, error);

  TransitionEffects effects;
  auto &state = shard_state(dag_run_id);

  {
    auto it = state.runs.find(dag_run_id);
    if (it == state.runs.end())
      return ok();

    auto &run = *it->second.run;
    auto delta =
        run.mark_task_failed(idx, error, 0, exit_code)
            .or_else(
                [&](std::error_code ec) -> Result<DAGRun::TransitionDelta> {
                  log::error(
                      "on_task_fail_immediately: failed to mark task {}: {}",
                      idx, ec.message());
                  return fail(ec);
                });
    if (!delta) {
      return fail(delta.error());
    }

    record_task_snapshot(run, idx, effects);
    if (auto info = run.get_task_info(idx); info) {
      record_task_failure_error_type(info->error_type);
    }
    collect_transition_delta(run, *delta, effects);
    complete_transition_if_needed(state, it, effects);
  }

  finalize_task_transition(dag_run_id, std::move(effects));

  return ok();
}

auto ExecutionService::finalize_task_transition(const DAGRunId &dag_run_id,
                                                TransitionEffects effects)
    -> void {
  if (effects.persisted_task_info) {
    maybe_persist_task(dag_run_id, *effects.persisted_task_info);
  }
  if (!effects.persisted_infos.empty()) {
    maybe_persist_tasks(dag_run_id, effects.persisted_infos);
  }
  if (!effects.status_updates.empty() && callbacks_.on_task_status) {
    for (const auto &[task_id, task_state] : effects.status_updates) {
      if (!task_id.value().empty()) {
        callbacks_.on_task_status(dag_run_id, task_id, task_state);
      }
    }
  }
  if (effects.completed_snapshot) {
    remember_completed_run(dag_run_id, effects.completed_snapshot);
    on_run_complete(std::move(effects.completed_snapshot), dag_run_id)
        .or_else([](std::error_code ec) {
          log::error("finalize_task_transition: on_run_complete failed: {}",
                     ec.message());
          return ok();
        });
    --active_run_count_;
  } else if (effects.ready_tasks_available) {
    schedule_dispatch(dag_run_id);
  }
  notify_capacity_available();
}

} // namespace dagforge
