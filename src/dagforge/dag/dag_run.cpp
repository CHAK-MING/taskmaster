#include "dagforge/dag/dag_run.hpp"
#include "dagforge/executor/executor_types.hpp"
#include "dagforge/scheduler/retry_policy.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <ranges>
#include <span>

namespace dagforge {

struct DAGRunDependencyState {
  explicit DAGRunDependencyState(std::size_t n)
      : in_degree(n, 0), terminal_dep_count(n, 0), success_dep_count(n, 0),
        failed_dep_count(n, 0), skipped_dep_count(n, 0),
        upstream_failed_dep_count(n, 0) {}

  std::vector<int> in_degree;
  std::vector<int> terminal_dep_count;
  std::vector<int> success_dep_count;
  std::vector<int> failed_dep_count;
  std::vector<int> skipped_dep_count;
  std::vector<int> upstream_failed_dep_count;
};

struct DAGRunRuntimeState {
  struct ReadyLink {
    NodeIndex prev{kInvalidNode};
    NodeIndex next{kInvalidNode};
  };

  explicit DAGRunRuntimeState(std::size_t n) : ready_links(n) {}

  DAGRunRuntimeState(const DAGRunRuntimeState &other)
      : ready_links(other.ready_links), ready_head(other.ready_head),
        ready_tail(other.ready_tail), pending_count(other.pending_count),
        retrying_count(other.retrying_count), ready_count(other.ready_count),
        running_count(other.running_count),
        completed_count(other.completed_count),
        failed_count(other.failed_count), skipped_count(other.skipped_count),
        failed_count_fast(
            other.failed_count_fast.load(std::memory_order_relaxed)) {}

  auto operator=(const DAGRunRuntimeState &other) -> DAGRunRuntimeState & {
    if (this != &other) {
      ready_links = other.ready_links;
      ready_head = other.ready_head;
      ready_tail = other.ready_tail;
      pending_count = other.pending_count;
      retrying_count = other.retrying_count;
      ready_count = other.ready_count;
      running_count = other.running_count;
      completed_count = other.completed_count;
      failed_count = other.failed_count;
      skipped_count = other.skipped_count;
      failed_count_fast.store(
          other.failed_count_fast.load(std::memory_order_relaxed),
          std::memory_order_relaxed);
    }
    return *this;
  }

  auto terminal_count() const noexcept -> std::size_t {
    return completed_count + failed_count + skipped_count;
  }

  std::vector<ReadyLink> ready_links;
  NodeIndex ready_head{kInvalidNode};
  NodeIndex ready_tail{kInvalidNode};

  std::size_t pending_count{0};
  std::size_t retrying_count{0};
  std::size_t ready_count{0};
  std::size_t running_count{0};
  std::size_t completed_count{0};
  std::size_t failed_count{0};
  std::size_t skipped_count{0};
  std::atomic<std::size_t> failed_count_fast{0};
};

struct DAGRun::Impl {
  Impl(DAGRunId dag_run_id, std::shared_ptr<const DAG> dag)
      : dag_run_id_(std::move(dag_run_id)), dag_(std::move(dag)),
        deps_(dag_->size()), runtime_(dag_->size()),
        trigger_rules_(dag_->size()) {
    std::size_t n = dag_->size();
    task_info_.resize(n);

    for (auto i : std::views::iota(0u, n)) {
      NodeIndex idx = static_cast<NodeIndex>(i);
      task_info_[i].task_idx = idx;
      task_info_[i].state = TaskState::Pending;
      deps_.in_degree[i] = static_cast<int>(dag_->get_deps_view(idx).size());
      trigger_rules_[i] = dag_->get_trigger_rule(idx);
    }

    runtime_.pending_count = n;
    init_ready_set();
  }

  Impl(const Impl &other)
      : dag_run_id_(other.dag_run_id_), dag_(other.dag_), state_(other.state_),
        scheduled_at_(other.scheduled_at_), started_at_(other.started_at_),
        finished_at_(other.finished_at_),
        execution_date_(other.execution_date_),
        data_interval_start_(other.data_interval_start_),
        data_interval_end_(other.data_interval_end_),
        trigger_type_(other.trigger_type_), deps_(other.deps_),
        runtime_(other.runtime_), trigger_rules_(other.trigger_rules_),
        task_info_(other.task_info_),
        run_rowid_(other.run_rowid_), dag_rowid_(other.dag_rowid_),
        dag_version_(other.dag_version_) {}

  auto reset_ready_list() -> void {
    runtime_.ready_head = kInvalidNode;
    runtime_.ready_tail = kInvalidNode;
    for (auto &link : runtime_.ready_links) {
      link.prev = kInvalidNode;
      link.next = kInvalidNode;
    }
  }

  auto link_ready_back(NodeIndex idx) -> void {
    auto &link = runtime_.ready_links[idx];
    link.prev = runtime_.ready_tail;
    link.next = kInvalidNode;
    if (runtime_.ready_tail != kInvalidNode) {
      runtime_.ready_links[runtime_.ready_tail].next = idx;
    } else {
      runtime_.ready_head = idx;
    }
    runtime_.ready_tail = idx;
  }

  auto unlink_ready(NodeIndex idx) -> void {
    auto &link = runtime_.ready_links[idx];
    if (link.prev != kInvalidNode) {
      runtime_.ready_links[link.prev].next = link.next;
    } else {
      runtime_.ready_head = link.next;
    }
    if (link.next != kInvalidNode) {
      runtime_.ready_links[link.next].prev = link.prev;
    } else {
      runtime_.ready_tail = link.prev;
    }
    link.prev = kInvalidNode;
    link.next = kInvalidNode;
  }

  auto transition_to_ready(NodeIndex idx,
                           DAGRun::TransitionDelta *delta = nullptr) -> void {
    auto &info = task_info_[idx];
    if (info.state == TaskState::Ready) {
      return;
    }

    if (info.state == TaskState::Pending) {
      --runtime_.pending_count;
    } else if (info.state == TaskState::Retrying) {
      --runtime_.retrying_count;
    }

    info.state = TaskState::Ready;
    link_ready_back(idx);
    ++runtime_.ready_count;
    if (delta != nullptr) {
      delta->ready_tasks.emplace_back(idx);
    }
  }

  auto update_state() -> void {
    if (runtime_.pending_count > 0 || runtime_.retrying_count > 0 ||
        runtime_.ready_count > 0 || runtime_.running_count > 0) {
      return;
    }

    if (runtime_.terminal_count() == dag_->size()) {
      state_ = (runtime_.failed_count > 0) ? DAGRunState::Failed
                                           : DAGRunState::Success;
      finished_at_ = std::chrono::system_clock::now();
    }
#ifndef NDEBUG
    verify_invariants();
#endif
  }

  auto init_ready_set() -> void {
    runtime_.ready_count = 0;
    reset_ready_list();

    for (auto [i, deg] : std::views::enumerate(deps_.in_degree)) {
      const auto task_index = static_cast<std::size_t>(i);
      if (deg == 0 || trigger_rules_[task_index] == TriggerRule::Always) {
        // Airflow-compatible: ALWAYS does not wait for upstream completion.
        transition_to_ready(static_cast<NodeIndex>(task_index));
      }
    }
#ifndef NDEBUG
    verify_invariants();
#endif
  }

#ifndef NDEBUG
  auto verify_invariants() const noexcept -> void {
    const auto n = dag_->size();
    const auto terminal_count = runtime_.terminal_count();
    std::size_t pending_count = 0;
    std::size_t retrying_count = 0;
    std::size_t ready_state_count = 0;
    std::size_t running_count = 0;
    std::size_t completed_count = 0;
    std::size_t failed_count = 0;
    std::size_t skipped_count = 0;

    for (const auto &info : task_info_) {
      switch (info.state) {
      case TaskState::Pending:
        ++pending_count;
        break;
      case TaskState::Retrying:
        ++retrying_count;
        break;
      case TaskState::Ready:
        ++ready_state_count;
        break;
      case TaskState::Running:
        ++running_count;
        break;
      case TaskState::Success:
        ++completed_count;
        break;
      case TaskState::Failed:
        ++failed_count;
        break;
      case TaskState::Skipped:
      case TaskState::UpstreamFailed:
        ++skipped_count;
        break;
      }
    }

    std::vector<std::uint8_t> seen(task_info_.size(), 0);
    std::size_t ready_list_size = 0;
    for (NodeIndex idx = runtime_.ready_head; idx != kInvalidNode;
         idx = runtime_.ready_links[idx].next) {
      assert(static_cast<std::size_t>(idx) < task_info_.size());
      assert(task_info_[idx].state == TaskState::Ready);
      assert(!seen[idx]);
      seen[idx] = 1;
      ++ready_list_size;
    }

    assert((runtime_.ready_head == kInvalidNode) ==
           (runtime_.ready_tail == kInvalidNode));
    assert(runtime_.pending_count == pending_count);
    assert(runtime_.retrying_count == retrying_count);
    assert(runtime_.ready_count == ready_state_count);
    assert(runtime_.ready_count == ready_list_size);
    assert(runtime_.running_count == running_count);
    assert(runtime_.completed_count == completed_count);
    assert(runtime_.failed_count == failed_count);
    assert(runtime_.skipped_count == skipped_count);
    assert(terminal_count <= n);
    assert(runtime_.pending_count + runtime_.retrying_count +
               runtime_.ready_count + runtime_.running_count + terminal_count ==
           n);
  }
#endif

  auto update_dependent_counters(NodeIndex terminal_task, TaskState state,
                                 int delta) -> void {
    for (NodeIndex dep : dag_->get_dependents_view(terminal_task)) {
      deps_.terminal_dep_count[dep] += delta;
      switch (state) {
      case TaskState::Success:
        deps_.success_dep_count[dep] += delta;
        break;
      case TaskState::Failed:
        deps_.failed_dep_count[dep] += delta;
        break;
      case TaskState::UpstreamFailed:
        deps_.upstream_failed_dep_count[dep] += delta;
        [[fallthrough]];
      case TaskState::Skipped:
        deps_.skipped_dep_count[dep] += delta;
        break;
      default:
        break;
      }
    }
  }

  enum class TriggerStatus : std::uint8_t {
    Ready,
    Waiting,
    Skipped,
    UpstreamFailed,
  };

  auto evaluate_trigger_rule(NodeIndex task) const -> TriggerStatus {
    const int total = deps_.in_degree[task];
    if (total == 0)
      return TriggerStatus::Ready;

    TriggerRule rule = trigger_rules_[task];
    const int success = deps_.success_dep_count[task];
    const int failed = deps_.failed_dep_count[task];
    const int upstream_failed = deps_.upstream_failed_dep_count[task];
    const int any_failed = failed + upstream_failed;
    const int pure_skipped = deps_.skipped_dep_count[task] - upstream_failed;
    const int terminal = deps_.terminal_dep_count[task];
    const bool is_all_done = (terminal == total);

    switch (rule) {
    case TriggerRule::AllSuccess:
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (pure_skipped > 0)
        return TriggerStatus::Skipped;
      return is_all_done ? TriggerStatus::Ready : TriggerStatus::Waiting;
    case TriggerRule::AllFailed:
      if (any_failed == total)
        return TriggerStatus::Ready;
      if (!is_all_done)
        return TriggerStatus::Waiting;
      return any_failed > 0 ? TriggerStatus::UpstreamFailed
                            : TriggerStatus::Skipped;
    case TriggerRule::AllDone:
      return is_all_done ? TriggerStatus::Ready : TriggerStatus::Waiting;
    case TriggerRule::OneFailed:
      if (any_failed > 0)
        return TriggerStatus::Ready;
      return is_all_done ? TriggerStatus::Skipped : TriggerStatus::Waiting;
    case TriggerRule::OneSuccess:
      if (success > 0)
        return TriggerStatus::Ready;
      if (!is_all_done)
        return TriggerStatus::Waiting;
      return pure_skipped == total ? TriggerStatus::Skipped
                                   : TriggerStatus::UpstreamFailed;
    case TriggerRule::NoneFailed:
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      return is_all_done ? TriggerStatus::Ready : TriggerStatus::Waiting;
    case TriggerRule::Always:
      return TriggerStatus::Ready;
    case TriggerRule::NoneSkipped:
      if (pure_skipped > 0)
        return TriggerStatus::Skipped;
      return is_all_done ? TriggerStatus::Ready : TriggerStatus::Waiting;
    case TriggerRule::AllDoneMinOneSuccess:
      if (!is_all_done)
        return TriggerStatus::Waiting;
      if (success > 0)
        return TriggerStatus::Ready;
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (pure_skipped > 0)
        return TriggerStatus::Skipped;
      return TriggerStatus::UpstreamFailed;
    case TriggerRule::AllSkipped:
      if (pure_skipped == total)
        return TriggerStatus::Ready;
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (success > 0)
        return TriggerStatus::Skipped;
      return TriggerStatus::Waiting;
    case TriggerRule::OneDone:
      if (success > 0 || any_failed > 0)
        return TriggerStatus::Ready;
      return is_all_done ? TriggerStatus::Skipped : TriggerStatus::Waiting;
    case TriggerRule::NoneFailedMinOneSuccess:
      if (any_failed > 0)
        return TriggerStatus::UpstreamFailed;
      if (!is_all_done)
        return TriggerStatus::Waiting;
      return (success > 0) ? TriggerStatus::Ready : TriggerStatus::Skipped;
    }
    return TriggerStatus::Waiting;
  }

  auto
  propagate_terminal_to_downstream(NodeIndex terminal_task,
                                   DAGRun::TransitionDelta *delta = nullptr)
      -> void {
    for (NodeIndex dep : dag_->get_dependents_view(terminal_task)) {
      if (task_info_[dep].state != TaskState::Pending) {
        continue;
      }

      auto status = evaluate_trigger_rule(dep);
      if (status == TriggerStatus::Ready) {
        transition_to_ready(dep, delta);
      } else if (status == TriggerStatus::UpstreamFailed) {
        [[maybe_unused]] auto transition_res =
            mark_task_upstream_failed(dep, delta);
        assert(transition_res.has_value());
      } else if (status == TriggerStatus::Skipped) {
        [[maybe_unused]] auto transition_res =
            mark_task_skipped_internal(dep, delta);
        assert(transition_res.has_value());
      }
    }
  }

  auto mark_task_upstream_failed(NodeIndex idx, DAGRun::TransitionDelta *delta)
      -> Result<void> {
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    auto &info = task_info_[idx];
    switch (info.state) {
    case TaskState::Pending:
      --runtime_.pending_count;
      break;
    case TaskState::Ready:
      unlink_ready(idx);
      --runtime_.ready_count;
      break;
    case TaskState::Running:
    case TaskState::Retrying:
    case TaskState::Success:
    case TaskState::Failed:
    case TaskState::Skipped:
    case TaskState::UpstreamFailed:
      return ok();
    }

    ++runtime_.skipped_count;
    info.state = TaskState::UpstreamFailed;
    if (delta != nullptr) {
      delta->terminal_tasks.emplace_back(idx);
    }
    update_dependent_counters(idx, TaskState::UpstreamFailed, +1);

    propagate_terminal_to_downstream(idx, delta);
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  auto mark_task_skipped_internal(NodeIndex idx, DAGRun::TransitionDelta *delta)
      -> Result<void> {
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    auto &info = task_info_[idx];
    switch (info.state) {
    case TaskState::Ready:
      unlink_ready(idx);
      --runtime_.ready_count;
      break;
    case TaskState::Pending:
      --runtime_.pending_count;
      break;
    case TaskState::Running:
    case TaskState::Retrying:
    case TaskState::Success:
    case TaskState::Failed:
    case TaskState::Skipped:
    case TaskState::UpstreamFailed:
      return ok();
    }

    ++runtime_.skipped_count;
    info.state = TaskState::Skipped;
    if (delta != nullptr) {
      delta->terminal_tasks.emplace_back(idx);
    }
    update_dependent_counters(idx, TaskState::Skipped, +1);

    propagate_terminal_to_downstream(idx, delta);
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  auto rebuild_runtime_state_from_task_info() -> void {
    reset_ready_list();

    std::ranges::fill(deps_.terminal_dep_count, 0);
    std::ranges::fill(deps_.success_dep_count, 0);
    std::ranges::fill(deps_.failed_dep_count, 0);
    std::ranges::fill(deps_.skipped_dep_count, 0);
    std::ranges::fill(deps_.upstream_failed_dep_count, 0);

    runtime_.pending_count = 0;
    runtime_.retrying_count = 0;
    runtime_.ready_count = 0;
    runtime_.running_count = 0;
    runtime_.completed_count = 0;
    runtime_.failed_count = 0;
    runtime_.skipped_count = 0;

    for (std::size_t i = 0; i < task_info_.size(); ++i) {
      const auto idx = static_cast<NodeIndex>(i);
      const auto state = task_info_[i].state;
      switch (state) {
      case TaskState::Pending:
        ++runtime_.pending_count;
        break;
      case TaskState::Retrying:
        ++runtime_.retrying_count;
        break;
      case TaskState::Ready:
        link_ready_back(idx);
        ++runtime_.ready_count;
        break;
      case TaskState::Running:
        ++runtime_.running_count;
        break;
      case TaskState::Success:
        ++runtime_.completed_count;
        update_dependent_counters(idx, TaskState::Success, +1);
        break;
      case TaskState::Failed:
        ++runtime_.failed_count;
        update_dependent_counters(idx, TaskState::Failed, +1);
        break;
      case TaskState::Skipped:
        ++runtime_.skipped_count;
        update_dependent_counters(idx, TaskState::Skipped, +1);
        break;
      case TaskState::UpstreamFailed:
        ++runtime_.skipped_count;
        update_dependent_counters(idx, TaskState::UpstreamFailed, +1);
        break;
      }
    }
    runtime_.failed_count_fast.store(runtime_.failed_count,
                                     std::memory_order_relaxed);

    auto try_resolve_pending = [&](NodeIndex idx,
                                   std::vector<NodeIndex> &terminal_queue) {
      auto &info = task_info_[idx];
      if (info.state != TaskState::Pending) {
        return;
      }

      const auto status = evaluate_trigger_rule(idx);
      if (status == TriggerStatus::Waiting) {
        return;
      }
      if (status == TriggerStatus::Ready) {
        transition_to_ready(idx);
        return;
      }

      info.state = (status == TriggerStatus::UpstreamFailed)
                       ? TaskState::UpstreamFailed
                       : TaskState::Skipped;
      if (info.finished_at == std::chrono::system_clock::time_point{}) {
        info.finished_at = std::chrono::system_clock::now();
      }
      ++runtime_.skipped_count;
      --runtime_.pending_count;
      update_dependent_counters(idx, info.state, +1);
      terminal_queue.push_back(idx);
    };

    std::vector<NodeIndex> terminal_queue;
    terminal_queue.reserve(task_info_.size());
    for (std::size_t i = 0; i < task_info_.size(); ++i) {
      try_resolve_pending(static_cast<NodeIndex>(i), terminal_queue);
    }

    for (std::size_t head = 0; head < terminal_queue.size(); ++head) {
      for (NodeIndex dep : dag_->get_dependents_view(terminal_queue[head])) {
        try_resolve_pending(dep, terminal_queue);
      }
    }
  }

  auto restore_task_instance(const TaskInstanceInfo &info) -> Result<void> {
    NodeIndex idx = info.task_idx;
    if (static_cast<std::size_t>(idx) >= task_info_.size())
      return fail(Error::NotFound);

    task_info_[idx] = info;
    rebuild_runtime_state_from_task_info();
    update_state();
#ifndef NDEBUG
    verify_invariants();
#endif
    return ok();
  }

  DAGRunId dag_run_id_;
  std::shared_ptr<const DAG> dag_;
  DAGRunState state_{DAGRunState::Running};

  std::chrono::system_clock::time_point scheduled_at_;
  std::chrono::system_clock::time_point started_at_;
  std::chrono::system_clock::time_point finished_at_;
  std::chrono::system_clock::time_point execution_date_;
  std::chrono::system_clock::time_point data_interval_start_;
  std::chrono::system_clock::time_point data_interval_end_;
  TriggerType trigger_type_{TriggerType::Manual};

  DAGRunDependencyState deps_;
  DAGRunRuntimeState runtime_;
  std::vector<TriggerRule> trigger_rules_;
  std::vector<TaskInstanceInfo> task_info_;
  int64_t run_rowid_{-1};
  int64_t dag_rowid_{-1};
  int dag_version_{0};
};

DAGRun::DAGRun(DAGRunPrivateTag, DAGRunId dag_run_id,
               std::shared_ptr<const DAG> dag)
    : impl_(std::make_unique<Impl>(std::move(dag_run_id), std::move(dag))) {}

DAGRun::~DAGRun() = default;

DAGRun::DAGRun(DAGRun &&) noexcept = default;
DAGRun &DAGRun::operator=(DAGRun &&) noexcept = default;

DAGRun::DAGRun(const DAGRun &other)
    : impl_([&] { return std::make_unique<Impl>(*other.impl_); }()) {}

DAGRun &DAGRun::operator=(const DAGRun &other) {
  if (this != &other) {
    impl_ = std::make_unique<Impl>(*other.impl_);
  }
  return *this;
}

auto DAGRun::create(DAGRunId dag_run_id, std::shared_ptr<const DAG> dag)
    -> Result<DAGRun> {
  if (!dag)
    return fail(Error::InvalidArgument);
  return DAGRun(DAGRunPrivateTag{}, std::move(dag_run_id), std::move(dag));
}

auto DAGRun::id() const noexcept -> const DAGRunId & {
  return impl_->dag_run_id_;
}
auto DAGRun::state() const noexcept -> DAGRunState { return impl_->state_; }
auto DAGRun::dag() const noexcept -> const DAG & { return *impl_->dag_; }

auto DAGRun::get_ready_tasks(pmr::memory_resource *resource) const
    -> pmr::vector<NodeIndex> {
  pmr::vector<NodeIndex> result(resource);
  copy_ready_tasks(result);
  return result;
}

auto DAGRun::copy_ready_tasks(pmr::vector<NodeIndex> &out) const -> void {
  out.clear();
  if (impl_->runtime_.ready_count == 0) {
    return;
  }

  out.reserve(impl_->runtime_.ready_count);
  for (NodeIndex idx = impl_->runtime_.ready_head; idx != kInvalidNode;
       idx = impl_->runtime_.ready_links[idx].next) {
    out.emplace_back(idx);
  }
}

auto DAGRun::is_task_ready(NodeIndex task_idx) const noexcept -> bool {
  return static_cast<std::size_t>(task_idx) < impl_->task_info_.size() &&
         impl_->task_info_[task_idx].state == TaskState::Ready;
}

auto DAGRun::ready_count() const noexcept -> size_t {
  return impl_->runtime_.ready_count;
}

auto DAGRun::mark_task_started(NodeIndex task_idx,
                               const InstanceId &instance_id) -> Result<void> {
  return mark_task_started(task_idx, instance_id,
                           std::chrono::system_clock::now());
}

auto DAGRun::mark_task_started(NodeIndex task_idx,
                               const InstanceId &instance_id,
                               std::chrono::system_clock::time_point started_at)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (impl_->task_info_[task_idx].state != TaskState::Ready)
    return fail(Error::InvalidState);

  impl_->unlink_ready(task_idx);

  --impl_->runtime_.ready_count;
  ++impl_->runtime_.running_count;

  auto &info = impl_->task_info_[task_idx];
  info.instance_id = instance_id;
  info.state = TaskState::Running;
  info.started_at = started_at;
  info.finished_at = {};
  info.exit_code = 0;
  info.error_message.clear();
  info.error_type.clear();
  info.attempt++;
  return ok();
}

auto DAGRun::mark_task_completed(NodeIndex task_idx, int exit_code)
    -> Result<TransitionDelta> {
  TransitionDelta delta;
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (impl_->task_info_[task_idx].state != TaskState::Running)
    return fail(Error::InvalidState);

  --impl_->runtime_.running_count;
  ++impl_->runtime_.completed_count;

  auto &info = impl_->task_info_[task_idx];
  info.state = TaskState::Success;
  info.exit_code = exit_code;
  info.finished_at = std::chrono::system_clock::now();

  impl_->update_dependent_counters(task_idx, TaskState::Success, +1);
  impl_->propagate_terminal_to_downstream(task_idx, &delta);
  impl_->update_state();
  return ok(std::move(delta));
}

auto DAGRun::mark_task_failed(NodeIndex task_idx, std::string_view error,
                              int max_retries, int exit_code)
    -> Result<TransitionDelta> {
  TransitionDelta delta;
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  if (impl_->task_info_[task_idx].state != TaskState::Running)
    return fail(Error::InvalidState);

  auto &info = impl_->task_info_[task_idx];
  info.error_message.assign(error.data(), error.size());
  info.exit_code = exit_code;
  const auto error_type =
      to_string_view(classify_task_failure_category(error, exit_code));
  info.error_type.assign(error_type.data(), error_type.size());

  if (info.attempt <= max_retries) {
    --impl_->runtime_.running_count;
    ++impl_->runtime_.retrying_count;

    info.state = TaskState::Retrying;
    info.started_at = {};
    info.finished_at = {};
  } else {
    --impl_->runtime_.running_count;
    ++impl_->runtime_.failed_count;
    impl_->runtime_.failed_count_fast.store(impl_->runtime_.failed_count,
                                            std::memory_order_relaxed);

    info.state = TaskState::Failed;
    info.finished_at = std::chrono::system_clock::now();

    impl_->update_dependent_counters(task_idx, TaskState::Failed, +1);
    impl_->propagate_terminal_to_downstream(task_idx, &delta);
    impl_->update_state();
  }

  return ok(std::move(delta));
}

auto DAGRun::mark_task_retry_ready(NodeIndex task_idx)
    -> Result<TransitionDelta> {
  TransitionDelta delta;
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size()) {
    return fail(Error::NotFound);
  }

  auto &info = impl_->task_info_[task_idx];
  if (info.state != TaskState::Retrying) {
    return fail(Error::InvalidState);
  }

  impl_->transition_to_ready(task_idx, &delta);
  return ok(std::move(delta));
}

auto DAGRun::mark_task_skipped(NodeIndex task_idx) -> Result<TransitionDelta> {
  TransitionDelta delta;
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);

  auto &info = impl_->task_info_[task_idx];
  if (info.state != TaskState::Ready && info.state != TaskState::Running) {
    return fail(Error::InvalidState);
  }

  if (info.state == TaskState::Ready) {
    impl_->unlink_ready(task_idx);
    --impl_->runtime_.ready_count;
  } else {
    --impl_->runtime_.running_count;
  }

  ++impl_->runtime_.skipped_count;

  info.state = TaskState::Skipped;

  impl_->update_dependent_counters(task_idx, TaskState::Skipped, +1);
  impl_->propagate_terminal_to_downstream(task_idx, &delta);
  impl_->update_state();
  return ok(std::move(delta));
}

auto DAGRun::mark_task_upstream_failed(NodeIndex task_idx)
    -> Result<TransitionDelta> {
  TransitionDelta delta;
  if (auto r = impl_->mark_task_upstream_failed(task_idx, &delta); !r) {
    return fail(r.error());
  }
  return ok(std::move(delta));
}

auto DAGRun::restore_task_instance(const TaskInstanceInfo &info)
    -> Result<void> {
  return impl_->restore_task_instance(info);
}

auto DAGRun::is_complete() const noexcept -> bool {
  return impl_->state_ == DAGRunState::Success ||
         impl_->state_ == DAGRunState::Failed;
}

auto DAGRun::has_failed() const noexcept -> bool {
  return impl_->runtime_.failed_count_fast.load(std::memory_order_relaxed) > 0;
}

auto DAGRun::all_task_info() const -> std::vector<TaskInstanceInfo> {
  auto out = impl_->task_info_;
  for (auto &info : out) {
    info.run_rowid = impl_->run_rowid_;
  }
  return out;
}

auto DAGRun::get_task_info(NodeIndex task_idx) const
    -> Result<TaskInstanceInfo> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  auto info = impl_->task_info_[task_idx];
  info.run_rowid = impl_->run_rowid_;
  return info;
}

auto DAGRun::set_instance_id(NodeIndex task_idx, const InstanceId &instance_id)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  impl_->task_info_[task_idx].instance_id = instance_id;
  return ok();
}

auto DAGRun::set_task_rowid(NodeIndex task_idx, int64_t task_rowid)
    -> Result<void> {
  if (static_cast<std::size_t>(task_idx) >= impl_->task_info_.size())
    return fail(Error::NotFound);
  impl_->task_info_[task_idx].task_rowid = task_rowid;
  return ok();
}

auto DAGRun::scheduled_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->scheduled_at_;
}

auto DAGRun::set_scheduled_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->scheduled_at_ = t;
}

auto DAGRun::started_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->started_at_;
}

auto DAGRun::set_started_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->started_at_ = t;
}

auto DAGRun::set_finished_at(std::chrono::system_clock::time_point t) noexcept
    -> void {
  impl_->finished_at_ = t;
}

auto DAGRun::finished_at() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->finished_at_;
}

auto DAGRun::execution_date() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->execution_date_;
}

auto DAGRun::set_execution_date(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->execution_date_ = t;
}

auto DAGRun::data_interval_start() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->data_interval_start_;
}

auto DAGRun::set_data_interval_start(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->data_interval_start_ = t;
}

auto DAGRun::data_interval_end() const noexcept
    -> std::chrono::system_clock::time_point {
  return impl_->data_interval_end_;
}

auto DAGRun::set_data_interval_end(
    std::chrono::system_clock::time_point t) noexcept -> void {
  impl_->data_interval_end_ = t;
}

auto DAGRun::trigger_type() const noexcept -> TriggerType {
  return impl_->trigger_type_;
}

auto DAGRun::set_trigger_type(TriggerType t) noexcept -> void {
  impl_->trigger_type_ = t;
}

auto DAGRun::run_rowid() const noexcept -> int64_t { return impl_->run_rowid_; }

auto DAGRun::set_run_rowid(int64_t rowid) noexcept -> void {
  impl_->run_rowid_ = rowid;
}

auto DAGRun::dag_rowid() const noexcept -> int64_t { return impl_->dag_rowid_; }

void DAGRun::set_dag_rowid(int64_t rowid) noexcept {
  impl_->dag_rowid_ = rowid;
}

auto DAGRun::dag_version() const noexcept -> int { return impl_->dag_version_; }

void DAGRun::set_dag_version(int version) noexcept {
  impl_->dag_version_ = version;
}

} // namespace dagforge
