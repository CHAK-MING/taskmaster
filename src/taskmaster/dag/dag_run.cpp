#include "taskmaster/dag/dag_run.hpp"

#include "taskmaster/config/task_config.hpp"

#include <ranges>

namespace taskmaster {

DAGRun::DAGRun(DAGRunPrivateTag, DAGRunId dag_run_id, const DAG& dag)
    : dag_run_id_(std::move(dag_run_id)), dag_(dag) {
  std::size_t n = dag_.size();
  in_degree_.resize(n, 0);
  task_info_.resize(n);

  for (auto [i, info] : std::views::enumerate(task_info_)) {
    info.task_idx = static_cast<NodeIndex>(i);
    info.state = TaskState::Pending;
    in_degree_[i] =
        static_cast<int>(dag_.get_deps_view(static_cast<NodeIndex>(i)).size());
  }

  pending_count_ = n;
  init_ready_set();
}

auto DAGRun::create(DAGRunId dag_run_id, const DAG& dag)
    -> Result<DAGRun> {
  std::size_t n = dag.size();
  if (n > kMaxTasks) {
    return fail(Error::InvalidArgument);
  }

  return DAGRun(DAGRunPrivateTag{}, std::move(dag_run_id), dag);
}

auto DAGRun::init_ready_set() -> void {
  ready_mask_.reset();
  ready_count_ = 0;
  ready_set_.clear();

  for (auto [i, deg] : std::views::enumerate(in_degree_)) {
    if (deg == 0) {
      ready_mask_.set(static_cast<std::size_t>(i));
      ready_set_.insert(static_cast<NodeIndex>(i));
      ++ready_count_;
    }
  }
  pending_count_ -= ready_count_;
}

auto DAGRun::get_ready_tasks() const -> std::vector<NodeIndex> {
  if (state_ == DAGRunState::Failed || state_ == DAGRunState::Success) {
    return {};
  }
  return {ready_set_.begin(), ready_set_.end()};
}

auto DAGRun::mark_task_started(NodeIndex task_idx, const InstanceId& instance_id)
    -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  if (ready_mask_.test(task_idx)) {
    ready_mask_.reset(task_idx);
    ready_set_.erase(task_idx);
    --ready_count_;
  }

  running_mask_.set(task_idx);
  ++running_count_;

  auto& info = task_info_[task_idx];
  info.instance_id = instance_id;
  info.state = TaskState::Running;
  info.attempt++;
  info.started_at = std::chrono::system_clock::now();
}

auto DAGRun::set_instance_id(NodeIndex task_idx, const InstanceId& instance_id)
    -> void {
  if (task_idx >= dag_.size()) {
    return;
  }
  auto& info = task_info_[task_idx];
  info.instance_id = instance_id;
}

auto DAGRun::mark_task_completed(NodeIndex task_idx, int exit_code) -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  running_mask_.reset(task_idx);
  completed_mask_.set(task_idx);
  --running_count_;
  ++completed_count_;

  auto& info = task_info_[task_idx];
  info.state = TaskState::Success;
  info.exit_code = exit_code;
  info.finished_at = std::chrono::system_clock::now();

  propagate_terminal_to_downstream(task_idx);
  update_state();
}

auto DAGRun::all_deps_terminal(NodeIndex task_idx) const -> bool {
  auto deps = dag_.get_deps_view(task_idx);
  return std::ranges::all_of(deps, [this](NodeIndex dep_idx) {
    return completed_mask_.test(dep_idx) || 
           failed_mask_.test(dep_idx) || 
           skipped_mask_.test(dep_idx);
  });
}

auto DAGRun::mark_task_skipped(NodeIndex task_idx) -> void {
  if (task_idx >= dag_.size()) {
    return;
  }
  
  if (ready_mask_.test(task_idx)) {
    ready_mask_.reset(task_idx);
    ready_set_.erase(task_idx);
    --ready_count_;
  } else if (!running_mask_.test(task_idx) && !completed_mask_.test(task_idx) &&
             !failed_mask_.test(task_idx) && !skipped_mask_.test(task_idx)) {
    --pending_count_;
  }
  
  skipped_mask_.set(task_idx);
  ++skipped_count_;
  
  auto& info = task_info_[task_idx];
  info.state = TaskState::Skipped;
  info.finished_at = std::chrono::system_clock::now();
  info.error_message = "Trigger rule not satisfied";
  
  propagate_terminal_to_downstream(task_idx);
}

auto DAGRun::propagate_terminal_to_downstream(NodeIndex terminal_task) -> void {
  for (NodeIndex dep : dag_.get_dependents_view(terminal_task)) {
    if (ready_mask_.test(dep) || running_mask_.test(dep) || 
        completed_mask_.test(dep) || failed_mask_.test(dep) ||
        skipped_mask_.test(dep)) {
      continue;
    }
    
    if (!all_deps_terminal(dep)) {
      continue;
    }
    
    if (should_trigger(dep)) {
      ready_mask_.set(dep);
      ready_set_.insert(dep);
      ++ready_count_;
      --pending_count_;
    } else {
      mark_task_skipped(dep);
    }
  }
}

auto DAGRun::should_trigger(NodeIndex task_idx) const -> bool {
  auto deps = dag_.get_deps_view(task_idx);
  
  if (deps.empty()) [[unlikely]] {
    return true;
  }
  
  TriggerRule rule = dag_.get_trigger_rule(task_idx);
  auto const total = deps.size();
  
  auto const success_count = static_cast<std::size_t>(
      std::ranges::count_if(deps, [this](NodeIndex i) { 
        return completed_mask_.test(i); 
      }));
  auto const failed_count = static_cast<std::size_t>(
      std::ranges::count_if(deps, [this](NodeIndex i) { 
        return failed_mask_.test(i); 
      }));
  auto const skipped_count = static_cast<std::size_t>(
      std::ranges::count_if(deps, [this](NodeIndex i) { 
        return skipped_mask_.test(i); 
      }));
  auto const done_count = success_count + failed_count + skipped_count;
  
  switch (rule) {
    case TriggerRule::AllSuccess:
      return success_count == total;
    case TriggerRule::AllFailed:
      return failed_count == total;
    case TriggerRule::AllDone:
      return done_count == total;
    case TriggerRule::OneSuccess:
      return success_count >= 1 && done_count == total;
    case TriggerRule::OneFailed:
      return failed_count >= 1 && done_count == total;
    case TriggerRule::NoneFailed:
      return failed_count == 0 && done_count == total;
    case TriggerRule::NoneSkipped:
      return skipped_count == 0 && done_count == total;
  }
  
  std::unreachable();
}

auto DAGRun::mark_task_failed(NodeIndex task_idx, std::string_view error,
                              int max_retries, int exit_code) -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  running_mask_.reset(task_idx);
  --running_count_;

  auto& info = task_info_[task_idx];
  info.exit_code = exit_code;
  info.error_message = std::string(error);
  info.finished_at = std::chrono::system_clock::now();

  if (info.attempt < max_retries) {
    info.state = TaskState::Pending;
    info.started_at = {};
    info.finished_at = {};
    ready_mask_.set(task_idx);
    ready_set_.insert(task_idx);
    ++ready_count_;
  } else {
    info.state = TaskState::Failed;
    failed_mask_.set(task_idx);
    ++failed_count_;
    propagate_terminal_to_downstream(task_idx);
  }

  update_state();
}

auto DAGRun::is_complete() const noexcept -> bool {
  return state_ == DAGRunState::Success || state_ == DAGRunState::Failed;
}

auto DAGRun::has_failed() const noexcept -> bool {
  return state_ == DAGRunState::Failed;
}

auto DAGRun::get_task_info(NodeIndex task_idx) const
    -> std::optional<TaskInstanceInfo> {
  if (task_idx >= task_info_.size()) {
    return std::nullopt;
  }
  return task_info_[task_idx];
}

auto DAGRun::all_task_info() const -> std::vector<TaskInstanceInfo> {
  return task_info_;
}

auto DAGRun::update_state() -> void {
  if (failed_count_ > 0 && running_count_ == 0 && pending_count_ == 0 &&
      ready_count_ == 0) {
    state_ = DAGRunState::Failed;
    finished_at_ = std::chrono::system_clock::now();
    return;
  }

  if (completed_count_ == dag_.size()) {
    state_ = DAGRunState::Success;
    finished_at_ = std::chrono::system_clock::now();
  }
}

}  // namespace taskmaster
