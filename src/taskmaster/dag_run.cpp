#include "taskmaster/dag_run.hpp"

#include <ranges>

namespace taskmaster {

DAGRun::DAGRun(std::string dag_run_id, const DAG &dag)
    : dag_run_id_(std::move(dag_run_id)), dag_(dag) {
  std::size_t n = dag_.size();
  if (n > MAX_TASKS) {
    throw std::runtime_error("DAG size exceeds MAX_TASKS limit");
  }

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

auto DAGRun::init_ready_set() -> void {
  ready_mask_.reset();
  ready_count_ = 0;
  ready_set_.clear();
  ready_set_.reserve(dag_.size());

  for (size_t i = 0; i < dag_.size(); ++i) {
    if (in_degree_[i] == 0) {
      ready_mask_.set(i);
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

auto DAGRun::mark_task_started(NodeIndex task_idx, std::string_view instance_id)
    -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  if (state_ == DAGRunState::Pending) {
    state_ = DAGRunState::Running;
    started_at_ = std::chrono::system_clock::now();
  }

  if (ready_mask_.test(task_idx)) {
    ready_mask_.reset(task_idx);
    ready_set_.erase(task_idx);
    --ready_count_;
  }

  running_mask_.set(task_idx);
  ++running_count_;

  auto &info = task_info_[task_idx];
  info.instance_id = std::string(instance_id);
  info.state = TaskState::Running;
  info.attempt++;
  info.started_at = std::chrono::system_clock::now();
}

auto DAGRun::mark_task_completed(NodeIndex task_idx, int exit_code) -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  running_mask_.reset(task_idx);
  completed_mask_.set(task_idx);
  --running_count_;
  ++completed_count_;

  auto &info = task_info_[task_idx];
  info.state = TaskState::Success;
  info.exit_code = exit_code;
  info.finished_at = std::chrono::system_clock::now();

  update_ready_set(task_idx);
  update_state();
}

auto DAGRun::update_ready_set(NodeIndex completed_task) -> void {
  for (NodeIndex dep : dag_.get_dependents_view(completed_task)) {
    if (--in_degree_[dep] == 0 && !ready_mask_.test(dep) &&
        !running_mask_.test(dep) && !completed_mask_.test(dep) &&
        !failed_mask_.test(dep)) {
      ready_mask_.set(dep);
      ready_set_.insert(dep);
      ++ready_count_;
      --pending_count_;
    }
  }
}

auto DAGRun::mark_task_failed(NodeIndex task_idx, std::string_view error,
                              int max_retries) -> void {
  if (task_idx >= dag_.size()) {
    return;
  }

  running_mask_.reset(task_idx);
  --running_count_;

  auto &info = task_info_[task_idx];
  info.error_message = std::string(error);
  info.finished_at = std::chrono::system_clock::now();

  // Fix: Use < max_retries because attempt was already incremented in mark_task_started
  // So if max_retries=3, we allow attempts 1, 2, 3 (3 total attempts)
  if (info.attempt < max_retries) {
    info.state = TaskState::Pending;
    info.started_at = {};
    info.finished_at = {};
    ready_mask_.set(task_idx);
    ready_set_.insert(task_idx);
    ++ready_count_;
    // Note: Don't adjust pending_count_ - task moves from running back to ready
  } else {
    info.state = TaskState::Failed;
    failed_mask_.set(task_idx);
    ++failed_count_;
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

} // namespace taskmaster
