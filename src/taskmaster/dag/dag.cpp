#include "taskmaster/dag/dag.hpp"

#include "taskmaster/core/arena.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <queue>
#include <ranges>
#include <span>

namespace taskmaster {

auto DAG::add_node(TaskId task_id, TriggerRule rule) -> NodeIndex {
  auto it = key_to_idx_.find(task_id);
  if (it != key_to_idx_.end()) {
    return it->second;
  }

  NodeIndex idx = static_cast<NodeIndex>(nodes_.size());
  Node node;
  node.trigger_rule = rule;
  nodes_.push_back(std::move(node));
  keys_.emplace_back(task_id);
  key_to_idx_.emplace(task_id, idx);
  return idx;
}

auto DAG::add_edge(TaskId from, TaskId to) -> Result<void> {
  NodeIndex from_idx = get_index(from);
  NodeIndex to_idx = get_index(to);
  if (from_idx == kInvalidNode || to_idx == kInvalidNode) [[unlikely]] {
    return fail(Error::NotFound);
  }
  return add_edge(from_idx, to_idx);
}

auto DAG::add_edge(NodeIndex from, NodeIndex to) -> Result<void> {
  if (from >= nodes_.size() || to >= nodes_.size() || from == to) [[unlikely]] {
    return fail(Error::InvalidArgument);
  }

  if (would_create_cycle(from, to)) {
    return fail(Error::CycleDetected);
  }

  nodes_[to].deps.push_back(from);
  nodes_[from].dependents.push_back(to);
  return ok();
}

auto DAG::would_create_cycle(NodeIndex from, NodeIndex to) const -> bool {
  Arena<2048> arena;
  auto visited = arena.vector<bool>(nodes_.size());
  visited.assign(nodes_.size(), false);
  auto stack = arena.vector<NodeIndex>();
  stack.push_back(from);

  while (!stack.empty()) {
    NodeIndex current = stack.back();
    stack.pop_back();

    if (current == to) {
      return true;
    }

    if (visited[current]) {
      continue;
    }
    visited[current] = true;

    for (NodeIndex dep : nodes_[current].deps) {
      if (!visited[dep]) {
        stack.push_back(dep);
      }
    }
  }
  return false;
}

auto DAG::has_node(TaskId task_id) const -> bool {
  return key_to_idx_.contains(task_id);
}

auto DAG::is_valid() const -> Result<void> {
  Arena<2048> arena;
  auto state = arena.vector<std::uint8_t>(nodes_.size());
  state.assign(nodes_.size(), 0);
  auto stack = arena.vector<std::pair<NodeIndex, std::size_t>>();

  for (NodeIndex start : std::views::iota(NodeIndex{0}, static_cast<NodeIndex>(nodes_.size()))) {
    if (state[start] != 0)
      continue;

    stack.push_back({start, 0});
    state[start] = 1;

    while (!stack.empty()) {
      auto& [node, child_idx] = stack.back();
      const auto& deps = nodes_[node].dependents;

      if (child_idx < deps.size()) {
        NodeIndex child = deps[child_idx++];
        if (state[child] == 1) {
          return fail(Error::InvalidArgument);
        }
        if (state[child] == 0) {
          state[child] = 1;
          stack.push_back({child, 0});
        }
      } else {
        state[node] = 2;
        stack.pop_back();
      }
    }
  }
  return ok();
}

auto DAG::get_topological_order() const -> std::vector<TaskId> {
  auto in_degree = nodes_ | std::views::transform([](const Node& n) {
                     return static_cast<int>(n.deps.size());
                   }) |
                   std::ranges::to<std::vector>();

  std::queue<NodeIndex> ready;
  for (auto [i, deg] : std::views::enumerate(in_degree)) {
    if (deg == 0) {
      ready.push(static_cast<NodeIndex>(i));
    }
  }

  std::vector<TaskId> result;
  result.reserve(nodes_.size());
  while (!ready.empty()) {
    NodeIndex current = ready.front();
    ready.pop();
    result.push_back(keys_[current]);

    for (NodeIndex dep : nodes_[current].dependents) {
      if (--in_degree[dep] == 0) {
        ready.push(dep);
      }
    }
  }

  return result;
}

auto DAG::get_deps(NodeIndex idx) const -> std::vector<NodeIndex> {
  if (idx >= nodes_.size()) {
    return {};
  }
  return nodes_[idx].deps;
}

auto DAG::get_dependents(NodeIndex idx) const -> std::vector<NodeIndex> {
  if (idx >= nodes_.size()) {
    return {};
  }
  return nodes_[idx].dependents;
}

auto DAG::get_deps_view(NodeIndex idx) const noexcept
    -> std::span<const NodeIndex> {
  if (idx >= nodes_.size()) {
    return {};
  }
  return nodes_[idx].deps;
}

auto DAG::get_dependents_view(NodeIndex idx) const noexcept
    -> std::span<const NodeIndex> {
  if (idx >= nodes_.size()) {
    return {};
  }
  return nodes_[idx].dependents;
}

auto DAG::get_index(TaskId task_id) const -> NodeIndex {
  auto it = key_to_idx_.find(task_id);
  return it != key_to_idx_.end() ? it->second : kInvalidNode;
}

auto DAG::get_key(NodeIndex idx) const -> TaskId {
  if (idx >= keys_.size()) {
    return {};
  }
  return keys_[idx];
}

auto DAG::get_trigger_rule(NodeIndex idx) const noexcept -> TriggerRule {
  if (idx >= nodes_.size()) {
    return TriggerRule::AllSuccess;
  }
  return nodes_[idx].trigger_rule;
}

auto DAG::clear() -> void {
  nodes_.clear();
  keys_.clear();
  key_to_idx_.clear();
}

auto DAG::all_nodes() const -> std::vector<TaskId> {
  return keys_;
}

}  // namespace taskmaster
