#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/config/task_policies.hpp"
#include "dagforge/core/arena.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/dag/graph_types.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"
#endif

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <ankerl/unordered_dense.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ranges>
#include <span>
#include <utility>
#include <vector>
#endif


namespace dagforge {

class DAG {
public:
  [[nodiscard]] auto add_node(const TaskId &task_id,
                              TriggerRule rule = TriggerRule::AllSuccess,
                              bool is_branch = false) -> Result<NodeIndex> {
    auto it = key_to_idx_.find(task_id);
    if (it != key_to_idx_.end()) {
      return ok(it->second);
    }

    // dynamic_bitset removes the fixed 4096 task cap; keep a sane upper bound
    if (nodes_.size() >= 1'000'000) {
      return fail(Error::ResourceExhausted);
    }

    NodeIndex idx = static_cast<NodeIndex>(nodes_.size());
    Node node;
    node.trigger_rule = rule;
    node.is_branch = is_branch;
    nodes_.emplace_back(std::move(node));
    keys_.emplace_back(task_id);
    key_to_idx_.emplace(task_id, idx);

    return ok(idx);
  }
  [[nodiscard]] auto add_edge(const TaskId &from, const TaskId &to)
      -> Result<void> {
    NodeIndex from_idx = get_index(from);
    NodeIndex to_idx = get_index(to);
    if (from_idx == kInvalidNode || to_idx == kInvalidNode) [[unlikely]] {
      return fail(Error::NotFound);
    }
    return add_edge(from_idx, to_idx);
  }
  [[nodiscard]] auto add_edge(NodeIndex from, NodeIndex to) -> Result<void> {
    if (from >= nodes_.size() || to >= nodes_.size() || from == to)
        [[unlikely]] {
      return fail(Error::InvalidArgument);
    }

    nodes_[to].deps.emplace_back(from);
    nodes_[from].dependents.emplace_back(to);
    return ok();
  }

  [[nodiscard]] auto has_node(const TaskId &task_id) const -> bool {
    return key_to_idx_.contains(task_id);
  }
  [[nodiscard]] auto is_valid() const -> Result<void> {
    Arena<2048> arena;
    auto state = arena.vector<std::uint8_t>(nodes_.size());
    state.assign(nodes_.size(), 0);
    auto stack = arena.vector<std::pair<NodeIndex, std::size_t>>();
    stack.reserve(nodes_.size());

    for (NodeIndex start :
         std::views::iota(NodeIndex{0},
                          static_cast<NodeIndex>(nodes_.size()))) {
      if (state[start] != 0) {
        continue;
      }

      stack.emplace_back(start, 0);
      state[start] = 1;

      while (!stack.empty()) {
        auto &[node, child_idx] = stack.back();
        const auto &deps = nodes_[node].dependents;

        if (child_idx < deps.size()) {
          NodeIndex child = deps[child_idx++];
          if (state[child] == 1) {
            return fail(Error::InvalidArgument);
          }
          if (state[child] == 0) {
            state[child] = 1;
            stack.emplace_back(child, 0);
          }
        } else {
          state[node] = 2;
          stack.pop_back();
        }
      }
    }
    return ok();
  }

  [[nodiscard]] auto get_topological_order() const -> std::vector<TaskId> {
    std::vector<int> in_degree;
    in_degree.reserve(nodes_.size());
    for (const auto &node : nodes_) {
      in_degree.emplace_back(static_cast<int>(node.deps.size()));
    }

    std::vector<NodeIndex> ready;
    ready.reserve(nodes_.size());
    for (auto [i, deg] : std::views::enumerate(in_degree)) {
      if (deg == 0) {
        ready.emplace_back(static_cast<NodeIndex>(i));
      }
    }

    std::vector<TaskId> result;
    result.reserve(nodes_.size());

    std::size_t head = 0;
    while (head < ready.size()) {
      NodeIndex current = ready[head++];
      result.emplace_back(keys_[current]);

      for (NodeIndex dep : nodes_[current].dependents) {
        if (--in_degree[dep] == 0) {
          ready.emplace_back(dep);
        }
      }
    }

    return result;
  }
  [[nodiscard]] auto get_deps(NodeIndex idx) const -> std::vector<NodeIndex> {
    if (idx >= nodes_.size()) {
      return {};
    }
    return nodes_[idx].deps;
  }
  [[nodiscard]] auto get_dependents(NodeIndex idx) const
      -> std::vector<NodeIndex> {
    if (idx >= nodes_.size()) {
      return {};
    }
    return nodes_[idx].dependents;
  }

  [[nodiscard]] auto get_deps_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex> {
    if (idx >= nodes_.size()) {
      return {};
    }
    return nodes_[idx].deps;
  }
  [[nodiscard]] auto get_dependents_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex> {
    if (idx >= nodes_.size()) {
      return {};
    }
    return nodes_[idx].dependents;
  }

  [[nodiscard]] auto get_index(const TaskId &task_id) const -> NodeIndex {
    auto it = key_to_idx_.find(task_id);
    return it != key_to_idx_.end() ? it->second : kInvalidNode;
  }
  [[nodiscard]] auto get_key(NodeIndex idx) const -> TaskId {
    if (idx >= keys_.size()) {
      return {};
    }
    return keys_[idx];
  }
  [[nodiscard]] auto get_trigger_rule(NodeIndex idx) const noexcept
      -> TriggerRule {
    if (idx >= nodes_.size()) {
      return TriggerRule::AllSuccess;
    }
    return nodes_[idx].trigger_rule;
  }
  [[nodiscard]] auto is_branch_task(NodeIndex idx) const noexcept -> bool {
    if (idx >= nodes_.size()) {
      return false;
    }
    return nodes_[idx].is_branch;
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return nodes_.size();
  }
  [[nodiscard]] auto empty() const noexcept -> bool { return nodes_.empty(); }
  auto clear() -> void {
    nodes_.clear();
    keys_.clear();
    key_to_idx_.clear();
  }

  [[nodiscard]] auto all_nodes() const -> std::vector<TaskId> { return keys_; }

private:
  [[nodiscard]] auto has_edge(NodeIndex from, NodeIndex to) const noexcept
      -> bool {
    if (from >= nodes_.size() || to >= nodes_.size()) [[unlikely]] {
      return false;
    }

    const auto &dependents = nodes_[from].dependents;
    return std::find(dependents.begin(), dependents.end(), to) !=
           dependents.end();
  }

  struct Node {
    std::vector<NodeIndex> deps;
    std::vector<NodeIndex> dependents;
    TriggerRule trigger_rule{TriggerRule::AllSuccess};
    bool is_branch{false};
  };

  std::vector<Node> nodes_;
  std::vector<TaskId> keys_;
  ankerl::unordered_dense::map<TaskId, NodeIndex> key_to_idx_;
};

} // namespace dagforge
