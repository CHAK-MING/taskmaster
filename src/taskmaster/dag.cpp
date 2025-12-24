#include "taskmaster/dag.hpp"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <queue>
#include <ranges>
#include <span>

namespace taskmaster {

auto DAG::add_node(std::string_view key) -> NodeIndex {
  auto it = key_to_idx_.find(key);
  if (it != key_to_idx_.end()) {
    return it->second;
  }

  NodeIndex idx = static_cast<NodeIndex>(nodes_.size());
  nodes_.emplace_back();
  keys_.emplace_back(key);
  key_to_idx_.emplace(std::string(key), idx);
  return idx;
}

auto DAG::add_edge(std::string_view from, std::string_view to) -> Result<void> {
  NodeIndex from_idx = get_index(from);
  NodeIndex to_idx = get_index(to);
  if (from_idx == INVALID_NODE || to_idx == INVALID_NODE) {
    return fail(Error::NotFound);
  }
  return add_edge(from_idx, to_idx);
}

auto DAG::add_edge(NodeIndex from, NodeIndex to) -> Result<void> {
  if (from >= nodes_.size() || to >= nodes_.size() || from == to) {
    return fail(Error::InvalidArgument);
  }
  nodes_[to].deps.push_back(from);
  nodes_[from].dependents.push_back(to);
  return ok();
}

auto DAG::has_node(std::string_view key) const -> bool {
  return key_to_idx_.contains(key);
}

auto DAG::is_valid() const -> Result<void> {
  std::vector<std::uint8_t> state(nodes_.size(), 0);
  std::vector<std::pair<NodeIndex, std::size_t>>
      stack; // (node, next_child_index)

  for (NodeIndex start = 0; start < nodes_.size(); ++start) {
    if (state[start] != 0)
      continue;

    stack.push_back({start, 0});
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

auto DAG::get_topological_order() const -> std::vector<std::string> {
  auto in_degree = nodes_ | std::views::transform([](const Node &n) {
                     return static_cast<int>(n.deps.size());
                   }) |
                   std::ranges::to<std::vector>();

  std::queue<NodeIndex> ready;
  for (auto [i, deg] : std::views::enumerate(in_degree)) {
    if (deg == 0) {
      ready.push(static_cast<NodeIndex>(i));
    }
  }

  std::vector<std::string> result;
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

auto DAG::get_index(std::string_view key) const -> NodeIndex {
  auto it = key_to_idx_.find(key);
  return it != key_to_idx_.end() ? it->second : INVALID_NODE;
}

auto DAG::get_key(NodeIndex idx) const -> const std::string & {
  if (idx >= keys_.size()) {
    std::fputs("Invalid node index\n", stderr);
    std::fflush(stderr);
    std::abort();
  }
  return keys_[idx];
}

auto DAG::clear() -> void {
  nodes_.clear();
  keys_.clear();
  key_to_idx_.clear();
}

auto DAG::all_nodes() const -> std::vector<std::string> { return keys_; }

} // namespace taskmaster
