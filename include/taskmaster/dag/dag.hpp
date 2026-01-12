#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/util/id.hpp"

#include <cstdint>
#include <flat_map>
#include <functional>
#include <span>
#include <vector>

namespace taskmaster {

using NodeIndex = std::uint32_t;
inline constexpr NodeIndex INVALID_NODE = UINT32_MAX;

class DAG {
public:
  auto add_node(TaskId task_id) -> NodeIndex;
  [[nodiscard]] auto add_edge(TaskId from, TaskId to)
      -> Result<void>;
  [[nodiscard]] auto add_edge(NodeIndex from, NodeIndex to) -> Result<void>;

  [[nodiscard]] auto has_node(TaskId task_id) const -> bool;
  [[nodiscard]] auto is_valid() const -> Result<void>;

  [[nodiscard]] auto get_topological_order() const -> std::vector<TaskId>;
  [[nodiscard]] auto get_deps(NodeIndex idx) const -> std::vector<NodeIndex>;
  [[nodiscard]] auto get_dependents(NodeIndex idx) const
      -> std::vector<NodeIndex>;

  [[nodiscard]] auto get_deps_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;
  [[nodiscard]] auto get_dependents_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;

  [[nodiscard]] auto get_index(TaskId task_id) const -> NodeIndex;
  [[nodiscard]] auto get_key(NodeIndex idx) const -> TaskId;

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return nodes_.size();
  }
  [[nodiscard]] auto empty() const noexcept -> bool {
    return nodes_.empty();
  }
  auto clear() -> void;

  [[nodiscard]] auto all_nodes() const -> std::vector<TaskId>;

private:
  [[nodiscard]] auto would_create_cycle(NodeIndex from, NodeIndex to) const -> bool;

  struct Node {
    std::vector<NodeIndex> deps;
    std::vector<NodeIndex> dependents;
  };

  std::vector<Node> nodes_;
  std::vector<TaskId> keys_;
  std::flat_map<TaskId, NodeIndex, std::less<>> key_to_idx_;
};

using TaskDAG = DAG;

}  // namespace taskmaster
