#pragma once

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "taskmaster/error.hpp"

namespace taskmaster {

using NodeIndex = std::uint32_t;
inline constexpr NodeIndex INVALID_NODE = UINT32_MAX;

class DAG {
public:
  auto add_node(std::string_view key) -> NodeIndex;
  [[nodiscard]] auto add_edge(std::string_view from, std::string_view to)
      -> Result<void>;
  [[nodiscard]] auto add_edge(NodeIndex from, NodeIndex to) -> Result<void>;

  [[nodiscard]] auto has_node(std::string_view key) const -> bool;
  [[nodiscard]] auto is_valid() const -> Result<void>;

  [[nodiscard]] auto get_topological_order() const -> std::vector<std::string>;
  [[nodiscard]] auto get_deps(NodeIndex idx) const -> std::vector<NodeIndex>;
  [[nodiscard]] auto get_dependents(NodeIndex idx) const
      -> std::vector<NodeIndex>;

  [[nodiscard]] auto get_deps_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;
  [[nodiscard]] auto get_dependents_view(NodeIndex idx) const noexcept
      -> std::span<const NodeIndex>;

  [[nodiscard]] auto get_index(std::string_view key) const -> NodeIndex;
  [[nodiscard]] auto get_key(NodeIndex idx) const -> const std::string &;

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return nodes_.size();
  }
  [[nodiscard]] auto empty() const noexcept -> bool { return nodes_.empty(); }
  auto clear() -> void;

  [[nodiscard]] auto all_nodes() const -> std::vector<std::string>;

private:
  struct Node {
    std::vector<NodeIndex> deps;
    std::vector<NodeIndex> dependents;
  };

  std::vector<Node> nodes_;
  std::vector<std::string> keys_;
  std::unordered_map<std::string, NodeIndex, StringHash, std::equal_to<>>
      key_to_idx_;
};

using TaskDAG = DAG;

} // namespace taskmaster
