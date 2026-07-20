#include "path_ast.hpp"

#include <algorithm>
#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace dagforge::jsonata::detail {

namespace {

class PathLowerer {
public:
  PathLowerer(ProgramData &program, const CompileLimits &limits)
      : program_(program), limits_(limits) {}

  [[nodiscard]] auto run() -> Result<void> {
    auto root = lower(program_.root);
    if (!root) {
      return std::unexpected(root.error());
    }
    program_.root = *root;
    (void)mark_tuple_paths(program_.root);
    auto validated = validate_node(program_.root, 0, true);
    if (!validated) {
      return std::unexpected(validated.error());
    }
    return {};
  }

private:
  [[nodiscard]] auto add_node(Node node) -> Result<NodeId> {
    if (program_.nodes.size() >= limits_.max_nodes) {
      return std::unexpected(host_failure("H1103",
                                          "JSONata syntax node limit exceeded",
                                          program_.source, node.span.end));
    }
    program_.nodes.push_back(std::move(node));
    return static_cast<NodeId>(program_.nodes.size() - 1);
  }

  [[nodiscard]] auto path_from(NodeId id, bool quoted_selector = false)
      -> Result<Node> {
    const auto &source = node(id);
    if (source.kind == NodeKind::Path) {
      return source;
    }
    if (source.kind == NodeKind::Filter && !source.children.empty()) {
      auto path = path_from(source.children.front(), quoted_selector);
      if (!path) {
        return std::unexpected(path.error());
      }
      if (!path->pairs.empty()) {
        return std::unexpected(syntax_failure(
            "S0209", "A predicate cannot follow a grouping expression",
            program_.source, source.span.end));
      }
      auto &step = path->path_steps.back();
      if (source.children.size() == 1) {
        step.keep_array = true;
        path->keep_singleton_array = true;
      } else {
        step.stages.push_back(PathStage{.kind = PathStageKind::Filter,
                                        .expression = source.children[1],
                                        .span = source.span});
      }
      path->span = source.span;
      return path;
    }
    auto expression = id;
    if (quoted_selector) {
      auto normalized = normalize_step(id);
      if (!normalized) {
        return std::unexpected(normalized.error());
      }
      expression = *normalized;
    }
    return Node{.kind = NodeKind::Path,
                .span = node(expression).span,
                .path_steps = {PathStep{.expression = expression}}};
  }

  [[nodiscard]] auto normalize_step(NodeId id) -> Result<NodeId> {
    auto &step = mutable_node(id);
    if (step.kind != NodeKind::Literal) {
      return id;
    }
    if (const auto *text = std::get_if<std::string>(&step.literal)) {
      step.kind = NodeKind::Name;
      step.text = *text;
      step.literal = nullptr;
      return id;
    }
    return std::unexpected(syntax_failure(
        "S0213", "A path step cannot be a number, boolean, or null",
        program_.source, step.span.end));
  }

  [[nodiscard]] auto lower(NodeId id) -> Result<NodeId> {
    const auto original = node(id);
    switch (original.kind) {
    case NodeKind::Name:
      return lower_name(id, original);
    case NodeKind::Binary:
      if (original.text == ".") {
        return lower_dot(id, original);
      }
      break;
    case NodeKind::Filter:
      return lower_filter(id, original);
    case NodeKind::IndexBind:
      return lower_index(id, original);
    case NodeKind::FocusBind:
      return lower_focus(id, original);
    case NodeKind::Sort:
      return lower_sort(id, original);
    case NodeKind::Group:
      return lower_group(id, original);
    default:
      break;
    }

    auto lowered = original;
    for (auto &child : lowered.children) {
      auto result = lower(child);
      if (!result) {
        return std::unexpected(result.error());
      }
      child = *result;
    }
    for (auto &[key, value] : lowered.pairs) {
      auto lowered_key = lower(key);
      if (!lowered_key) {
        return std::unexpected(lowered_key.error());
      }
      auto lowered_value = lower(value);
      if (!lowered_value) {
        return std::unexpected(lowered_value.error());
      }
      key = *lowered_key;
      value = *lowered_value;
    }
    mutable_node(id) = std::move(lowered);
    return id;
  }

  [[nodiscard]] auto lower_name(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto step = add_node(original);
    if (!step) {
      return std::unexpected(step.error());
    }
    mutable_node(id) = Node{
        .kind = NodeKind::Path,
        .span = original.span,
        .path_steps = {PathStep{.expression = *step}},
    };
    return id;
  }

  [[nodiscard]] auto lower_dot(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto left = lower(original.children[0]);
    if (!left) {
      return std::unexpected(left.error());
    }
    auto right = lower(original.children[1]);
    if (!right) {
      return std::unexpected(right.error());
    }
    auto path = path_from(*left, true);
    if (!path) {
      return std::unexpected(path.error());
    }
    auto rest = path_from(*right, true);
    if (!rest) {
      return std::unexpected(rest.error());
    }
    path->path_steps.insert(path->path_steps.end(), rest->path_steps.begin(),
                            rest->path_steps.end());
    if (!path->pairs.empty() && !rest->pairs.empty()) {
      return std::unexpected(syntax_failure(
          "S0210", "Each step can contain only one grouping expression",
          program_.source, original.span.end));
    }
    if (path->pairs.empty()) {
      path->pairs = std::move(rest->pairs);
    }
    path->keep_singleton_array =
        path->keep_singleton_array || rest->keep_singleton_array;
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto lower_filter(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto source = lower(original.children[0]);
    if (!source) {
      return std::unexpected(source.error());
    }
    if (node(*source).kind != NodeKind::Path) {
      auto lowered = original;
      lowered.children[0] = *source;
      if (lowered.children.size() > 1) {
        auto predicate = lower(lowered.children[1]);
        if (!predicate) {
          return std::unexpected(predicate.error());
        }
        lowered.children[1] = *predicate;
      }
      mutable_node(id) = std::move(lowered);
      return id;
    }
    auto path = path_from(*source);
    if (!path) {
      return std::unexpected(path.error());
    }
    if (!path->pairs.empty()) {
      return std::unexpected(syntax_failure(
          "S0209", "A predicate cannot follow a grouping expression",
          program_.source, original.span.end));
    }
    auto &step = path->path_steps.back();
    if (original.children.size() == 1) {
      step.keep_array = true;
      path->keep_singleton_array = true;
    } else {
      auto predicate = lower(original.children[1]);
      if (!predicate) {
        return std::unexpected(predicate.error());
      }
      step.stages.push_back(PathStage{.kind = PathStageKind::Filter,
                                      .expression = *predicate,
                                      .span = original.span});
    }
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto lower_index(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto source = lower(original.children[0]);
    if (!source) {
      return std::unexpected(source.error());
    }
    auto path = path_from(*source);
    if (!path) {
      return std::unexpected(path.error());
    }
    auto &step = path->path_steps.back();
    if (step.stages.empty()) {
      step.index_binding = original.text;
    } else {
      step.stages.push_back(PathStage{.kind = PathStageKind::Index,
                                      .name = original.text,
                                      .span = original.span});
    }
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto lower_focus(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto source = lower(original.children[0]);
    if (!source) {
      return std::unexpected(source.error());
    }
    auto path = path_from(*source);
    if (!path) {
      return std::unexpected(path.error());
    }
    auto &step = path->path_steps.back();
    if (!step.stages.empty()) {
      return std::unexpected(syntax_failure(
          "S0215", "The focus binding operator cannot follow a predicate",
          program_.source, original.span.end));
    }
    if (node(step.expression).kind == NodeKind::Sort) {
      return std::unexpected(syntax_failure(
          "S0216", "The focus binding operator cannot follow an order-by",
          program_.source, original.span.end));
    }
    step.focus_binding = original.text;
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto lower_sort(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto source = lower(original.children[0]);
    if (!source) {
      return std::unexpected(source.error());
    }
    auto path = path_from(*source);
    if (!path) {
      return std::unexpected(path.error());
    }
    std::vector<NodeId> terms;
    terms.reserve(original.children.size() - 1);
    for (std::size_t index = 1; index < original.children.size(); ++index) {
      auto term = lower(original.children[index]);
      if (!term) {
        return std::unexpected(term.error());
      }
      terms.push_back(*term);
    }
    auto sort = add_node(Node{.kind = NodeKind::Sort,
                              .span = original.span,
                              .children = std::move(terms),
                              .flags = original.flags});
    if (!sort) {
      return std::unexpected(sort.error());
    }
    path->path_steps.push_back(PathStep{.expression = *sort});
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto lower_group(NodeId id, const Node &original)
      -> Result<NodeId> {
    auto source = lower(original.children[0]);
    if (!source) {
      return std::unexpected(source.error());
    }
    auto path = path_from(*source);
    if (!path) {
      return std::unexpected(path.error());
    }
    if (!path->pairs.empty()) {
      return std::unexpected(syntax_failure(
          "S0210", "Each step can contain only one grouping expression",
          program_.source, original.span.end));
    }
    for (const auto &[key_id, value_id] : original.pairs) {
      auto key = lower(key_id);
      if (!key) {
        return std::unexpected(key.error());
      }
      auto value = lower(value_id);
      if (!value) {
        return std::unexpected(value.error());
      }
      path->pairs.emplace_back(*key, *value);
    }
    path->span = original.span;
    mutable_node(id) = std::move(*path);
    return id;
  }

  [[nodiscard]] auto validate_node(NodeId id, std::size_t available_depth,
                                   bool root_position) -> Result<std::size_t> {
    const auto &current = node(id);
    if (current.kind == NodeKind::Parent) {
      if (root_position || available_depth == 0) {
        return std::unexpected(syntax_failure(
            "S0217", "The parent of the current context cannot be derived",
            program_.source, current.span.end, "%"));
      }
      return available_depth - 1;
    }
    if (current.kind == NodeKind::Path) {
      auto depth = available_depth;
      bool previous_focus_binding = false;
      for (const auto &step : current.path_steps) {
        const auto &expression = node(step.expression);
        if (expression.kind == NodeKind::Parent) {
          if (depth == 0) {
            return std::unexpected(syntax_failure(
                "S0217", "The parent of the current context cannot be derived",
                program_.source, expression.span.end, "%"));
          }
          --depth;
        } else {
          auto nested = validate_node(step.expression, depth, false);
          if (!nested) {
            return std::unexpected(nested.error());
          }
          if (expression.kind == NodeKind::Block) {
            depth = *nested;
          } else if (step.focus_binding) {
            if (!previous_focus_binding) {
              ++depth;
            }
          } else if (expression.kind != NodeKind::Sort) {
            if (expression.kind == NodeKind::Variable) {
              depth = 0;
            } else if (expression.kind == NodeKind::Name ||
                       expression.kind == NodeKind::Wildcard) {
              ++depth;
            }
          }
        }
        previous_focus_binding = step.focus_binding.has_value();
        for (const auto &stage : step.stages) {
          if (stage.kind == PathStageKind::Filter) {
            auto valid = validate_node(stage.expression, depth, false);
            if (!valid) {
              return std::unexpected(valid.error());
            }
          }
        }
      }
      for (const auto &[key, value] : current.pairs) {
        auto key_valid = validate_node(key, depth, false);
        if (!key_valid) {
          return std::unexpected(key_valid.error());
        }
        auto value_valid = validate_node(value, depth, false);
        if (!value_valid) {
          return std::unexpected(value_valid.error());
        }
      }
      return depth;
    }
    if (current.kind == NodeKind::Block && !current.children.empty()) {
      for (std::size_t index = 0; index + 1 < current.children.size();
           ++index) {
        auto valid =
            validate_node(current.children[index], available_depth, false);
        if (!valid) {
          return std::unexpected(valid.error());
        }
      }
      return validate_node(current.children.back(), available_depth,
                           root_position);
    }
    if (current.kind == NodeKind::Call) {
      return available_depth;
    }
    for (const auto child : current.children) {
      auto valid = validate_node(child, available_depth, false);
      if (!valid) {
        return std::unexpected(valid.error());
      }
    }
    for (const auto &[key, value] : current.pairs) {
      auto key_valid = validate_node(key, available_depth, false);
      if (!key_valid) {
        return std::unexpected(key_valid.error());
      }
      auto value_valid = validate_node(value, available_depth, false);
      if (!value_valid) {
        return std::unexpected(value_valid.error());
      }
    }
    return available_depth;
  }

  [[nodiscard]] auto mark_tuple_paths(NodeId id) -> bool {
    auto &current = mutable_node(id);
    bool contains_parent = current.kind == NodeKind::Parent;
    for (const auto child : current.children) {
      contains_parent = mark_tuple_paths(child) || contains_parent;
    }
    for (const auto &[key, value] : current.pairs) {
      contains_parent = mark_tuple_paths(key) || contains_parent;
      contains_parent = mark_tuple_paths(value) || contains_parent;
    }
    if (current.kind == NodeKind::Path) {
      for (const auto &step : current.path_steps) {
        contains_parent = mark_tuple_paths(step.expression) || contains_parent;
        for (const auto &stage : step.stages) {
          if (stage.kind == PathStageKind::Filter) {
            contains_parent =
                mark_tuple_paths(stage.expression) || contains_parent;
          }
        }
      }
      current.tuple_path = contains_parent;
    }
    return contains_parent;
  }

  [[nodiscard]] auto node(NodeId id) const -> const Node & {
    return program_.nodes[id];
  }

  [[nodiscard]] auto mutable_node(NodeId id) -> Node & {
    return program_.nodes[id];
  }

  ProgramData &program_;
  const CompileLimits &limits_;
};

} // namespace

auto lower_path_ast(ProgramData &program, const CompileLimits &limits)
    -> Result<void> {
  return PathLowerer(program, limits).run();
}

} // namespace dagforge::jsonata::detail
