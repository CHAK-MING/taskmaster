#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/id.hpp"

#include <ranges>
#include <string>
#include <vector>
#endif

namespace dagforge {

struct TaskDependency {
  TaskId task_id;
  std::string label;

  bool operator==(const TaskId &other) const { return task_id == other; }
  bool operator==(const TaskDependency &other) const = default;
};

inline auto get_dep_task_ids(const std::vector<TaskDependency> &deps) {
  return deps |
         std::views::transform([](const TaskDependency &d) -> const TaskId & {
           return d.task_id;
         });
}

} // namespace dagforge
