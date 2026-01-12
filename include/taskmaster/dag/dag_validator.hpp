#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_manager.hpp"

#include <format>
#include <string>

namespace taskmaster {

class DAGValidator {
public:
  [[nodiscard]] static auto validate(const DAGInfo& info) -> Result<void> {
    std::string error_message;

    DAG temp_dag;
    for (const auto& task : info.tasks) {
      temp_dag.add_node(task.task_id);
    }

    for (const auto& task : info.tasks) {
      for (const auto& dep : task.dependencies) {
        if (auto result = temp_dag.add_edge(dep, task.task_id); !result) {
          error_message += std::format("Invalid dependency: {} -> {}\n",
                                       dep.value(), task.task_id.value());
        }
      }
    }

    if (auto result = temp_dag.is_valid(); !result) {
      error_message += "DAG contains circular dependencies\n";
    }

    return error_message.empty() ? ok() : fail(Error::InvalidArgument);
  }
};

}
