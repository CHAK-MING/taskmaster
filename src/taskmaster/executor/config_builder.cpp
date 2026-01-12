#include "taskmaster/executor/config_builder.hpp"

#include "taskmaster/dag/dag_manager.hpp"

namespace taskmaster {

auto ExecutorConfigBuilder::build(const DAGInfo& dag_info, const DAG& graph)
    -> std::vector<ExecutorConfig> {
  std::vector<ExecutorConfig> cfgs(dag_info.tasks.size());

  for (const auto& task : dag_info.tasks) {
    NodeIndex idx = graph.get_index(task.task_id);
    if (idx != kInvalidNode && idx < cfgs.size()) {
      ShellExecutorConfig exec;
      exec.command = task.command;
      exec.working_dir = task.working_dir;
      exec.timeout = task.timeout;
      cfgs[idx] = exec;
    }
  }

  return cfgs;
}

}  // namespace taskmaster
