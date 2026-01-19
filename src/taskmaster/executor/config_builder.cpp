#include "taskmaster/executor/config_builder.hpp"

#include "taskmaster/dag/dag_manager.hpp"

namespace taskmaster {

auto ExecutorConfigBuilder::build(const DAGInfo& dag_info, const DAG& graph)
    -> std::vector<ExecutorConfig> {
  std::vector<ExecutorConfig> cfgs(dag_info.tasks.size());

  for (const auto& task : dag_info.tasks) {
    NodeIndex idx = graph.get_index(task.task_id);
    if (idx != kInvalidNode && idx < cfgs.size()) {
      switch (task.executor) {
        case ExecutorType::Shell: {
          ShellExecutorConfig exec;
          exec.command = task.command;
          exec.working_dir = task.working_dir;
          exec.execution_timeout = task.execution_timeout;
          cfgs[idx] = exec;
          break;
        }
        case ExecutorType::Docker: {
          DockerExecutorConfig exec;
          if (auto* docker_cfg = std::get_if<DockerTaskConfig>(&task.executor_config)) {
            exec.image = docker_cfg->image;
            exec.docker_socket = docker_cfg->socket;
            exec.pull_policy = docker_cfg->pull_policy;
          }
          exec.command = task.command;
          exec.working_dir = task.working_dir;
          exec.execution_timeout = task.execution_timeout;
          cfgs[idx] = exec;
          break;
        }
      }
    }
  }

  return cfgs;
}

}  // namespace taskmaster
