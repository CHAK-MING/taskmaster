#include "taskmaster/executor/config_builder.hpp"

#include "taskmaster/dag/dag_manager.hpp"

namespace taskmaster {

auto ExecutorConfigBuilder::build(const DAGInfo &dag_info, const DAG &graph)
    -> std::vector<ExecutorConfig> {
  std::vector<ExecutorConfig> cfgs(dag_info.tasks.size());

  for (const auto &task : dag_info.tasks) {
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
        if (auto *docker_cfg =
                std::get_if<DockerTaskConfig>(&task.executor_config)) {
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
      case ExecutorType::Sensor: {
        SensorExecutorConfig exec;
        if (auto *sensor_cfg =
                std::get_if<SensorTaskConfig>(&task.executor_config)) {
          exec.type = sensor_cfg->type;
          exec.target = sensor_cfg->target;
          exec.poke_interval = sensor_cfg->poke_interval;
          exec.timeout = sensor_cfg->sensor_timeout;
          exec.soft_fail = sensor_cfg->soft_fail;
          exec.expected_status = sensor_cfg->expected_status;
          exec.http_method = sensor_cfg->http_method;
        }
        cfgs[idx] = exec;
        break;
      }
      }
    }
  }

  return cfgs;
}

} // namespace taskmaster
