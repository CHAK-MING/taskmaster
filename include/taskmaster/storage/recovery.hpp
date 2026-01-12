#pragma once

#include "taskmaster/core/error.hpp"
#include "taskmaster/dag/dag.hpp"
#include "taskmaster/dag/dag_run.hpp"
#include "taskmaster/storage/persistence.hpp"

#include <functional>
#include <string>
#include <string_view>
#include <vector>

namespace taskmaster {

struct RecoveryResult {
  std::vector<DAGRunId> recovered_dag_runs;
  std::vector<DAGRunId> failed_dag_runs;
  int tasks_to_retry{0};
};

class Recovery {
public:
  explicit Recovery(Persistence& persistence);

  [[nodiscard]] auto recover(
      std::move_only_function<TaskDAG(DAGRunId)> dag_provider,
      std::move_only_function<void(DAGRunId, DAGRun&)> on_recovered)
      -> Result<RecoveryResult>;

  static auto mark_running_as_failed(Persistence& persistence,
                                     DAGRunId dag_run_id)
      -> Result<void>;

private:
  Persistence& persistence_;
};

}  // namespace taskmaster
