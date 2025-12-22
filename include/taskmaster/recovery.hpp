#pragma once

#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "taskmaster/dag.hpp"
#include "taskmaster/dag_run.hpp"
#include "taskmaster/error.hpp"
#include "taskmaster/persistence.hpp"

namespace taskmaster {

struct RecoveryResult {
  std::vector<std::string> recovered_dag_runs;
  std::vector<std::string> failed_dag_runs;
  int tasks_to_retry{0};
};

class Recovery {
public:
  explicit Recovery(Persistence &persistence);

  [[nodiscard]] auto recover(
      std::move_only_function<TaskDAG(const std::string &)> dag_provider,
      std::move_only_function<void(const std::string &, DAGRun &)> on_recovered)
      -> Result<RecoveryResult>;

  static auto mark_running_as_failed(Persistence &persistence,
                                     std::string_view dag_run_id)
      -> Result<void>;

private:
  Persistence &persistence_;
};

} // namespace taskmaster
