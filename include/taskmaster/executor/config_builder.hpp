#pragma once

#include "taskmaster/dag/dag.hpp"
#include "taskmaster/executor/executor.hpp"

#include <vector>

namespace taskmaster {

struct DAGInfo;

class ExecutorConfigBuilder {
public:
  [[nodiscard]] static auto build(const DAGInfo& dag_info, const DAG& graph)
      -> std::vector<ExecutorConfig>;
};

}  // namespace taskmaster
