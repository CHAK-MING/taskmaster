#include "taskmaster/cli/commands.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/dag/dag_manager.hpp"

#include <print>

namespace taskmaster::cli {

auto cmd_run(const RunOptions& opts) -> int {
  Persistence db(opts.db_file);

  if (auto r = db.open(); !r) {
    std::println(stderr, "Error: Failed to open database: {}", r.error().message());
    return 1;
  }

  auto dag_result = db.get_dag(DAGId{opts.dag_id});
  if (!dag_result) {
    std::println(stderr, "Error: DAG not found: {}", opts.dag_id);
    return 1;
  }

  std::println("DAG '{}' found.", opts.dag_id);
  std::println("To trigger this DAG:");
  std::println("  1. Start server: taskmaster serve -c system_config.yaml");
  std::println("  2. Then run:     curl -X POST http://localhost:8888/api/dags/{}/trigger", opts.dag_id);
  return 0;
}

}
