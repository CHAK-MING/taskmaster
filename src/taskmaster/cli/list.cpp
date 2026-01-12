#include "taskmaster/cli/commands.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/dag/dag_manager.hpp"

#include <print>

namespace taskmaster::cli {

auto cmd_list(const ListOptions& opts) -> int {
  Persistence db(opts.db_file);

  if (auto r = db.open(); !r) {
    std::println(stderr, "Error: Failed to open database: {}", r.error().message());
    return 1;
  }

  auto result = db.list_dags();
  if (!result) {
    std::println(stderr, "Error: {}", result.error().message());
    return 1;
  }

  const auto& dags = *result;
  if (dags.empty()) {
    std::println("No DAGs found.");
    return 0;
  }

  std::println("{:<20} {:<30} {:<15} {:<10}", "DAG_ID", "NAME", "CRON", "TASKS");
  for (const auto& dag : dags) {
    auto tasks_result = db.get_tasks(dag.dag_id);
    std::size_t task_count = tasks_result ? tasks_result->size() : 0;
    std::println("{:<20} {:<30} {:<15} {:<10}",
                 std::string(dag.dag_id),
                 dag.name.substr(0, 28),
                 dag.cron.empty() ? "-" : dag.cron,
                 task_count);
  }
  return 0;
}

}  // namespace taskmaster::cli
