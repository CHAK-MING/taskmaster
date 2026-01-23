#include "taskmaster/app/application.hpp"
#include "taskmaster/cli/commands.hpp"
#include "taskmaster/util/log.hpp"

#include <print>

namespace taskmaster::cli {

auto cmd_run(const RunOptions &opts) -> int {
  log::set_level("info");
  log::start();

  Application app(opts.db_file);

  if (auto r = app.init_db_only(); !r.has_value()) {
    std::println(stderr, "Error: {}", r.error().message());
    log::stop();
    return 1;
  }

  if (auto r = app.start(); !r.has_value()) {
    std::println(stderr, "Error: {}", r.error().message());
    log::stop();
    return 1;
  }

  auto run_id = app.trigger_dag_by_id(DAGId{opts.dag_id}, TriggerType::Manual);
  if (!run_id) {
    std::println(stderr, "Error: Failed to trigger DAG '{}'", opts.dag_id);
    app.stop();
    log::stop();
    return 1;
  }

  std::println("DAG '{}' triggered, run_id: {}", opts.dag_id, run_id->str());

  app.wait_for_completion();
  app.stop();
  log::stop();

  std::println("DAG run completed.");
  return 0;
}

} // namespace taskmaster::cli
