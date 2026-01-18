#include "taskmaster/cli/commands.hpp"
#include "taskmaster/storage/persistence.hpp"
#include "taskmaster/dag/dag_manager.hpp"

#include <chrono>
#include <format>
#include <print>

namespace taskmaster::cli {

namespace {
auto state_str(DAGRunState state) -> std::string_view {
  switch (state) {
    case DAGRunState::Running: return "running";
    case DAGRunState::Success: return "success";
    case DAGRunState::Failed: return "failed";
  }
  return "unknown";
}
}

auto cmd_status(const StatusOptions& opts) -> int {
  Persistence db(opts.db_file);

  if (auto r = db.open(); !r.has_value()) {
    std::println(stderr, "Error: Failed to open database: {}", r.error().message());
    return 1;
  }

  if (!opts.dag_run_id.empty()) {
    auto run_result = db.get_run_history(DAGRunId{opts.dag_run_id});
    if (!run_result) {
      std::println(stderr, "Error: Run not found: {}", opts.dag_run_id);
      return 1;
    }
    const auto& e = *run_result;
    std::println("Run:     {}", e.dag_run_id);
    std::println("DAG:     {}", e.dag_id);
    std::println("Status:  {}", state_str(e.state));
    std::println("Trigger: {}", to_string_view(e.trigger_type));
    if (e.started_at > 0) {
      auto t = std::chrono::system_clock::from_time_t(e.started_at / 1000);
      std::println("Started: {:%Y-%m-%d %H:%M:%S}", t);
    }
    if (e.finished_at > 0) {
      auto t = std::chrono::system_clock::from_time_t(e.finished_at / 1000);
      std::println("Finished: {:%Y-%m-%d %H:%M:%S}", t);
      std::println("Duration: {}s", (e.finished_at - e.started_at) / 1000);
    }
    return 0;
  }

  std::optional<DAGId> filter;
  if (!opts.dag_id.empty()) {
    filter = DAGId{opts.dag_id};
  }

  auto list_result = db.list_run_history(filter, 20);
  if (!list_result) {
    std::println(stderr, "Error: {}", list_result.error().message());
    return 1;
  }

  const auto& runs = *list_result;
  if (runs.empty()) {
    std::println("No runs found.");
    return 0;
  }

  std::println("{:<36} {:<15} {:<12} {:<20}", "RUN_ID", "DAG", "STATUS", "STARTED");
  for (const auto& run : runs) {
    std::string started = "-";
    if (run.started_at > 0) {
      auto t = std::chrono::system_clock::from_time_t(run.started_at / 1000);
      started = std::format("{:%Y-%m-%d %H:%M}", t);
    }
    std::println("{:<36} {:<15} {:<12} {:<20}",
                 std::string(run.dag_run_id),
                 std::string(run.dag_id),
                 state_str(run.state),
                 started);
  }
  return 0;
}

}  // namespace taskmaster::cli
