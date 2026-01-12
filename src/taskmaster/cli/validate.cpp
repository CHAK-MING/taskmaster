#include "taskmaster/cli/commands.hpp"
#include "taskmaster/config/config.hpp"
#include "taskmaster/config/dag_file_loader.hpp"
#include "taskmaster/dag/dag.hpp"

#include <filesystem>
#include <print>

namespace taskmaster::cli {

auto cmd_validate(const ValidateOptions& opts) -> int {
  auto result = ConfigLoader::load_from_file(opts.config_file);
  if (!result) {
    std::println(stderr, "Error: {}", result.error().message());
    return 1;
  }
  const auto& config = *result;

  if (config.dag_source.directory.empty()) {
    std::println(stderr, "Error: DAG directory not configured");
    return 1;
  }

  const auto& dir = config.dag_source.directory;
  std::println("Validating DAG files in {}...\n", dir);

  if (!std::filesystem::exists(dir)) {
    std::println(stderr, "Error: Directory does not exist: {}", dir);
    return 1;
  }

  int valid_count = 0;
  int invalid_count = 0;
  int total_files = 0;

  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (!entry.is_regular_file()) continue;

    auto ext = entry.path().extension().string();
    if (ext != ".yaml" && ext != ".yml") continue;

    total_files++;
    auto dag_id = entry.path().stem().string();

    auto def_result = DAGDefinitionLoader::load_from_file(entry.path().string());
    if (!def_result) {
      std::println("\u2717 {}", dag_id);
      invalid_count++;
      continue;
    }

    const auto& def = *def_result;
    if (def.tasks.empty()) {
      std::println("\u2717 {} - No tasks defined", dag_id);
      invalid_count++;
      continue;
    }

    DAG dag;
    for (const auto& task : def.tasks) {
      dag.add_node(task.task_id);
    }

    bool has_error = false;
    for (const auto& task : def.tasks) {
      for (const auto& dep : task.dependencies) {
        if (auto r = dag.add_edge(dep, task.task_id); !r) {
          std::println("\u2717 {} - Invalid dependency: {} -> {}",
                       dag_id, dep, task.task_id);
          has_error = true;
          break;
        }
      }
      if (has_error) break;
    }

    if (has_error) {
      invalid_count++;
      continue;
    }

    if (auto r = dag.is_valid(); !r) {
      std::println("\u2717 {} - {}", dag_id, r.error().message());
      invalid_count++;
      continue;
    }

    std::println("\u2713 {} - Valid ({} tasks)", dag_id, def.tasks.size());
    valid_count++;
  }

  std::println("\nSummary: {} valid, {} invalid out of {} DAG files",
               valid_count, invalid_count, total_files);

  return invalid_count > 0 ? 1 : 0;
}

}  // namespace taskmaster::cli
