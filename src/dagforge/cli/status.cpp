#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/context.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include <algorithm>
#include <chrono>
#include <format>
#include <print>
#include <unordered_map>
#include <vector>


namespace dagforge::cli {
namespace {

auto resolve_run_id_prefix(ManagementClient &client, const DAGId &dag_id,
                           std::string_view input) -> Result<DAGRunId> {
  if (input.empty()) {
    return fail(Error::InvalidArgument);
  }
  auto runs = client.list_dag_runs(dag_id, 500);
  if (!runs) {
    return fail(runs.error());
  }
  auto matches = *runs | std::views::filter([&](const auto &r) {
    return r.dag_run_id.str().starts_with(input);
  }) | std::views::transform([](const auto &r) {
    return r.dag_run_id.clone();
  }) | std::ranges::to<std::vector>();
  if (matches.empty()) {
    return fail(Error::NotFound);
  }
  if (matches.size() > 1) {
    std::println(stderr, "Error: run_id prefix '{}' is ambiguous ({} matches).",
                 input, matches.size());
    return fail(Error::InvalidArgument);
  }
  return ok(std::move(matches.front()));
}

} // namespace

auto cmd_inspect(const InspectOptions &opts) -> int {
  log::set_output_stderr();
  auto ctx = load_context_or_print(opts.config_file);
  if (!ctx) {
    return 1;
  }
  auto &client = ctx->db();

  // Resolve --latest to the most recent run_id
  std::string resolved_run_id = opts.run_id;
  if ((opts.latest && resolved_run_id.empty()) || resolved_run_id == "latest") {
    auto latest = client.get_latest_run(DAGId{opts.dag_id});
    if (!latest) {
      std::println(stderr, "Error: No runs found for DAG '{}'", opts.dag_id);
      return 1;
    }
    resolved_run_id = latest->dag_run_id.str();
    if (!opts.json) {
      std::println("Latest run: {}", resolved_run_id);
    }
  }

  if (resolved_run_id.empty()) {
    auto list_result = client.list_dag_runs(DAGId{opts.dag_id}, 20);
    if (!list_result) {
      std::println(stderr, "Error: {}", list_result.error().message());
      return 1;
    }

    const auto &runs = *list_result;
    if (runs.empty()) {
      std::println("No runs found for DAG '{}'.", opts.dag_id);
      return 0;
    }

    if (opts.json) {
      JsonValue arr = std::vector<JsonValue>{};
      for (const auto &run : runs) {
        arr.get_array().emplace_back(JsonValue{
            {"dag_run_id", run.dag_run_id.str()},
            {"dag_id", run.dag_id.str()},
            {"state", enum_to_string(run.state)},
            {"trigger_type", enum_to_string(run.trigger_type)},
            {"started_at", fmt::format_timestamp(run.started_at)},
            {"finished_at", fmt::format_timestamp(run.finished_at)},
        });
      }
      std::println("{}", dump_json(arr));
      return 0;
    }

    fmt::Table table({{.header = "RUN_ID", .width = 36},
                      {.header = "STATUS", .width = 10},
                      {.header = "TRIGGER", .width = 10},
                      {.header = "STARTED", .width = 20},
                      {.header = "DURATION", .width = 10}});
    table.print_header();

    for (const auto &run : runs) {
      auto state_sv = to_string_view(run.state);
      table.print_row(
          {run.dag_run_id.str(), fmt::colorize_dag_run_state(state_sv),
           enum_to_string(run.trigger_type),
           fmt::format_timestamp_short(run.started_at),
           fmt::format_duration_seconds(run.started_at, run.finished_at)});
    }
    return 0;
  }

  auto exact_run_id = resolved_run_id;
  auto run_result = client.get_run(DAGRunId{exact_run_id});
  if (!run_result && run_result.error() == make_error_code(Error::NotFound) &&
      !opts.dag_id.empty()) {
    auto resolved =
        resolve_run_id_prefix(client, DAGId{opts.dag_id}, resolved_run_id);
    if (!resolved) {
      std::println(stderr, "Error: Run not found: {}", resolved_run_id);
      return 1;
    }
    exact_run_id = resolved->str();
    run_result = client.get_run(*resolved);
  }
  if (!run_result) {
    std::println(stderr, "Error: Run not found: {}", resolved_run_id);
    return 1;
  }

  const auto &run = *run_result;
  auto state_sv = to_string_view(run.state);

  std::unordered_map<std::int64_t, std::string> task_id_by_rowid;
  std::unordered_map<std::int64_t, std::size_t> task_index_by_rowid;
  if (auto dag_result = client.get_dag(run.dag_id); dag_result) {
    const auto &dag = *dag_result;
    task_id_by_rowid.reserve(dag.tasks.size());
    task_index_by_rowid.reserve(dag.tasks.size());
    for (std::size_t i = 0; i < dag.tasks.size(); ++i) {
      const auto &task = dag.tasks[i];
      task_id_by_rowid.emplace(task.task_rowid, task.task_id.str());
      task_index_by_rowid.emplace(task.task_rowid, i);
    }
  }

  if (opts.json) {
    JsonValue j{
        {"dag_run_id", run.dag_run_id.str()},
        {"dag_id", run.dag_id.str()},
        {"state", std::string(state_sv)},
        {"trigger_type", enum_to_string(run.trigger_type)},
        {"started_at", fmt::format_timestamp(run.started_at)},
        {"finished_at", fmt::format_timestamp(run.finished_at)},
        {"execution_date", fmt::format_timestamp(run.execution_date)},
    };

    if (auto tasks = client.get_task_instances(run.dag_run_id); tasks) {
      JsonValue task_arr = std::vector<JsonValue>{};
      for (const auto &t : *tasks) {
        auto started_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              t.started_at.time_since_epoch())
                              .count();
        auto finished_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                t.finished_at.time_since_epoch())
                .count();
        JsonValue tj{
            {"state", enum_to_string(t.state)},
            {"attempt", static_cast<std::int64_t>(t.attempt)},
            {"exit_code", static_cast<std::int64_t>(t.exit_code)},
            {"started_at", fmt::format_timestamp(started_ms)},
            {"finished_at", fmt::format_timestamp(finished_ms)},
        };
        if (auto it = task_id_by_rowid.find(t.task_rowid);
            it != task_id_by_rowid.end()) {
          tj.get_object().emplace("task_id", it->second);
        }
        if (auto it = task_index_by_rowid.find(t.task_rowid);
            it != task_index_by_rowid.end()) {
          tj.get_object().emplace("task_idx",
                                  static_cast<std::int64_t>(it->second));
        }
        if (!t.error_message.empty()) {
          tj.get_object().emplace("error_message", t.error_message);
        }
        task_arr.get_array().emplace_back(std::move(tj));
      }
      j.get_object().emplace("tasks", std::move(task_arr));
    }

    std::println("{}", dump_json(j));
    return 0;
  }

  std::println("{}", fmt::ansi::bold("Run Details"));
  std::println("  Run:       {}", run.dag_run_id);
  std::println("  DAG:       {}", run.dag_id);
  std::println("  Status:    {}", fmt::colorize_dag_run_state(state_sv));
  std::println("  Trigger:   {}", to_string_view(run.trigger_type));
  std::println("  Started:   {}", fmt::format_timestamp(run.started_at));
  std::println("  Finished:  {}", fmt::format_timestamp(run.finished_at));
  std::println("  Duration:  {}",
               fmt::format_duration_seconds(run.started_at, run.finished_at));
  if (run.execution_date != std::chrono::system_clock::time_point{}) {
    std::println("  Exec Date: {}", fmt::format_timestamp(run.execution_date));
  }

  auto tasks = client.get_task_instances(run.dag_run_id);
  if (tasks && !tasks->empty()) {
    std::println("\n{}", fmt::ansi::bold("Task Instances"));

    fmt::Table table({{.header = "IDX", .width = 5, .right_align = true},
                      {.header = "TASK_ID", .width = 24},
                      {.header = "STATE", .width = 18},
                      {.header = "ATTEMPT", .width = 8, .right_align = true},
                      {.header = "EXIT", .width = 6, .right_align = true},
                      {.header = "STARTED", .width = 16},
                      {.header = "FINISHED", .width = 16},
                      {.header = "DURATION", .width = 12},
                      {.header = "ERROR", .width = 30}});
    table.print_header();

    std::int64_t max_duration_ms = 0;
    if (opts.details) {
      for (const auto &t : *tasks) {
        auto s = std::chrono::duration_cast<std::chrono::milliseconds>(
                     t.started_at.time_since_epoch())
                     .count();
        auto f = std::chrono::duration_cast<std::chrono::milliseconds>(
                     t.finished_at.time_since_epoch())
                     .count();
        if (s > 0 && f > 0) {
          max_duration_ms = std::max(max_duration_ms, f - s);
        }
      }
    }

    for (const auto &t : *tasks) {
      auto started_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            t.started_at.time_since_epoch())
                            .count();
      auto finished_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             t.finished_at.time_since_epoch())
                             .count();
      auto task_state_sv = to_string_view(t.state);
      auto idx_it = task_index_by_rowid.find(t.task_rowid);
      auto id_it = task_id_by_rowid.find(t.task_rowid);
      std::string idx_text = idx_it == task_index_by_rowid.end()
                                 ? "-"
                                 : std::format("{}", idx_it->second);
      std::string task_id_text =
          id_it == task_id_by_rowid.end() ? "unknown" : id_it->second;

      table.print_row({std::move(idx_text), std::move(task_id_text),
                       fmt::colorize_task_state(task_state_sv),
                       std::format("{}", t.attempt),
                       std::format("{}", t.exit_code),
                       fmt::format_timestamp_short(started_ms),
                       fmt::format_timestamp_short(finished_ms),
                       fmt::format_duration_seconds(started_ms, finished_ms),
                       t.error_message.empty() ? "-" : t.error_message});

      if (opts.details && started_ms > 0 && finished_ms > 0 &&
          max_duration_ms > 0) {
        auto dur = finished_ms - started_ms;
        auto fraction =
            static_cast<double>(dur) / static_cast<double>(max_duration_ms);
        std::println("        {} {}ms", fmt::ascii_bar(fraction), dur);
      }
    }
  }

  return 0;
}

} // namespace dagforge::cli
