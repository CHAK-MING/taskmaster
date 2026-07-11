#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/context.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/config/dag_info_loader.hpp"
#include "dagforge/executor/composite_executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/time.hpp"


#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>

#include <chrono>
#include <filesystem>
#include <format>
#include <print>


namespace dagforge::cli {
namespace {

auto to_executor_config(const TaskConfig &task) -> ExecutorConfig {
  auto config = ExecutorRegistry::instance().build_config(task);
  if (config) {
    return std::move(*config);
  }
  auto fallback =
      ExecutorRegistry::instance().parse_persisted_config(task.executor, "{}");
  return fallback ? std::move(*fallback) : ExecutorConfig{};
}

auto load_dag(const SystemConfig &config, const DAGId &dag_id)
    -> Result<DAGInfo> {
  if (!config.dag_source.directory.empty()) {
    const auto dag_file = std::filesystem::path(config.dag_source.directory) /
                          std::format("{}.toml", dag_id.str());
    if (std::filesystem::exists(dag_file)) {
      return DAGInfoLoader::load_from_file(dag_file.string());
    }
  }

  auto client = open_client_or_print(config.database);
  if (!client) {
    return fail(client.error());
  }
  return (*client)->get_dag(dag_id);
}

} // namespace

auto cmd_test_task(const TestTaskOptions &opts) -> int {
  log::set_output_stderr();
  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    return 1;
  }

  auto dag_res = load_dag(*config_res, DAGId{opts.dag_id});
  if (!dag_res) {
    std::println(stderr, "Error: DAG '{}' not found: {}", opts.dag_id,
                 dag_res.error().message());
    return 1;
  }

  auto *task = dag_res->find_task(TaskId{opts.task_id});
  if (!task) {
    std::println(stderr, "Error: task '{}' not found in DAG '{}'", opts.task_id,
                 opts.dag_id);
    return 1;
  }

  Runtime runtime(config_res->scheduler.shards);
  if (auto r = runtime.start(); !r) {
    std::println(stderr, "Error: failed to start runtime: {}",
                 r.error().message());
    return 1;
  }

  auto executor = create_composite_executor(runtime);
  const auto now = std::chrono::system_clock::now();
  const auto iid = std::format("test_{}_{}_{}", opts.dag_id, opts.task_id,
                               util::to_unix_millis(now));

  auto fut = boost::asio::co_spawn(
      runtime.shard(0).ctx(),
      execute_async(runtime, *executor,
                    ExecutorRequest{.instance_id = InstanceId{iid},
                                    .command = task->command,
                                    .working_dir = task->working_dir,
                                    .execution_timeout = task->execution_timeout,
                                    .config = to_executor_config(*task),
                                    .memory_resource = {}}),
      boost::asio::use_future);

  Result<ExecutorResult> result_res = fail(Error::Unknown);
  try {
    result_res = fut.get();
  } catch (const std::exception &e) {
    runtime.stop();
    std::println(stderr, "Error: task test execution failed: {}", e.what());
    return 1;
  }
  runtime.stop();
  if (!result_res) {
    std::println(stderr, "Error: task test execution failed: {}",
                 result_res.error().message());
    return 1;
  }
  auto &result = *result_res;

  if (opts.json) {
    JsonValue out{
        {"dag_id", opts.dag_id},
        {"task_id", opts.task_id},
        {"instance_id", iid},
        {"exit_code", result.exit_code},
        {"timed_out", result.timed_out},
        {"status", result.exit_code == 0 ? "success" : "failed"},
        {"stdout", std::string(result.stdout_output)},
        {"stderr", std::string(result.stderr_output)},
        {"error", std::string(result.error)},
    };
    std::println("{}", dump_json(out));
    return result.exit_code == 0 ? 0 : 1;
  }

  std::println("Task test finished:");
  std::println("  DAG:        {}", opts.dag_id);
  std::println("  Task:       {}", opts.task_id);
  std::println("  Instance:   {}", iid);
  std::println("  Exit code:  {}", result.exit_code);
  std::println("  Timed out:  {}", result.timed_out ? "yes" : "no");
  if (!result.error.empty()) {
    std::println("  Error:      {}", std::string(result.error));
  }
  if (!result.stdout_output.empty()) {
    std::println("\nstdout:\n{}", std::string(result.stdout_output));
  }
  if (!result.stderr_output.empty()) {
    std::println("\nstderr:\n{}", std::string(result.stderr_output));
  }

  return result.exit_code == 0 ? 0 : 1;
}

} // namespace dagforge::cli
