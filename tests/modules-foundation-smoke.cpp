#include <cstdint>
#include <span>

import dagforge.foundation;

auto main() -> int {
  auto result = dagforge::ok(42);
  dagforge::metrics::Counter counter;
  counter.inc();

  if (!result || *result != 42) {
    return 1;
  }
  if (counter.load() != 1U) {
    return 2;
  }
  dagforge::detail::HttpMetricsRegistry http_metrics;
  auto route = http_metrics.register_route(
      dagforge::http::HttpMethod::GET, "/metrics",
      std::span<const std::uint64_t>{});
  route.record(dagforge::http::HttpStatus::Ok, 1000);
  if (http_metrics.request_counts().empty()) {
    return 27;
  }
  if (dagforge::timing::kShutdownPollInterval.count() <= 0) {
    return 3;
  }
  auto parsed = dagforge::util::parse_int<int>("7");
  if (!parsed || *parsed != 7) {
    return 4;
  }
  dagforge::DAGId dag_id{"module"};
  if (dag_id.empty() || dag_id.size() != 6) {
    return 5;
  }
  auto io_ec = dagforge::io::make_error_code(dagforge::io::IoError::TimedOut);
  const char *io_name = io_ec.category().name();
  if (io_name == nullptr || io_name[0] != 'd' || io_name[1] != 'a') {
    return 6;
  }
  dagforge::SystemConfig config;
  if (config.database.port != 3306 || config.api.port != 8888) {
    return 7;
  }
  dagforge::TaskConfig task{};
  task.task_id = dagforge::TaskId{"task-a"};
  task.command = "echo ok";
  if (task.executor != dagforge::ExecutorType::Shell ||
      task.execution_timeout.count() !=
          dagforge::task_defaults::kExecutionTimeout.count()) {
    return 18;
  }
  dagforge::XComPushConfig push{.key = "result", .regex_pattern = "ok"};
  if (!push.compile_regex()) {
    return 8;
  }
  dagforge::ShellExecutorConfig shell_cfg{};
  dagforge::ExecutorConfig exec_cfg{shell_cfg};
  if (exec_cfg.type() != dagforge::ExecutorType::Shell) {
    return 19;
  }
  dagforge::TaskId producer{"producer"};
  dagforge::XComPullConfig pull{
      .ref = dagforge::XComRef{.task_id = producer, .key = "payload"},
      .env_var = "PAYLOAD",
      .required = false,
      .default_value_json = "\"fallback\"",
      .default_value_rendered = "fallback",
      .has_default_value = true,
  };
  if (pull.source_task().empty() || pull.key().empty()) {
    return 16;
  }
  if (!pull.has_default_value || pull.default_value_json.size() != 10 ||
      pull.default_value_rendered.compare("fallback") != 0 ||
      pull.default_value_json.front() != '"' ||
      pull.default_value_json.back() != '"') {
    return 20;
  }
  if (dagforge::to_string_view(dagforge::DAGRunState::Running).size() != 7) {
    return 9;
  }
  dagforge::TaskInstanceInfo ti{};
  if (ti.task_idx != dagforge::kInvalidNode || ti.attempt != 0) {
    return 10;
  }
  if (ti.instance_id.size() != 0 || ti.task_id.size() != 0) {
    return 11;
  }
  if (dagforge::schema::CURRENT_SCHEMA_VERSION < 2) {
    return 12;
  }
  dagforge::orm::RunHistoryEntry run_entry{};
  if (run_entry.state != dagforge::DAGRunState::Running ||
      run_entry.trigger_type != dagforge::TriggerType::Manual) {
    return 29;
  }
  auto cron = dagforge::CronExpr::parse("*/5 * * * *");
  if (!cron || cron->raw().empty()) {
    return 13;
  }
  dagforge::http::QueryParams params{"dag_id=example&limit=10"};
  if (!params.has("dag_id")) {
    return 14;
  }
  auto limit = params.get("limit");
  if (!limit || limit->size() != 2) {
    return 15;
  }
  dagforge::cli::ServeStartOptions serve_opts;
  serve_opts.config_file = "system_config.toml";
  if (serve_opts.config_file.empty()) {
    return 28;
  }
  dagforge::XComCache xcom_cache;
  xcom_cache.set(dagforge::DAGRunId{"run-1"}, dagforge::TaskId{"task-1"},
                 "payload", R"("hello-xcom")");
  auto cached_xcom =
      xcom_cache.get(dagforge::DAGRunId{"run-1"}, dagforge::TaskId{"task-1"},
                     "payload");
  if (!cached_xcom || cached_xcom->get().compare(R"("hello-xcom")") != 0) {
    return 30;
  }
  return 0;
}
