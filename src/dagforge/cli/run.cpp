#include "dagforge/app/application.hpp"
#include "dagforge/cli/commands.hpp"
#include "dagforge/cli/context.hpp"
#include "dagforge/cli/formatting.hpp"
#include "dagforge/cli/management_client.hpp"
#include "dagforge/client/http/http_client.hpp"
#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/util/url.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <future>
#include <print>
#include <sstream>
#include <thread>


namespace dagforge::cli {

namespace {

template <typename T>
auto run_async(io::IoContext &io, task<T> op) -> T {
  auto fut = boost::asio::co_spawn(io, std::move(op), boost::asio::use_future);
  (void)io.run();
  io.restart();
  return fut.get();
}

auto parse_execution_date_arg(std::string_view value)
    -> std::optional<std::chrono::system_clock::time_point> {
  if (value.empty() || value == "now") {
    return std::chrono::system_clock::now();
  }

  std::chrono::system_clock::time_point tp;
  std::istringstream ss{std::string(value)};

  if (value.size() == 10) {
    std::chrono::year_month_day ymd;
    ss >> std::chrono::parse("%Y-%m-%d", ymd);
    if (ss.fail()) {
      return std::nullopt;
    }
    tp = std::chrono::sys_days{ymd};
  } else {
    ss >> std::chrono::parse("%Y-%m-%dT%H:%M:%SZ", tp);
    if (ss.fail()) {
      return std::nullopt;
    }
  }
  return tp;
}

auto extract_json_string_field(std::string_view body, std::string_view key)
    -> std::optional<std::string> {
  auto key_pos = body.find(std::format("\"{}\"", key));
  if (key_pos == std::string_view::npos) {
    return std::nullopt;
  }

  auto colon = body.find(':', key_pos);
  if (colon == std::string_view::npos) {
    return std::nullopt;
  }

  auto first_quote = body.find('"', colon + 1);
  if (first_quote == std::string_view::npos) {
    return std::nullopt;
  }

  auto second_quote = body.find('"', first_quote + 1);
  if (second_quote == std::string_view::npos) {
    return std::nullopt;
  }

  return std::string(
      body.substr(first_quote + 1, second_quote - first_quote - 1));
}

auto is_terminal(DAGRunState state) -> bool {
  return state == DAGRunState::Success || state == DAGRunState::Failed ||
         state == DAGRunState::Skipped || state == DAGRunState::Cancelled;
}

auto trigger_via_running_service(const SystemConfig &config,
                                 const TriggerOptions &opts)
    -> std::optional<int> {
  if (!config.api.enabled || opts.no_api) {
    return std::nullopt;
  }

  io::IoContext io;
  auto client_res = run_async(
      io, http::HttpClient::connect_tcp(io, config.api.host, config.api.port));
  if (!client_res) {
    return std::nullopt;
  }

  auto &client = *client_res;
  auto path =
      std::format("/api/dags/{}/trigger", util::url_encode(opts.dag_id));
  auto resp_res = run_async(io, client->post_json(path, "{}"));
  if (!resp_res) {
    return std::nullopt;
  }
  auto &resp = *resp_res;
  if (resp.status != http::HttpStatus::Created &&
      resp.status != http::HttpStatus::Ok) {
    const std::string body(resp.body.begin(), resp.body.end());
    const auto api_error =
        extract_json_string_field(body, "error").value_or(body);
    std::println(stderr, "Error: API trigger failed (HTTP {}): {}",
                 static_cast<int>(resp.status), api_error);
    if (resp.status == http::HttpStatus::BadRequest) {
      std::println(stderr, "Hint: verify DAG ID '{}' and request format.",
                   opts.dag_id);
      std::println(
          stderr,
          "Hint: run `dagforge list dags -c {}` to check available DAGs.",
          opts.config_file);
    } else if (resp.status == http::HttpStatus::NotFound) {
      std::println(stderr,
                   "Hint: DAG '{}' was not found on the running service.",
                   opts.dag_id);
    } else if (resp.status == http::HttpStatus::ServiceUnavailable) {
      std::println(stderr, "Hint: API is up but not ready; retry shortly.");
    }
    return 1;
  }

  const std::string body(resp.body.begin(), resp.body.end());
  auto run_id = extract_json_string_field(body, "dag_run_id");
  if (!run_id.has_value()) {
    std::println(stderr, "Error: trigger API response missing dag_run_id: {}",
                 body);
    return 1;
  }

  if (opts.json) {
    std::println("{}", body);
  } else {
    std::println("{} DAG '{}' triggered, run_id: {}",
                 fmt::ansi::green("\u2713"), opts.dag_id, *run_id);
    std::println("RUN_ID: {}", *run_id);
  }

  if (!opts.wait) {
    return 0;
  }

  if (!opts.json) {
    std::println("Waiting for completion...");
  }

  ManagementClient management(config.database);
  if (auto open_res = management.open(); !open_res) {
    std::println(stderr, "Error: {}", open_res.error().message());
    return 1;
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::minutes(60);
  while (std::chrono::steady_clock::now() < deadline) {
    auto run_res = management.get_run(DAGRunId{*run_id});
    if (run_res && is_terminal(run_res->state)) {
      auto state_str = enum_to_string(run_res->state);
      if (opts.json) {
        JsonValue result{{"dag_id", opts.dag_id},
                         {"dag_run_id", run_id.value()},
                         {"status", state_str}};
        std::println("{}", dump_json(result));
      } else {
        std::println("DAG run completed: {}",
                     fmt::colorize_dag_run_state(state_str));
      }
      return run_res->state == DAGRunState::Success ? 0 : 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  std::println(stderr, "Error: timeout waiting for DAG run '{}' completion",
               run_id.value());
  return 1;
}

} // namespace

auto cmd_trigger(const TriggerOptions &opts) -> int {
  log::set_output_stderr();

  auto config_res = load_config_or_print(opts.config_file);
  if (!config_res) {
    log::stop();
    return 1;
  }
  // Trigger CLI should stay concise; service mode owns verbose runtime logs.
  log::set_level(log::Level::Warn);

  if (auto remote_rc = trigger_via_running_service(*config_res, opts);
      remote_rc.has_value()) {
    log::stop();
    return *remote_rc;
  }

  if (!opts.json) {
    if (opts.no_api) {
      std::println("API path disabled by --no-api; using local one-shot "
                   "execution mode.");
    } else if (config_res->api.enabled) {
      std::println("API enabled in config but no running service at {}:{}; "
                   "falling back to local one-shot execution mode.",
                   config_res->api.host, config_res->api.port);
      std::println(
          "Hint: run `dagforge serve start -c {}` for persistent service mode.",
          opts.config_file);
    }
  }

  Application app(std::move(*config_res));
  app.config().api.enabled = false;

  std::optional<std::chrono::system_clock::time_point> execution_date;
  if (!opts.execution_date.empty()) {
    execution_date = parse_execution_date_arg(opts.execution_date);
    if (!execution_date) {
      std::println(stderr,
                   "Error: Invalid execution date '{}', expected 'now', "
                   "YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ",
                   opts.execution_date);
      log::stop();
      return 1;
    }
  }

  if (auto r = app.init(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    log::stop();
    return 1;
  }

  if (auto r = app.start(); !r) {
    std::println(stderr, "Error: {}", r.error().message());
    log::stop();
    return 1;
  }

  auto run_id = app.trigger_run_blocking(DAGId{opts.dag_id},
                                         TriggerType::Manual, execution_date);
  if (!run_id) {
    std::println(stderr, "Error: Failed to trigger DAG '{}'", opts.dag_id);
    app.stop();
    log::stop();
    return 1;
  }

  if (opts.json) {
    JsonValue result{
        {"dag_id", opts.dag_id},
        {"dag_run_id", run_id->str()},
        {"status", "triggered"},
    };
    std::println("{}", dump_json(result));
  } else {
    std::println("{} DAG '{}' triggered, run_id: {}",
                 fmt::ansi::green("\u2713"), opts.dag_id, run_id->value());
    std::println("RUN_ID: {}", run_id->value());
  }

  if (opts.wait) {
    if (!opts.json) {
      std::println("Waiting for completion...");
    }
    app.wait_for_completion();

    auto state = app.get_run_state(*run_id);
    auto state_str = state ? enum_to_string(*state) : "unknown";

    if (opts.json) {
      JsonValue result{
          {"dag_id", opts.dag_id},
          {"dag_run_id", run_id->str()},
          {"status", state_str},
      };
      std::println("{}", dump_json(result));
    } else {
      std::println("DAG run completed: {}",
                   fmt::colorize_dag_run_state(state_str));
      std::println("RUN_ID: {}", run_id->value());
    }

    app.stop();
    log::stop();
    return (state && *state == DAGRunState::Success) ? 0 : 1;
  }

  app.stop();
  log::stop();
  return 0;
}

} // namespace dagforge::cli
