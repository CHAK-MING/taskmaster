#include "dagforge/app/application.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <CLI/CLI.hpp>
#include <boost/url/parse.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <format>
#include <optional>
#include <print>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>

namespace {

std::atomic<bool> g_shutdown_requested{false};

extern "C" void handle_signal(int) {
  g_shutdown_requested.store(true, std::memory_order_release);
}

[[nodiscard]] auto read_text_file(const std::string &path)
    -> dagforge::Result<std::string> {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return dagforge::fail(dagforge::Error::FileOpenFailed);
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  if (!input.good() && !input.eof()) {
    return dagforge::fail(dagforge::Error::FileOpenFailed);
  }
  return dagforge::ok(buffer.str());
}

[[nodiscard]] auto load_plan(const std::string &path)
    -> dagforge::Result<dagforge::workflow::WorkflowPlan> {
  auto text = read_text_file(path);
  if (!text) {
    return dagforge::fail(text.error());
  }
  return dagforge::workflow::WorkflowPlanLoader::from_json(*text);
}

[[nodiscard]] auto terminal(dagforge::workflow::RunState state) noexcept
    -> bool {
  return dagforge::workflow::is_terminal(state);
}

[[nodiscard]] auto offline_validation_config(
    const dagforge::workflow::WorkflowPlan &plan)
    -> dagforge::config::SystemConfig {
  dagforge::config::SystemConfig config;
  config.api.enabled = false;
  config.admission.allow_unlisted_executors = true;
  config.executors.command.policy.allow_unlisted_programs = true;
  config.executors.command.policy.allow_unlisted_environment = true;
  config.executors.command.policy.require_trusted_programs = false;
  config.executors.command.minijail.require_trusted_files = false;
  config.executors.http.enabled = true;
  config.executors.http.egress.allow_plaintext = true;
  config.executors.http.egress.deny_private_networks = false;

  for (const auto &node : plan.nodes) {
    if (node.executor != "http" || !node.config.is_object()) {
      continue;
    }
    const auto &object = node.config.get_object();
    const auto url = object.find("url");
    if (url == object.end() || !url->second.is_string()) {
      continue;
    }
    const auto url_text = url->second.as<std::string>();
    const auto parsed = boost::urls::parse_uri(url_text);
    if (!parsed || parsed->scheme().empty() || parsed->host().empty()) {
      continue;
    }
    std::string origin;
    origin.reserve(parsed->scheme().size() + parsed->encoded_authority().size() +
                   3);
    origin.append(parsed->scheme().data(), parsed->scheme().size());
    origin.append("://");
    origin.append(parsed->encoded_authority().data(),
                  parsed->encoded_authority().size());
    config.executors.http.egress.allowed_origins.push_back(std::move(origin));
  }
  return config;
}

auto run_serve(const std::string &config_path) -> int {
  dagforge::Application app;
  auto loaded = app.load_config(config_path);
  if (!loaded) {
    std::println(stderr, "Failed to load config: {}",
                 loaded.error().message());
    return 1;
  }
  auto started = app.start();
  if (!started) {
    std::println(stderr, "Failed to start DAGForge: {}",
                 started.error().message());
    return 1;
  }

  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);
  while (!g_shutdown_requested.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  app.stop();
  return 0;
}

auto run_validate(const std::string &config_path,
                  const std::string &plan_path) -> int {
  auto plan = load_plan(plan_path);
  if (!plan) {
    std::println(stderr, "Invalid workflow plan: {}", plan.error().message());
    return 1;
  }
  dagforge::Application app{config_path.empty()
                                ? offline_validation_config(*plan)
                                : dagforge::config::SystemConfig{}};
  if (!config_path.empty()) {
    auto loaded = app.load_config(config_path);
    if (!loaded) {
      std::println(stderr, "Failed to load config: {}",
                   loaded.error().message());
      return 1;
    }
  }
  auto initialized = app.init();
  if (!initialized) {
    std::println(stderr, "Failed to initialize workflow validation: {}",
                 initialized.error().message());
    return 1;
  }
  if (app.workflow_control_plane() == nullptr) {
    std::println(stderr, "Failed to initialize workflow validation components");
    return 1;
  }
  auto compiled =
      app.workflow_control_plane()->register_plan(std::move(*plan));
  if (!compiled) {
    std::println(stderr, "Workflow rejected: {}", compiled.error().message());
    return 1;
  }
  std::println("workflow_id={}", (*compiled)->workflow_id);
  std::println("plan_id={}", (*compiled)->plan_id);
  std::println("digest={}", (*compiled)->digest);
  std::println("nodes={}", (*compiled)->nodes.size());
  return 0;
}

auto run_local(const std::string &config_path, const std::string &plan_path,
               const std::string &payload_text, bool wait) -> int {
  dagforge::Application app;
  auto loaded = app.load_config(config_path);
  if (!loaded) {
    std::println(stderr, "Failed to load config: {}",
                 loaded.error().message());
    return 1;
  }
  app.config().api.enabled = false;
  auto initialized = app.init();
  if (!initialized) {
    std::println(stderr, "Failed to initialize: {}",
                 initialized.error().message());
    return 1;
  }
  auto started_app = app.start();
  if (!started_app) {
    std::println(stderr, "Failed to start runtime: {}",
                 started_app.error().message());
    return 1;
  }

  auto plan = load_plan(plan_path);
  if (!plan) {
    std::println(stderr, "Invalid workflow plan: {}", plan.error().message());
    app.stop();
    return 1;
  }
  auto registered = app.workflow_control_plane()->register_plan(std::move(*plan));
  if (!registered) {
    std::println(stderr, "Workflow rejected: {}",
                 registered.error().message());
    app.stop();
    return 1;
  }

  dagforge::workflow::WorkflowValue payload;
  if (!payload_text.empty()) {
    auto parsed = dagforge::parse_json(payload_text);
    payload = parsed ? dagforge::workflow::WorkflowValue{std::move(*parsed)}
                     : dagforge::workflow::WorkflowValue{payload_text};
  }
  auto run = app.workflow_runtime()->start(
      *registered,
      dagforge::workflow::TriggerEnvelope{
          .workflow_id = (*registered)->workflow_id.clone(),
          .source = "cli",
          .event_type = "request",
          .payload = std::move(payload),
          .principal = dagforge::workflow::Principal{.subject = "cli"},
      });
  if (!run) {
    std::println(stderr, "Failed to start workflow: {}",
                 run.error().message());
    app.stop();
    return 1;
  }
  std::println("{}", run->str());

  if (!wait) {
    app.stop();
    return 0;
  }

  for (;;) {
    auto snapshot = dagforge::sync_wait_on_runtime(
        app.runtime(), app.workflow_runtime()->snapshot(*run));
    if (!snapshot) {
      std::println(stderr, "Failed to query workflow: {}",
                   snapshot.error().message());
      app.stop();
      return 1;
    }
    if (terminal((*snapshot)->state)) {
      std::println("{}",
                   dagforge::workflow::to_string_view((*snapshot)->state));
      const auto exit_code =
          (*snapshot)->state == dagforge::workflow::RunState::Succeeded ? 0
                                                                        : 1;
      app.stop();
      return exit_code;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

} // namespace

int main(int argc, char **argv) {
  CLI::App app{"DAGForge 0.4 AI workflow runtime"};
  app.require_subcommand(1);

  std::string serve_config;
  auto *serve = app.add_subcommand("serve", "Run the workflow API service");
  serve->add_option("-c,--config", serve_config, "System config TOML")
      ->required()
      ->check(CLI::ExistingFile);

  std::string validate_plan;
  std::string validate_config;
  auto *validate = app.add_subcommand("validate", "Compile and validate a plan");
  validate->add_option("-c,--config", validate_config,
                       "Optional system config TOML for executor policy")
      ->check(CLI::ExistingFile);
  validate->add_option("-f,--file", validate_plan, "Workflow Plan JSON")
      ->required()
      ->check(CLI::ExistingFile);

  std::string run_config;
  std::string run_plan;
  std::string run_payload;
  bool run_wait = false;
  auto *run = app.add_subcommand("run", "Run a workflow plan locally");
  run->add_option("-c,--config", run_config, "System config TOML")
      ->required()
      ->check(CLI::ExistingFile);
  run->add_option("-f,--file", run_plan, "Workflow Plan JSON")
      ->required()
      ->check(CLI::ExistingFile);
  run->add_option("--payload", run_payload, "JSON or text trigger payload");
  run->add_flag("--wait", run_wait, "Wait for terminal state");

  CLI11_PARSE(app, argc, argv);
  if (*serve) {
    return run_serve(serve_config);
  }
  if (*validate) {
    return run_validate(validate_config, validate_plan);
  }
  return run_local(run_config, run_plan, run_payload, run_wait);
}
