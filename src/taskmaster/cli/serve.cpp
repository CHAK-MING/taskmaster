#include "taskmaster/app/application.hpp"
#include "taskmaster/cli/commands.hpp"
#include "taskmaster/config/config.hpp"
#include "taskmaster/util/daemon.hpp"
#include "taskmaster/util/log.hpp"

#include <print>

namespace taskmaster::cli {

auto cmd_serve(const ServeOptions &opts) -> int {
  auto result = ConfigLoader::load_from_file(opts.config_file);
  if (!result) {
    std::println(stderr, "Error: {}", result.error().message());
    return 1;
  }
  auto config = std::move(*result);

  config.api.enabled = !opts.no_api;

  const auto log_file = opts.log_file.value_or(config.scheduler.log_file);
  if (opts.daemon && log_file.empty()) {
    std::println(
        stderr,
        "Error: --daemon requires log_file (set in config or --log-file)");
    return 1;
  }
  if (!log_file.empty() && !log::set_output_file(log_file)) {
    std::println(stderr, "Error: Failed to open log file: {}", log_file);
    return 1;
  }

  if (opts.daemon && !daemonize()) {
    std::println(stderr, "Error: Failed to daemonize");
    return 1;
  }

  log::set_level(config.scheduler.log_level);
  log::start();

  Application app(std::move(config));

  if (auto r = app.init(); !r.has_value()) {
    log::error("Initialization failed: {}", r.error().message());
    return 1;
  }

  setup_signal_handlers();

  if (auto r = app.recover_from_crash(); !r.has_value()) {
    log::warn("Recovery failed: {}", r.error().message());
  }

  const auto &cfg = app.config();
  if (opts.no_api) {
    log::info("TaskMaster starting (scheduler only)...");
  } else {
    log::info("TaskMaster starting on {}:{}...", cfg.api.host, cfg.api.port);
  }

  if (auto r = app.start(); !r.has_value()) {
    log::error("Failed to start: {}", r.error().message());
    return 1;
  }

  wait_for_shutdown();
  app.stop();

  log::info("TaskMaster stopped.");
  log::stop();
  return 0;
}

} // namespace taskmaster::cli
