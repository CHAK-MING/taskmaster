#include "taskmaster/app/application.hpp"
#include "taskmaster/storage/config.hpp"
#include "taskmaster/util/log.hpp"

#include <csignal>
#include <cstdlib>
#include <format>
#include <filesystem>
#include <print>
#include <string>
#include <string_view>
#include <thread>

namespace {

std::atomic<bool> g_shutdown_requested{false};

void signal_handler(int) {
  g_shutdown_requested.store(true, std::memory_order_release);
}

void print_usage(const char* prog) {
  std::println("TaskMaster - A DAG-based Task Scheduler");
  std::println("Usage: {} [OPTIONS]", prog);
  std::println("");
  std::println("Modes:");
  std::println("  CLI Mode (default):   Load tasks from config file");
  std::println("  Server Mode:          API-only, manage DAGs via REST API");
  std::println("");
  std::println("Options:");
  std::println("  -c, --config <file>   Config file (YAML or JSON)");
  std::println(
      "  --server              Run in server mode (API only, no config file)");
  std::println("  --port <port>         API server port (default: 8888)");
  std::println("  --host <host>         API server host (default: 127.0.0.1)");
  std::println(
      "  --db <file>           Database file (default: taskmaster.db)");
  std::println("  -d, --daemon          Run as daemon");
  std::println("  -l, --list            List all tasks and exit");
  std::println("  -t, --trigger <dag>   Trigger a DAG run and exit");
  std::println("  -v, --version         Show version and exit");
  std::println("  -h, --help            Show this help message");
  std::println("");
  std::println("Examples:");
  std::println("  {} -c config.yaml              # CLI mode", prog);
  std::println("  {} -c config.yaml --port 8888  # CLI + API", prog);
  std::println("  {} --server --port 8888        # Server mode", prog);
}

void print_version() {
  std::println("TaskMaster v0.1.0");
}

void setup_logging(const std::string& log_level) {
  taskmaster::log::set_level(log_level);
  taskmaster::log::start();
}

auto daemonize() -> bool {
  pid_t pid = fork();
  if (pid < 0)
    return false;
  if (pid > 0)
    _exit(0);

  if (setsid() < 0)
    return false;

  pid = fork();
  if (pid < 0)
    return false;
  if (pid > 0)
    _exit(0);

  freopen("/dev/null", "r", stdin);
  freopen("/dev/null", "w", stdout);
  freopen("/dev/null", "w", stderr);

  return true;
}

struct Options {
  std::string config_file;
  std::string db_file = "taskmaster.db";
  std::string trigger_dag;
  std::string host = "127.0.0.1";
  std::uint16_t port = 8888;
  bool server_mode = false;
  bool daemon = false;
  bool list_tasks = false;
};

auto parse_args(int argc, char* argv[]) -> Options {
  Options opts;

  for (int i = 1; i < argc; ++i) {
    std::string_view arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      std::exit(0);
    } else if (arg == "-v" || arg == "--version") {
      print_version();
      std::exit(0);
    } else if (arg == "-c" || arg == "--config") {
      if (++i >= argc) {
        std::println(stderr, "Error: --config requires an argument");
        std::exit(1);
      }
      opts.config_file = argv[i];
    } else if (arg == "--server") {
      opts.server_mode = true;
    } else if (arg == "--port") {
      if (++i >= argc) {
        std::println(stderr, "Error: --port requires an argument");
        std::exit(1);
      }
      opts.port = static_cast<std::uint16_t>(std::stoi(argv[i]));
    } else if (arg == "--host") {
      if (++i >= argc) {
        std::println(stderr, "Error: --host requires an argument");
        std::exit(1);
      }
      opts.host = argv[i];
    } else if (arg == "--db") {
      if (++i >= argc) {
        std::println(stderr, "Error: --db requires an argument");
        std::exit(1);
      }
      opts.db_file = argv[i];
    } else if (arg == "-d" || arg == "--daemon") {
      opts.daemon = true;
    } else if (arg == "-l" || arg == "--list") {
      opts.list_tasks = true;
    } else if (arg == "-t" || arg == "--trigger") {
      if (++i >= argc) {
        std::println(stderr, "Error: --trigger requires an argument");
        std::exit(1);
      }
      opts.trigger_dag = argv[i];
    } else {
      std::println(stderr, "Unknown option: {}", arg);
      print_usage(argv[0]);
      std::exit(1);
    }
  }

  return opts;
}

auto run_cli_mode(const Options& opts) -> int {
  if (opts.config_file.empty()) {
    std::println(stderr,
                 "Error: Config file required in CLI mode. Use -c <file>");
    return 1;
  }

  if (!std::filesystem::exists(opts.config_file)) {
    std::println(stderr, "Error: Config file not found: {}", opts.config_file);
    return 1;
  }

  taskmaster::Application app(opts.db_file);

  auto result = app.load_config(opts.config_file);
  if (!result) {
    std::println(stderr, "Error: Failed to load config: {}",
                 result.error().message());
    return 1;
  }

  if (opts.port != 8888 || opts.host != "127.0.0.1") {
    app.config().api.enabled = true;
    app.config().api.port = opts.port;
    app.config().api.host = opts.host;
  }

  setup_logging(app.config().scheduler.log_level);

  if (opts.list_tasks) {
    app.list_tasks();
    return 0;
  }

  if (!opts.trigger_dag.empty()) {
    app.start();
    app.trigger_dag(opts.trigger_dag);
    taskmaster::log::info("Triggered DAG: {}", opts.trigger_dag);
    app.wait_for_completion();
    app.stop();
    return 0;
  }

  if (opts.daemon && !daemonize()) {
    std::println(stderr, "Error: Failed to daemonize");
    return 1;
  }

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  if (auto r = app.recover_from_crash(); !r) {
    taskmaster::log::warn("Recovery failed: {}", r.error().message());
  }

  taskmaster::log::info("TaskMaster starting in CLI mode...");
  app.start();

  while (app.is_running() &&
         !g_shutdown_requested.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  if (g_shutdown_requested.load(std::memory_order_acquire)) {
    taskmaster::log::info("Received shutdown signal, stopping...");
  }

  app.stop();
  taskmaster::log::info("TaskMaster stopped.");
  taskmaster::log::stop();
  return 0;
}

auto run_server_mode(const Options& opts) -> int {
  setup_logging("info");

  taskmaster::Application app(opts.db_file);

  app.config().api.enabled = true;
  app.config().api.port = opts.port;
  app.config().api.host = opts.host;

  if (opts.daemon && !daemonize()) {
    std::println(stderr, "Error: Failed to daemonize");
    return 1;
  }

  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  taskmaster::log::info("TaskMaster starting in Server mode on {}:{}...",
                        opts.host, opts.port);
  app.start();

  while (app.is_running() &&
         !g_shutdown_requested.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  if (g_shutdown_requested.load(std::memory_order_acquire)) {
    taskmaster::log::info("Received shutdown signal, stopping...");
  }

  app.stop();
  taskmaster::log::info("TaskMaster stopped.");
  return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
  auto opts = parse_args(argc, argv);

  if (opts.server_mode) {
    return run_server_mode(opts);
  }
  return run_cli_mode(opts);
}
