#include "taskmaster/cli/commands.hpp"

#include <CLI/CLI.hpp>

#include <cstdlib>

int main(int argc, char *argv[]) {
  CLI::App app{"TaskMaster", "A DAG-based Task Scheduler"};
  app.require_subcommand(1);
  app.footer("\nExamples:\n"
             "  taskmaster serve -c system_config.yaml\n"
             "  taskmaster run --db taskmaster.db data_pipeline\n"
             "  taskmaster list --db taskmaster.db\n"
             "  taskmaster validate -c system_config.yaml\n"
             "  taskmaster status --db taskmaster.db my_dag");

  taskmaster::cli::ServeOptions serve_opts;
  taskmaster::cli::RunOptions run_opts;
  taskmaster::cli::ListOptions list_opts;
  taskmaster::cli::ValidateOptions validate_opts;
  taskmaster::cli::StatusOptions status_opts;

  auto *serve = app.add_subcommand("serve", "Start TaskMaster service");
  serve->add_option("-c,--config", serve_opts.config_file, "System config file")
      ->check(CLI::ExistingFile)
      ->required();
  serve->add_flag("--no-api", serve_opts.no_api, "Disable REST API");
  serve->add_option(
      "--log-file", serve_opts.log_file,
      "Log file path (required for --daemon unless set in config)");
  serve->add_flag("-d,--daemon", serve_opts.daemon, "Run as daemon");
  serve->callback([&]() { std::exit(taskmaster::cli::cmd_serve(serve_opts)); });

  auto *run = app.add_subcommand("run", "Trigger a DAG run");
  run->add_option("--db", run_opts.db_file, "Database file")
      ->check(CLI::ExistingFile)
      ->required();
  run->add_option("dag_id", run_opts.dag_id, "DAG ID")->required();
  run->callback([&]() { std::exit(taskmaster::cli::cmd_run(run_opts)); });

  auto *list = app.add_subcommand("list", "List all DAGs");
  list->add_option("--db", list_opts.db_file, "Database file")
      ->check(CLI::ExistingFile)
      ->required();
  list->callback([&]() { std::exit(taskmaster::cli::cmd_list(list_opts)); });

  auto *validate = app.add_subcommand("validate", "Validate DAG files");
  validate
      ->add_option("-c,--config", validate_opts.config_file,
                   "System config file")
      ->check(CLI::ExistingFile)
      ->required();
  validate->callback(
      [&]() { std::exit(taskmaster::cli::cmd_validate(validate_opts)); });

  auto *status = app.add_subcommand("status", "Show DAG or run status");
  status->add_option("--db", status_opts.db_file, "Database file")
      ->check(CLI::ExistingFile)
      ->required();
  status->add_option("dag_id", status_opts.dag_id, "DAG ID")->required();
  status->add_option("--run", status_opts.dag_run_id, "Specific run ID");
  status->callback(
      [&]() { std::exit(taskmaster::cli::cmd_status(status_opts)); });

  CLI11_PARSE(app, argc, argv);
  return 0;
}
