#include "detail/command_line.hpp"

#include "detail/commands.hpp"

#include <CLI/CLI.hpp>
#include <boost/url/url.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace dagforge::cli {
namespace {

constexpr std::string_view kVersion = "dagforge 0.4";

struct IdentifierArgs {
  std::string value;
};

struct PageArgs {
  std::size_t offset{0};
  std::size_t limit{100};
};

struct StartRunArgs {
  std::string workflow_id;
  std::string request_body;
};

struct RepairRunArgs {
  std::string run_id;
  std::string request_body;
};

struct OutputArgs {
  std::string run_id;
  std::string node_id;
  std::string port;
};

struct ArtifactPutArgs {
  std::string file;
  std::string media_type{"application/octet-stream"};
};

using ParsedCommand =
    std::variant<ServeOptions, ValidateOptions, RunOptions, ApiOptions>;

class CommandSelection {
public:
  template <typename Options>
  auto select(Options options) -> void {
    ++count_;
    if (!command_) {
      command_.emplace(std::move(options));
    }
  }

  [[nodiscard]] auto count() const noexcept -> std::size_t { return count_; }

  [[nodiscard]] auto run() const -> int {
    return std::visit(
        [](const auto &options) -> int { return execute(options); }, *command_);
  }

private:
  std::optional<ParsedCommand> command_;
  std::size_t count_{0};
};

[[nodiscard]] auto contains_line_break(std::string_view value) -> bool {
  return value.contains('\r') || value.contains('\n');
}

[[nodiscard]] auto http_method_validator() -> CLI::Validator {
  return CLI::Validator{
      [](std::string &value) -> std::string {
        std::ranges::transform(value, value.begin(), [](unsigned char ch) {
          return static_cast<char>(std::toupper(ch));
        });
        constexpr std::array<std::string_view, 7> methods{
            "GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"};
        if (std::ranges::find(methods, value) == methods.end()) {
          return "method must be GET, POST, PUT, DELETE, PATCH, OPTIONS, or HEAD";
        }
        return {};
      },
      "HTTP method", "HTTP_METHOD"};
}

[[nodiscard]] auto request_path_validator() -> CLI::Validator {
  return CLI::Validator{
      [](std::string &value) -> std::string {
        if (value.empty() || value.front() != '/') {
          return "request path must start with '/'";
        }
        if (contains_line_break(value)) {
          return "request path must not contain line breaks";
        }
        return {};
      },
      "absolute HTTP path", "HTTP_PATH"};
}

[[nodiscard]] auto path_segment_validator() -> CLI::Validator {
  return CLI::Validator{
      [](std::string &value) -> std::string {
        if (value.empty()) {
          return "identifier must not be empty";
        }
        if (value.find_first_of("/?#") != std::string::npos ||
            contains_line_break(value)) {
          return "identifier must be one URL path segment";
        }
        return {};
      },
      "URL path segment", "ID"};
}

[[nodiscard]] auto endpoint_validator() -> CLI::Validator {
  return CLI::Validator{
      [](std::string &value) -> std::string {
        try {
          const boost::urls::url endpoint{value};
          if ((endpoint.scheme() != "http" && endpoint.scheme() != "https") ||
              endpoint.host().empty()) {
            return "endpoint must be an http:// or https:// origin";
          }
          if (endpoint.has_userinfo() || endpoint.has_query() ||
              endpoint.has_fragment() ||
              (!endpoint.encoded_path().empty() &&
               endpoint.encoded_path() != "/")) {
            return "endpoint must contain only scheme, host, and optional port";
          }
        } catch (const boost::system::system_error &) {
          return "endpoint is not a valid URL";
        }
        return {};
      },
      "HTTP(S) origin", "ENDPOINT"};
}

[[nodiscard]] auto header_validator() -> CLI::Validator {
  return CLI::Validator{
      [](std::string &value) -> std::string {
        const auto separator = value.find(':');
        if (separator == std::string::npos || separator == 0 ||
            contains_line_break(value)) {
          return "header must use 'Name: value' without line breaks";
        }
        return {};
      },
      "HTTP header", "HEADER"};
}

auto require_subcommand(CLI::App &command) -> void {
  command.require_subcommand(1, 1);
  command.subcommand_fallthrough(false);
}

auto configure_leaf(CLI::App &command, std::string usage) -> void {
  command.require_subcommand(0, 0);
  command.usage(std::move(usage));
  command.validate_positionals();
}

auto configure_app(CLI::App &app) -> void {
  require_subcommand(app);
  app.set_version_flag("-V,--version", std::string{kVersion});
  app.set_help_all_flag("--help-all", "Show help for every command");
  app.option_defaults()->always_capture_default();
  app.get_formatter()->column_width(30);
  app.get_formatter()->right_column_width(72);
  app.get_formatter()->label("REQUIRED", "required");
  app.footer(
      "Core objects are positional. Local configuration defaults to "
      "system_config.json. Remote commands default to http://127.0.0.1:8888.\n"
      "Use DAGFORGE_CONFIG, DAGFORGE_ENDPOINT, and DAGFORGE_API_TOKEN to avoid "
      "repeating connection options.");
}

auto configure_serve(CLI::App &root, ServeOptions &options,
                     CommandSelection &selection) -> void {
  auto *command =
      root.add_subcommand("serve", "Run the long-lived Workflow HTTP service");
  configure_leaf(*command, "[CONFIG]");
  command->add_option("config", options.config_path,
                      "System configuration JSON")
      ->default_val(options.config_path)
      ->envname("DAGFORGE_CONFIG")
      ->check(CLI::ExistingFile)
      ->type_name("JSON");
  command->footer("Example: dagforge serve");
  command->callback([&] { selection.select(options); });
}

auto configure_validate(CLI::App &root, ValidateOptions &options,
                        CommandSelection &selection) -> void {
  auto *command = root.add_subcommand(
      "validate", "Compile and validate a Workflow Plan without running it");
  configure_leaf(*command, "PLAN [--config CONFIG]");
  command->add_option("plan", options.plan_path, "Workflow Plan JSON")
      ->required()
      ->check(CLI::ExistingFile)
      ->type_name("PLAN");
  command->add_option("-c,--config", options.config_path,
                      "Use server executor and admission policy")
      ->envname("DAGFORGE_CONFIG")
      ->check(CLI::ExistingFile)
      ->type_name("JSON");
  command->footer(
      "Without --config, validation uses an isolated permissive policy.\n"
      "Example: dagforge validate dags/hello_world.json");
  command->callback([&] { selection.select(options); });
}

auto configure_run(CLI::App &root, RunOptions &options,
                   CommandSelection &selection) -> void {
  auto *command = root.add_subcommand(
      "run", "Run one Workflow Plan locally and wait for its terminal state");
  configure_leaf(*command, "PLAN [--config CONFIG]");
  command->add_option("plan", options.plan_path, "Workflow Plan JSON")
      ->required()
      ->check(CLI::ExistingFile)
      ->type_name("PLAN");
  command->add_option("-c,--config", options.config_path,
                      "System configuration JSON")
      ->default_val(options.config_path)
      ->envname("DAGFORGE_CONFIG")
      ->check(CLI::ExistingFile)
      ->type_name("JSON");
  command->footer("Example: dagforge run dags/hello_world.json");
  command->callback([&] { selection.select(options); });
}

auto configure_api_options(CLI::App &command, ApiOptions &options) -> void {
  auto *connection = command.add_option_group("Connection");
  connection->set_help_all_flag("");
  connection
      ->add_option("-e,--endpoint", options.endpoint, "DAGForge API origin")
      ->default_val(options.endpoint)
      ->envname("DAGFORGE_ENDPOINT")
      ->check(endpoint_validator())
      ->type_name("URL");
  connection
      ->add_option("--token", options.bearer_token, "Bearer token")
      ->envname("DAGFORGE_API_TOKEN")
      ->type_name("TOKEN");

  auto *request = command.add_option_group("Request");
  request->set_help_all_flag("");
  request->add_option("-H,--header", options.headers,
                      "Additional request header; repeat as needed")
      ->check(header_validator())
      ->take_all()
      ->type_name("HEADER");

  auto *tls = command.add_option_group("TLS");
  tls->set_help_all_flag("");
  tls->add_option("--tls-min-version", options.tls_min_version,
                  "Minimum TLS version")
      ->default_val(options.tls_min_version)
      ->check(CLI::IsMember({"1.2", "1.3"}))
      ->type_name("VERSION");
  tls->add_option("--ca-file", options.tls_ca_file,
                  "Additional trusted CA bundle")
      ->check(CLI::ExistingFile)
      ->type_name("FILE");
  auto *client_cert =
      tls->add_option("--client-cert", options.tls_client_cert_file,
                      "mTLS client certificate")
          ->check(CLI::ExistingFile)
          ->type_name("FILE");
  auto *client_key =
      tls->add_option("--client-key", options.tls_client_key_file,
                      "mTLS client private key")
          ->check(CLI::ExistingFile)
          ->type_name("FILE");
  client_cert->needs(client_key);
  client_key->needs(client_cert);

  auto *output = command.add_option_group("Output");
  output->set_help_all_flag("");
  output->add_flag("-i,--include", options.include_headers,
                   "Print response status and headers");
  output->add_option("-o,--output", options.output_path,
                     "Write response body to a file")
      ->type_name("FILE");
}

auto configure_api_leaf(CLI::App &command, ApiOptions &options,
                        std::string usage) -> void {
  configure_leaf(command, std::move(usage));
  configure_api_options(command, options);
  command.footer(
      "Connection defaults to http://127.0.0.1:8888. Override it with "
      "--endpoint or DAGFORGE_ENDPOINT.");
}

auto select_api_command(const ApiOptions &common, CommandSelection &selection,
                        std::string method, std::string path,
                        std::string body = {},
                        std::string content_type = {}) -> void {
  auto options = common;
  options.method = std::move(method);
  options.path = std::move(path);
  options.body = std::move(body);
  options.content_type = std::move(content_type);
  selection.select(std::move(options));
}

auto add_identifier(CLI::App &command, std::string name, std::string &value,
                    std::string description) -> void {
  command.add_option(std::move(name), value, std::move(description))
      ->required()
      ->check(path_segment_validator())
      ->type_name("ID");
}

auto add_pagination(CLI::App &command, PageArgs &page) -> void {
  command.add_option("--offset", page.offset, "First result offset")
      ->check(CLI::Range(std::size_t{0},
                        std::numeric_limits<std::size_t>::max()))
      ->type_name("N");
  command.add_option("--limit", page.limit, "Maximum returned results")
      ->check(CLI::Range(std::size_t{1}, std::size_t{1000}))
      ->type_name("N");
}

auto configure_api_system(CLI::App &api, ApiOptions &options,
                          CommandSelection &selection) -> void {
  auto *health = api.add_subcommand("health", "Check service liveness");
  configure_api_leaf(*health, options, "");
  health->callback([&] {
    select_api_command(options, selection, "GET", "/api/health");
  });

  auto *ready = api.add_subcommand("ready", "Check service readiness");
  configure_api_leaf(*ready, options, "");
  ready->callback(
      [&] { select_api_command(options, selection, "GET", "/api/ready"); });

  auto *status = api.add_subcommand("status", "Show service runtime status");
  configure_api_leaf(*status, options, "");
  status->callback([&] {
    select_api_command(options, selection, "GET", "/api/status");
  });

  auto *metrics = api.add_subcommand("metrics", "Read Prometheus metrics");
  configure_api_leaf(*metrics, options, "");
  metrics->callback(
      [&] { select_api_command(options, selection, "GET", "/metrics"); });
}

auto configure_api_plan(CLI::App &api, ApiOptions &options,
                        CommandSelection &selection) -> void {
  auto *plan = api.add_subcommand("plan", "Register and inspect Workflow Plans");
  require_subcommand(*plan);
  plan->footer("Run `dagforge api plan COMMAND --help` for command options.");

  auto add_args = std::make_shared<IdentifierArgs>();
  auto *add = plan->add_subcommand("add", "Register a Workflow Plan");
  add->alias("register");
  configure_api_leaf(*add, options, "PLAN");
  add->add_option("plan", add_args->value, "Workflow Plan JSON")
      ->required()
      ->check(CLI::ExistingFile)
      ->type_name("PLAN");
  add->callback([&, add_args] {
    select_api_command(options, selection, "POST", "/api/v1/workflows/plans",
                       "@" + add_args->value, "application/json");
  });

  auto list_args = std::make_shared<PageArgs>();
  auto *list = plan->add_subcommand("list", "List registered Workflow Plans");
  list->alias("ls");
  configure_api_leaf(*list, options, "[--offset N] [--limit N]");
  add_pagination(*list, *list_args);
  list->callback([&, list_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/workflows/plans?offset=" +
                           std::to_string(list_args->offset) + "&limit=" +
                           std::to_string(list_args->limit));
  });

  auto get_args = std::make_shared<IdentifierArgs>();
  auto *get = plan->add_subcommand("get", "Get one registered Workflow Plan");
  get->alias("show");
  configure_api_leaf(*get, options, "PLAN_ID");
  add_identifier(*get, "plan_id", get_args->value, "Workflow Plan ID");
  get->callback([&, get_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/workflows/plans/" + get_args->value);
  });
}

auto configure_api_run(CLI::App &api, ApiOptions &options,
                       CommandSelection &selection) -> void {
  auto *run = api.add_subcommand("run", "Start and control remote Workflow Runs");
  require_subcommand(*run);
  run->footer("Run `dagforge api run COMMAND --help` for command options.");

  auto start_args = std::make_shared<StartRunArgs>();
  auto *start = run->add_subcommand("start", "Start a remote Workflow Run");
  configure_api_leaf(*start, options, "WORKFLOW_ID [REQUEST]");
  add_identifier(*start, "workflow_id", start_args->workflow_id,
                 "Workflow ID");
  start
      ->add_option(
          "request", start_args->request_body,
          "StartRunRequest JSON, @file, or - for standard input")
      ->type_name("JSON");
  start->callback([&, start_args] {
    select_api_command(
        options, selection, "POST",
        "/api/v1/workflows/" + start_args->workflow_id + "/runs",
        start_args->request_body, "application/json");
  });

  auto get_args = std::make_shared<IdentifierArgs>();
  auto *get = run->add_subcommand("get", "Get a Workflow Run snapshot");
  get->alias("show");
  configure_api_leaf(*get, options, "RUN_ID");
  add_identifier(*get, "run_id", get_args->value, "Workflow Run ID");
  get->callback([&, get_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/workflow-runs/" + get_args->value);
  });

  auto failures_args = std::make_shared<IdentifierArgs>();
  auto *failures =
      run->add_subcommand("failures", "Get a Workflow Run failure report");
  configure_api_leaf(*failures, options, "RUN_ID");
  add_identifier(*failures, "run_id", failures_args->value,
                 "Workflow Run ID");
  failures->callback([&, failures_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/workflow-runs/" + failures_args->value +
                           "/failures");
  });

  auto repair_args = std::make_shared<RepairRunArgs>();
  auto *repair = run->add_subcommand("repair", "Start a repaired Workflow Run");
  configure_api_leaf(*repair, options, "RUN_ID REQUEST");
  add_identifier(*repair, "run_id", repair_args->run_id, "Parent Run ID");
  repair
      ->add_option("request", repair_args->request_body,
                   "RepairRunRequest JSON, @file, or - for standard input")
      ->required()
      ->type_name("JSON");
  repair->callback([&, repair_args] {
    select_api_command(
        options, selection, "POST",
        "/api/v1/workflow-runs/" + repair_args->run_id + "/repairs",
        repair_args->request_body, "application/json");
  });

  auto output_args = std::make_shared<OutputArgs>();
  auto *output = run->add_subcommand("output", "Read one node output value");
  configure_api_leaf(*output, options, "RUN_ID NODE_ID PORT");
  add_identifier(*output, "run_id", output_args->run_id, "Workflow Run ID");
  add_identifier(*output, "node_id", output_args->node_id, "Node ID");
  add_identifier(*output, "port", output_args->port, "Output port");
  output->callback([&, output_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/workflow-runs/" + output_args->run_id +
                           "/outputs/" + output_args->node_id + "/" +
                           output_args->port);
  });

  struct EvidenceArgs {
    std::string run_id;
    PageArgs page;
  };
  auto evidence_args = std::make_shared<EvidenceArgs>();
  auto *evidence = run->add_subcommand("evidence", "List Workflow Run evidence");
  configure_api_leaf(*evidence, options,
                     "RUN_ID [--offset N] [--limit N]");
  add_identifier(*evidence, "run_id", evidence_args->run_id,
                 "Workflow Run ID");
  add_pagination(*evidence, evidence_args->page);
  evidence->callback([&, evidence_args] {
    select_api_command(
        options, selection, "GET",
        "/api/v1/workflow-runs/" + evidence_args->run_id +
            "/evidence?offset=" +
            std::to_string(evidence_args->page.offset) + "&limit=" +
            std::to_string(evidence_args->page.limit));
  });

  for (const auto action : {std::string_view{"pause"},
                            std::string_view{"resume"},
                            std::string_view{"cancel"}}) {
    auto args = std::make_shared<IdentifierArgs>();
    auto *command = run->add_subcommand(std::string(action),
                                        std::string(action) + " a Workflow Run");
    configure_api_leaf(*command, options, "RUN_ID");
    add_identifier(*command, "run_id", args->value, "Workflow Run ID");
    command->callback([&, args, action] {
      select_api_command(options, selection, "POST",
                         "/api/v1/workflow-runs/" + args->value + "/" +
                             std::string(action));
    });
  }
}

auto configure_api_artifact(CLI::App &api, ApiOptions &options,
                            CommandSelection &selection) -> void {
  auto *artifact =
      api.add_subcommand("artifact", "Store and retrieve binary artifacts");
  require_subcommand(*artifact);
  artifact->footer(
      "Run `dagforge api artifact COMMAND --help` for command options.");

  auto put_args = std::make_shared<ArtifactPutArgs>();
  auto *put = artifact->add_subcommand("put", "Upload an artifact");
  configure_api_leaf(*put, options, "FILE [--type MEDIA_TYPE]");
  put->add_option("file", put_args->file, "Artifact file")
      ->required()
      ->check(CLI::ExistingFile)
      ->type_name("FILE");
  put->add_option("-t,--type", put_args->media_type, "Artifact media type")
      ->default_val(put_args->media_type)
      ->type_name("MEDIA_TYPE");
  put->callback([&, put_args] {
    select_api_command(options, selection, "POST", "/api/v1/artifacts",
                       "@" + put_args->file, put_args->media_type);
  });

  auto get_args = std::make_shared<IdentifierArgs>();
  auto *get = artifact->add_subcommand("get", "Download an artifact");
  configure_api_leaf(*get, options, "ARTIFACT_ID");
  add_identifier(*get, "artifact_id", get_args->value, "Artifact ID");
  get->callback([&, get_args] {
    select_api_command(options, selection, "GET",
                       "/api/v1/artifacts/" + get_args->value);
  });

  auto delete_args = std::make_shared<IdentifierArgs>();
  auto *remove = artifact->add_subcommand("delete", "Delete an artifact");
  remove->alias("rm");
  configure_api_leaf(*remove, options, "ARTIFACT_ID");
  add_identifier(*remove, "artifact_id", delete_args->value, "Artifact ID");
  remove->callback([&, delete_args] {
    select_api_command(options, selection, "DELETE",
                       "/api/v1/artifacts/" + delete_args->value);
  });
}

auto configure_api_request(CLI::App &api, ApiOptions &options,
                           CommandSelection &selection) -> void {
  auto *request = api.add_subcommand(
      "request", "Send a raw request to any DAGForge API endpoint");
  configure_api_leaf(*request, options, "METHOD PATH [BODY]");
  request->add_option("method", options.method, "HTTP method")
      ->required()
      ->transform(http_method_validator())
      ->type_name("METHOD");
  request->add_option("path", options.path,
                      "API path, including query string")
      ->required()
      ->check(request_path_validator())
      ->type_name("PATH");
  request
      ->add_option("body", options.body,
                   "Literal body, @file, or - for standard input")
      ->type_name("BODY");
  request->callback([&] { selection.select(options); });
}

auto configure_api(CLI::App &root, ApiOptions &options,
                   CommandSelection &selection) -> void {
  auto *api = root.add_subcommand(
      "api", "Control a running DAGForge service over its HTTP API");
  require_subcommand(*api);
  configure_api_system(*api, options, selection);
  configure_api_plan(*api, options, selection);
  configure_api_run(*api, options, selection);
  configure_api_artifact(*api, options, selection);
  configure_api_request(*api, options, selection);
  api->footer("Examples:\n"
              "  dagforge api health\n"
              "  dagforge api ready\n"
              "  dagforge api plan add workflow.json\n"
              "  dagforge api run start hello-world @start-run.json\n"
              "  dagforge api run cancel RUN_ID\n"
              "  dagforge api request GET /api/status");
}

} // namespace

auto run_command_line(int argc, char **argv) -> int {
  CLI::App app{"DAGForge 0.4 AI workflow runtime"};
  configure_app(app);

  ServeOptions serve_options;
  ValidateOptions validate_options;
  RunOptions run_options;
  ApiOptions api_options;
  CommandSelection selection;

  configure_serve(app, serve_options, selection);
  configure_validate(app, validate_options, selection);
  configure_run(app, run_options, selection);
  configure_api(app, api_options, selection);

  try {
    app.parse(argc, argv);
    if (selection.count() != 1) {
      throw CLI::ValidationError(
          "command", "exactly one executable subcommand must be specified");
    }
  } catch (const CLI::ParseError &error) {
    return app.exit(error);
  }
  return selection.run();
}

} // namespace dagforge::cli
