#include "detail/commands.hpp"

#include "dagforge/app/application.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/http/http_client.hpp"
#include "dagforge/io/context.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <boost/algorithm/string/predicate.hpp>
#include <boost/url/url.hpp>

#include <atomic>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <print>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace dagforge::cli {
namespace {

std::atomic<bool> g_shutdown_requested{false};

extern "C" void handle_signal(int) {
  g_shutdown_requested.store(true, std::memory_order_release);
}

struct ApiEndpoint {
  std::string host;
  std::string authority;
  std::uint16_t port{0};
  bool tls{false};
};

[[nodiscard]] auto read_binary_file(std::string_view path)
    -> Result<std::vector<std::uint8_t>> {
  std::ifstream input(std::string(path), std::ios::binary);
  if (!input) {
    return fail(Error::FileOpenFailed);
  }
  std::vector<std::uint8_t> bytes;
  for (std::istreambuf_iterator<char> current(input), end; current != end;
       ++current) {
    bytes.push_back(static_cast<std::uint8_t>(*current));
  }
  if (!input.good() && !input.eof()) {
    return fail(Error::FileOpenFailed);
  }
  return ok(std::move(bytes));
}

[[nodiscard]] auto read_text_file(std::string_view path)
    -> Result<std::string> {
  auto bytes = read_binary_file(path);
  if (!bytes) {
    return fail(bytes.error());
  }
  return ok(std::string(reinterpret_cast<const char *>(bytes->data()),
                        bytes->size()));
}

[[nodiscard]] auto read_standard_input() -> Result<std::vector<std::uint8_t>> {
  std::vector<std::uint8_t> bytes;
  for (std::istreambuf_iterator<char> current(std::cin), end; current != end;
       ++current) {
    bytes.push_back(static_cast<std::uint8_t>(*current));
  }
  if (!std::cin.good() && !std::cin.eof()) {
    return fail(Error::FileOpenFailed);
  }
  return ok(std::move(bytes));
}

[[nodiscard]] auto read_request_body(std::string_view value)
    -> Result<std::vector<std::uint8_t>> {
  if (value.empty()) {
    return ok(std::vector<std::uint8_t>{});
  }
  if (value == "-") {
    return read_standard_input();
  }
  if (value.front() == '@') {
    if (value.size() == 1) {
      return fail(Error::InvalidArgument);
    }
    return read_binary_file(value.substr(1));
  }
  return ok(std::vector<std::uint8_t>(value.begin(), value.end()));
}

[[nodiscard]] auto load_plan(std::string_view path)
    -> Result<workflow::WorkflowPlan> {
  auto text = read_text_file(path);
  if (!text) {
    return fail(text.error());
  }
  return workflow::WorkflowPlanLoader::from_json(*text);
}

[[nodiscard]] auto offline_validation_config(
    const workflow::WorkflowPlan &plan) -> config::SystemConfig {
  config::SystemConfig system_config;
  system_config.api.enabled = false;
  system_config.admission.allow_unlisted_executors = true;
  system_config.executors.command.policy.allow_unlisted_programs = true;
  system_config.executors.command.policy.allow_unlisted_environment = true;
  system_config.executors.command.policy.require_trusted_programs = false;
  system_config.executors.command.minijail.require_trusted_files = false;
  system_config.executors.http.enabled = true;
  system_config.executors.http.egress.allow_plaintext = true;
  system_config.executors.http.egress.deny_private_networks = false;

  for (const auto &node : plan.nodes) {
    if (node.executor != "http" || !node.config.is_object()) {
      continue;
    }
    auto url = glz::get_as_json<std::string, "/url">(node.config.encoded());
    if (!url || url->empty()) {
      continue;
    }
    try {
      const boost::urls::url parsed{*url};
      if (parsed.scheme().empty() || parsed.host().empty()) {
        continue;
      }
      std::string origin;
      origin.reserve(parsed.scheme().size() +
                     parsed.encoded_authority().size() + 3);
      origin.append(parsed.scheme().data(), parsed.scheme().size());
      origin.append("://");
      origin.append(parsed.encoded_authority().data(),
                    parsed.encoded_authority().size());
      system_config.executors.http.egress.allowed_origins.push_back(
          std::move(origin));
    } catch (const boost::system::system_error &) {
    }
  }
  return system_config;
}

[[nodiscard]] auto parse_http_method(std::string_view method)
    -> Result<http::HttpMethod> {
  using enum http::HttpMethod;
  if (method == "GET") {
    return ok(GET);
  }
  if (method == "POST") {
    return ok(POST);
  }
  if (method == "PUT") {
    return ok(PUT);
  }
  if (method == "DELETE") {
    return ok(DELETE);
  }
  if (method == "PATCH") {
    return ok(PATCH);
  }
  if (method == "OPTIONS") {
    return ok(OPTIONS);
  }
  if (method == "HEAD") {
    return ok(HEAD);
  }
  return fail(Error::InvalidArgument);
}

[[nodiscard]] auto parse_api_endpoint(std::string_view value)
    -> Result<ApiEndpoint> {
  try {
    const boost::urls::url endpoint{value};
    const bool tls = endpoint.scheme() == "https";
    if ((!tls && endpoint.scheme() != "http") || endpoint.host().empty()) {
      return fail(Error::InvalidArgument);
    }

    std::uint16_t port = tls ? 443 : 80;
    const auto port_text = endpoint.port();
    if (!port_text.empty()) {
      unsigned parsed_port = 0;
      const auto [end, error] =
          std::from_chars(port_text.data(), port_text.data() + port_text.size(),
                          parsed_port);
      if (error != std::errc{} || end != port_text.data() + port_text.size() ||
          parsed_port == 0 || parsed_port > 65535) {
        return fail(Error::InvalidArgument);
      }
      port = static_cast<std::uint16_t>(parsed_port);
    }
    return ok(ApiEndpoint{
        .host = endpoint.host_address(),
        .authority = std::string(endpoint.encoded_authority()),
        .port = port,
        .tls = tls,
    });
  } catch (const boost::system::system_error &) {
    return fail(Error::InvalidArgument);
  }
}

auto trim_ascii(std::string_view &value) -> void {
  while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
    value.remove_prefix(1);
  }
  while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) {
    value.remove_suffix(1);
  }
}

[[nodiscard]] auto add_headers(const ApiOptions &options,
                               http::HttpRequest &request) -> Result<void> {
  for (const auto &entry : options.headers) {
    const auto separator = entry.find(':');
    if (separator == std::string::npos) {
      return fail(Error::InvalidArgument);
    }
    std::string_view name{entry.data(), separator};
    std::string_view value{entry.data() + separator + 1,
                           entry.size() - separator - 1};
    trim_ascii(name);
    trim_ascii(value);
    if (name.empty() || boost::algorithm::iequals(name, "Content-Length") ||
        boost::algorithm::iequals(name, "Transfer-Encoding")) {
      return fail(Error::InvalidArgument);
    }
    request.headers.add(std::string(name), std::string(value));
  }
  return ok();
}

[[nodiscard]] auto build_api_request(const ApiOptions &options,
                                     const ApiEndpoint &endpoint)
    -> Result<http::HttpRequest> {
  auto method = parse_http_method(options.method);
  auto body = read_request_body(options.body);
  if (!method || !body) {
    return fail(method ? body.error() : method.error());
  }

  http::HttpRequest request;
  request.method = *method;
  request.path = options.path;
  request.body = std::move(*body);
  auto headers = add_headers(options, request);
  if (!headers) {
    return fail(headers.error());
  }
  if (!request.headers.contains("Host")) {
    request.headers.set("Host", endpoint.authority);
  }
  if (!options.bearer_token.empty() &&
      !request.headers.contains("Authorization")) {
    request.headers.set("Authorization", "Bearer " + options.bearer_token);
  }
  if (!request.body.empty() && !request.headers.contains("Content-Type")) {
    const auto body_view = request.body_as_string();
    request.headers.set(
        "Content-Type",
        !options.content_type.empty()
            ? options.content_type
            : (is_valid_json(body_view) ? "application/json"
                                        : "application/octet-stream"));
  }
  return ok(std::move(request));
}

auto send_api_request(ApiEndpoint endpoint, http::HttpClientConfig client_config,
                      http::HttpRequest request)
    -> task<Result<http::HttpResponse>> {
  auto connected = endpoint.tls
                       ? co_await http::HttpClient::connect_tls(
                             current_io_context(), std::move(endpoint.host),
                             endpoint.port, std::move(client_config))
                       : co_await http::HttpClient::connect_tcp(
                             current_io_context(), std::move(endpoint.host),
                             endpoint.port, std::move(client_config));
  if (!connected) {
    co_return fail(connected.error());
  }
  co_return co_await (*connected)->request(std::move(request));
}

[[nodiscard]] auto write_response_body(const ApiOptions &options,
                                       const http::HttpResponse &response)
    -> Result<void> {
  if (!options.output_path.empty()) {
    std::ofstream output(options.output_path,
                         std::ios::binary | std::ios::trunc);
    if (!output) {
      return fail(Error::FileOpenFailed);
    }
    const auto body = response.body_as_string();
    output.write(body.data(), static_cast<std::streamsize>(body.size()));
    output.flush();
    if (!output) {
      return fail(Error::FileOpenFailed);
    }
    return ok();
  }
  if (!response.body.empty()) {
    const auto written = std::fwrite(response.body.data(), 1,
                                     response.body.size(), stdout);
    if (written != response.body.size()) {
      return fail(Error::FileOpenFailed);
    }
  }
  return ok();
}

[[nodiscard]] auto is_success(http::HttpStatus status) -> bool {
  const auto code = static_cast<std::uint16_t>(status);
  return code >= 200 && code < 300;
}

} // namespace

auto execute(const ServeOptions &options) -> int {
  Application app;
  auto loaded = app.load_config(options.config_path);
  if (!loaded) {
    std::println(stderr, "Failed to configure DAGForge: {}",
                 loaded.error().message());
    return 1;
  }
  auto started = app.start();
  if (!started) {
    std::println(stderr, "Failed to start DAGForge: {}",
                 started.error().message());
    return 1;
  }

  g_shutdown_requested.store(false, std::memory_order_release);
  std::signal(SIGINT, handle_signal);
  std::signal(SIGTERM, handle_signal);
  while (!g_shutdown_requested.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  app.stop();
  return 0;
}

auto execute(const ValidateOptions &options) -> int {
  auto plan = load_plan(options.plan_path);
  if (!plan) {
    std::println(stderr, "Invalid Workflow Plan: {}", plan.error().message());
    return 1;
  }

  Application app{options.config_path.empty()
                      ? offline_validation_config(*plan)
                      : config::SystemConfig{}};
  if (!options.config_path.empty()) {
    auto loaded = app.load_config(options.config_path);
    if (!loaded) {
      std::println(stderr, "Failed to configure Workflow validation: {}",
                   loaded.error().message());
      return 1;
    }
  }
  auto initialized = app.init();
  if (!initialized || app.workflow_control_plane() == nullptr) {
    std::println(stderr, "Failed to initialize Workflow validation: {}",
                 initialized ? "Workflow runtime is disabled"
                             : initialized.error().message());
    return 1;
  }
  auto compiled = app.workflow_control_plane()->register_plan(std::move(*plan));
  if (!compiled) {
    std::println(stderr, "Workflow rejected: {}", compiled.error().message());
    return 1;
  }
  std::println("workflow_id={}", (*compiled)->workflow_id);
  std::println("plan_id={}", (*compiled)->plan_id);
  std::println("digest={}", (*compiled)->digest);
  std::println("nodes={}", (*compiled)->nodes.size());
  std::println("durability_deferred={}", compiled->durability_deferred);
  return 0;
}

auto execute(const RunOptions &options) -> int {
  Application app;
  auto loaded = app.load_config(options.config_path);
  if (!loaded) {
    std::println(stderr, "Failed to configure runtime: {}",
                 loaded.error().message());
    return 1;
  }
  auto system_config = app.config();
  system_config.api.enabled = false;
  auto configured = app.apply_config(std::move(system_config));
  if (!configured) {
    std::println(stderr, "Failed to apply CLI configuration: {}",
                 configured.error().message());
    return 1;
  }
  auto started = app.start();
  if (!started) {
    std::println(stderr, "Failed to start runtime: {}",
                 started.error().message());
    return 1;
  }

  auto plan = load_plan(options.plan_path);
  if (!plan) {
    std::println(stderr, "Invalid Workflow Plan: {}", plan.error().message());
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
  if (registered->durability_deferred) {
    std::println(stderr,
                 "Workflow Plan was registered but storage durability is deferred");
  }

  auto run = app.workflow_runtime()->start(
      *registered,
      workflow::TriggerEnvelope{
          .workflow_id = (*registered)->workflow_id.clone(),
          .source = "cli",
          .event_type = "request",
          .principal = workflow::Principal{.subject = "cli"},
      });
  if (!run) {
    std::println(stderr, "Failed to start Workflow: {}",
                 run.error().message());
    app.stop();
    return 1;
  }
  std::println("run_id={}", *run);

  for (;;) {
    auto snapshot = sync_wait_on_runtime(
        app.runtime(), app.workflow_runtime()->snapshot(*run));
    if (!snapshot) {
      std::println(stderr, "Failed to query Workflow: {}",
                   snapshot.error().message());
      app.stop();
      return 1;
    }
    if (workflow::is_terminal((*snapshot)->state)) {
      std::println("state={}", workflow::to_string_view((*snapshot)->state));
      const auto exit_code =
          (*snapshot)->state == workflow::RunState::Succeeded ? 0 : 1;
      app.stop();
      return exit_code;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

auto execute(const ApiOptions &options) -> int {
  auto endpoint = parse_api_endpoint(options.endpoint);
  if (!endpoint) {
    std::println(stderr, "Invalid API endpoint: {}", options.endpoint);
    return 2;
  }
  auto request = build_api_request(options, *endpoint);
  if (!request) {
    std::println(stderr, "Invalid API request: {}", request.error().message());
    return 2;
  }

  Runtime runtime{1};
  auto started = runtime.start();
  if (!started) {
    std::println(stderr, "Failed to start HTTP client runtime: {}",
                 started.error().message());
    return 1;
  }
  http::HttpClientConfig client_config{
      .keep_alive = false,
      .tls_min_version = options.tls_min_version,
      .tls_ca_file = options.tls_ca_file,
      .tls_client_cert_file = options.tls_client_cert_file,
      .tls_client_key_file = options.tls_client_key_file,
  };
  auto response = sync_wait_on_runtime(
      runtime, send_api_request(std::move(*endpoint), std::move(client_config),
                                std::move(*request)));
  runtime.stop();
  if (!response) {
    std::println(stderr, "API request failed: {}", response.error().message());
    return 1;
  }

  const auto status_code = static_cast<std::uint16_t>(response->status);
  if (options.include_headers) {
    std::println("HTTP/1.1 {} {}", status_code,
                 http::status_reason_phrase(response->status));
    for (const auto &header : response->headers) {
      std::println("{}: {}", header.name, header.value);
    }
    std::println();
  }
  auto written = write_response_body(options, *response);
  if (!written) {
    std::println(stderr, "Failed to write API response: {}",
                 written.error().message());
    return 1;
  }
  if (!is_success(response->status)) {
    std::println(stderr, "HTTP {} {}", status_code,
                 http::status_reason_phrase(response->status));
    return 1;
  }
  return 0;
}

} // namespace dagforge::cli
