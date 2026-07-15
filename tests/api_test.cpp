#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/checkpoint_store.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include "../src/dagforge/app/api/detail/api_context.hpp"
#include "../src/dagforge/app/api/detail/routes/system.hpp"
#include "../src/dagforge/app/api/detail/routes/workflows.hpp"

#include "gtest/gtest.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>

using namespace dagforge;
using namespace dagforge::config;

namespace {

[[nodiscard]] auto response_text(const http::HttpResponse &response)
    -> std::string {
  return std::string(response.body.begin(), response.body.end());
}

} // namespace

TEST(ApiTest, ApiServerConstructs) {
  SystemConfig cfg;
  cfg.api.enabled = false;
  Application app(std::move(cfg));
  ApiServer server(app);
  EXPECT_FALSE(server.is_running());
}

TEST(ApiTest, ApplicationConfigAccessible) {
  Application app;
  EXPECT_EQ(app.config().api.host, "127.0.0.1");
}

TEST(ApiTest, ApplicationRestoresPersistentCheckpointOnStart) {
  const auto root = std::filesystem::temp_directory_path() /
                    std::format("dagforge-app-restore-{}", ::getpid());
  std::error_code error;
  std::filesystem::remove_all(root, error);

  auto plan = workflow::WorkflowPlanLoader::from_json(R"({
    "workflow_id":"persisted-flow","schema_version":1,
    "nodes":[{"id":"command","executor":"command",
      "config":{"program":"/bin/true","arguments":[],"env":[],"input_env":[]},
      "outputs":["result"]}]
  })");
  ASSERT_TRUE(plan.has_value()) << plan.error().message();
  workflow::WorkflowCheckpoint checkpoint;
  checkpoint.plan = std::move(*plan);
  checkpoint.trigger.trigger_id = WorkflowTriggerId{"persisted-trigger"};
  checkpoint.trigger.workflow_id = WorkflowId{"persisted-flow"};
  checkpoint.trigger.idempotency_key = "persisted-key";
  checkpoint.snapshot.run_id = WorkflowRunId{"persisted-run"};
  checkpoint.snapshot.workflow_id = WorkflowId{"persisted-flow"};
  checkpoint.snapshot.plan_id = WorkflowPlanId{"persisted-plan"};
  checkpoint.snapshot.state = workflow::RunState::Succeeded;
  checkpoint.snapshot.tasks.push_back(workflow::TaskSnapshot{
      .node_id = WorkflowNodeId{"command"},
      .state = workflow::TaskState::Succeeded,
  });
  checkpoint.values.emplace_back(
      workflow::OutputRef{.node_id = WorkflowNodeId{"command"},
                          .port = WorkflowPortId{"result"}},
      std::string{"restored-value"});
  workflow::CheckpointStore store(root / "runs");
  auto saved = store.save(std::move(checkpoint));
  ASSERT_TRUE(saved.has_value()) << saved.error().message();

  SystemConfig config;
  config.api.enabled = false;
  config.storage.enabled = true;
  config.storage.directory = root.string();
  config.storage.max_completed_runs = 4;
  config.admission.allowed_executors = {"command"};
  config.executors.command.policy.allowed_programs = {"/bin/true"};
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  const Application &const_app = app;
  EXPECT_EQ(const_app.config().storage.directory, root.string());
  ASSERT_NE(const_app.workflow_runtime(), nullptr);
  ASSERT_NE(const_app.workflow_control_plane(), nullptr);
  auto restored = sync_wait_on_runtime(
      app.runtime(),
      app.workflow_runtime()->snapshot(WorkflowRunId{"persisted-run"}));
  ASSERT_TRUE(restored.has_value()) << restored.error().message();
  EXPECT_EQ((*restored)->state, workflow::RunState::Succeeded);
  auto value = sync_wait_on_runtime(
      app.runtime(), app.workflow_runtime()->output(
                         WorkflowRunId{"persisted-run"},
                         workflow::OutputRef{
                             .node_id = WorkflowNodeId{"command"},
                             .port = WorkflowPortId{"result"}}));
  ASSERT_TRUE(value.has_value()) << value.error().message();
  EXPECT_EQ(std::get<std::string>(**value), "restored-value");
  app.stop();
  std::filesystem::remove_all(root, error);
}

TEST(ApiTest, ApplicationRestoresPlanCatalogWithoutRunCheckpoint) {
  const auto root = std::filesystem::temp_directory_path() /
                    std::format("dagforge-app-plan-catalog-{}", ::getpid());
  std::error_code error;
  std::filesystem::remove_all(root, error);

  const auto make_config = [&] {
    SystemConfig config;
    config.api.enabled = false;
    config.storage.enabled = true;
    config.storage.directory = root.string();
    config.admission.allowed_executors = {"command"};
    config.executors.command.policy.allowed_programs = {"/bin/true"};
    return config;
  };

  WorkflowPlanId plan_id;
  {
    Application app(make_config());
    ASSERT_TRUE(app.start().has_value());
    auto plan = workflow::WorkflowPlanLoader::from_json(R"({
      "workflow_id":"catalog-flow","schema_version":1,
      "nodes":[{"id":"command","executor":"command",
        "config":{"program":"/bin/true","arguments":[],"env":[],"input_env":[]},
        "outputs":["result"]}]
    })");
    ASSERT_TRUE(plan.has_value()) << plan.error().message();
    auto registered =
        app.workflow_control_plane()->register_plan(std::move(*plan));
    ASSERT_TRUE(registered.has_value()) << registered.error().message();
    plan_id = (*registered)->plan_id.clone();
    app.stop();
  }

  EXPECT_TRUE(std::filesystem::exists(root / "plans" /
                                      (plan_id.str() + ".json")));
  bool has_run_checkpoint = false;
  for (const auto &entry : std::filesystem::directory_iterator(root / "runs")) {
    has_run_checkpoint = has_run_checkpoint || entry.is_regular_file();
  }
  EXPECT_FALSE(has_run_checkpoint);

  Application restored(make_config());
  ASSERT_TRUE(restored.start().has_value());
  auto by_id = restored.workflow_control_plane()->get_plan(plan_id);
  ASSERT_TRUE(by_id.has_value()) << by_id.error().message();
  EXPECT_EQ((*by_id)->workflow_id, WorkflowId{"catalog-flow"});
  auto latest = restored.workflow_control_plane()->get_latest(
      WorkflowId{"catalog-flow"});
  ASSERT_TRUE(latest.has_value()) << latest.error().message();
  EXPECT_EQ((*latest)->plan_id, plan_id);
  restored.stop();
  std::filesystem::remove_all(root, error);
}

TEST(ApiTest, ApplicationRejectsCorruptPersistentCheckpoint) {
  const auto root = std::filesystem::temp_directory_path() /
                    std::format("dagforge-app-corrupt-{}", ::getpid());
  std::error_code error;
  std::filesystem::remove_all(root, error);
  std::filesystem::create_directories(root / "runs", error);
  ASSERT_FALSE(error);
  {
    std::ofstream output(root / "runs" / "broken.json",
                         std::ios::binary | std::ios::trunc);
    output << "not-json";
  }

  SystemConfig config;
  config.api.enabled = false;
  config.storage.enabled = true;
  config.storage.directory = root.string();
  Application app(std::move(config));
  auto started = app.start();
  ASSERT_FALSE(started.has_value());
  EXPECT_EQ(started.error(), make_error_code(Error::ParseError));
  EXPECT_FALSE(app.is_running());
  std::filesystem::remove_all(root, error);
}

TEST(ApiTest, InitCreatesApiServerInstance) {
  SystemConfig cfg;
  cfg.api.enabled = true;
  Application app(std::move(cfg));
  ASSERT_TRUE(app.init().has_value());
  EXPECT_NE(app.api_server(), nullptr);
}

TEST(ApiTest, DisabledApiDoesNotAllocateServer) {
  Application app;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_EQ(app.api_server(), nullptr);
}

TEST(ApiTest, InitReconcilesApiConfigurationChanges) {
  SystemConfig cfg;
  cfg.api.enabled = true;
  Application app(std::move(cfg));
  ASSERT_NE(app.api_server(), nullptr);

  app.config().api.enabled = false;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_EQ(app.api_server(), nullptr);

  app.config().api.enabled = true;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_NE(app.api_server(), nullptr);
}

TEST(ApiTest, InitReconcilesWorkflowConfigurationChanges) {
  Application app;
  ASSERT_NE(app.workflow_runtime(), nullptr);
  ASSERT_NE(app.workflow_control_plane(), nullptr);

  app.config().workflow.enabled = false;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_EQ(app.workflow_runtime(), nullptr);
  EXPECT_EQ(app.workflow_control_plane(), nullptr);

  app.config().workflow.enabled = true;
  ASSERT_TRUE(app.init().has_value());
  EXPECT_NE(app.workflow_runtime(), nullptr);
  EXPECT_NE(app.workflow_control_plane(), nullptr);
}

TEST(ApiTest, InitReconcilesHttpExecutorEnablement) {
  SystemConfig config;
  config.admission.allowed_executors = {"http"};
  Application app(std::move(config));
  const auto make_plan = [] {
    return workflow::WorkflowPlanLoader::from_json(R"({
      "workflow_id":"http-toggle",
      "schema_version":1,
      "nodes":[{
        "id":"request",
        "executor":"http",
        "outputs":["result"],
        "config":{
          "method":"GET",
          "url":"https://example.com/resource",
          "headers":[],
          "input_headers":[],
          "accepted_statuses":[]
        }
      }]
    })");
  };

  auto disabled_plan = make_plan();
  ASSERT_TRUE(disabled_plan.has_value());
  auto disabled =
      app.workflow_control_plane()->register_plan(std::move(*disabled_plan));
  ASSERT_FALSE(disabled.has_value());
  EXPECT_EQ(disabled.error(), make_error_code(Error::Unsupported));

  app.config().executors.http.enabled = true;
  app.config().executors.http.egress.allowed_origins = {
      "https://example.com"};
  ASSERT_TRUE(app.init().has_value());
  auto enabled_plan = make_plan();
  ASSERT_TRUE(enabled_plan.has_value());
  auto enabled =
      app.workflow_control_plane()->register_plan(std::move(*enabled_plan));
  ASSERT_TRUE(enabled.has_value()) << enabled.error().message();

  app.config().executors.http.enabled = false;
  ASSERT_TRUE(app.init().has_value());
  auto disabled_again_plan = make_plan();
  ASSERT_TRUE(disabled_again_plan.has_value());
  auto disabled_again = app.workflow_control_plane()->register_plan(
      std::move(*disabled_again_plan));
  ASSERT_FALSE(disabled_again.has_value());
  EXPECT_EQ(disabled_again.error(), make_error_code(Error::Unsupported));
}

TEST(ApiTest, RestartRebuildsQuiescedWorkflowComponents) {
  Application app;
  ASSERT_TRUE(app.start().has_value());
  app.stop();
  ASSERT_EQ(app.workflow_runtime(), nullptr);
  ASSERT_EQ(app.workflow_control_plane(), nullptr);

  ASSERT_TRUE(app.start().has_value());
  EXPECT_NE(app.workflow_runtime(), nullptr);
  EXPECT_NE(app.workflow_control_plane(), nullptr);
  app.stop();
}

TEST(ApiTest, AccessPolicyAuthenticatesAndLimitsRequests) {
  SystemConfig config;
  config.api.enabled = false;
  config.admission.allowed_executors = {"command"};
  config.executors.command.policy.allowed_programs = {"/bin/true",
                                                       "/bin/echo"};
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  api_detail::HttpMetricsRegistry metrics;
  api_detail::ApiContext context{
      .app = app,
      .server = server,
      .http_active_requests = active,
      .http_metrics = metrics,
      .bearer_token = "secret",
      .max_request_body_bytes = 4,
      .max_concurrent_requests = 1,
  };
  auto handler = context.make_instrumented_route(
      http::HttpMethod::POST, "/test",
      [](http::HttpRequest) -> task<http::HttpResponse> {
        co_return http::HttpResponse::ok();
      });
  const auto invoke = [&](http::HttpRequest request)
      -> task<Result<http::HttpResponse>> {
    co_return ok(co_await handler(std::move(request)));
  };

  http::HttpRequest unauthorized;
  unauthorized.method = http::HttpMethod::POST;
  auto unauthorized_response =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(unauthorized)));
  ASSERT_TRUE(unauthorized_response.has_value());
  EXPECT_EQ(unauthorized_response->status, http::HttpStatus::Unauthorized);

  http::HttpRequest oversized;
  oversized.method = http::HttpMethod::POST;
  oversized.headers.set("Authorization", "Bearer secret");
  oversized.body.resize(5);
  auto oversized_response =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(oversized)));
  ASSERT_TRUE(oversized_response.has_value());
  EXPECT_EQ(oversized_response->status, http::HttpStatus::PayloadTooLarge);

  active.store(1, std::memory_order_release);
  http::HttpRequest saturated;
  saturated.method = http::HttpMethod::POST;
  saturated.headers.set("Authorization", "Bearer secret");
  auto saturated_response =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(saturated)));
  ASSERT_TRUE(saturated_response.has_value());
  EXPECT_EQ(saturated_response->status, http::HttpStatus::TooManyRequests);

  active.store(0, std::memory_order_release);
  http::HttpRequest accepted;
  accepted.method = http::HttpMethod::POST;
  accepted.headers.set("Authorization", "Bearer secret");
  auto accepted_response =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(accepted)));
  ASSERT_TRUE(accepted_response.has_value());
  EXPECT_EQ(accepted_response->status, http::HttpStatus::Ok);
  app.stop();
}

TEST(ApiTest, MissingConfiguredBearerTokenPreventsStart) {
  constexpr auto *kMissingEnvironment = "DAGFORGE_TEST_MISSING_TOKEN";
  ::unsetenv(kMissingEnvironment);
  SystemConfig config;
  config.api.bearer_token_env = kMissingEnvironment;
  Application app(std::move(config));
  ASSERT_TRUE(app.runtime().start().has_value());
  ApiServer server(app);
  auto started = server.start();
  ASSERT_FALSE(started.has_value());
  EXPECT_EQ(started.error(), make_error_code(Error::InvalidArgument));
  app.runtime().stop();
}

TEST(ApiTest, SystemRoutesReportHealthStatusAndMetrics) {
  SystemConfig config;
  config.api.enabled = false;
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  api_detail::HttpMetricsRegistry metrics;
  api_detail::ApiContext context{
      .app = app,
      .server = server,
      .http_active_requests = active,
      .http_metrics = metrics,
  };
  api_detail::register_system_routes(context);
  const auto invoke = [&](std::string path) {
    http::HttpRequest request;
    request.method = http::HttpMethod::GET;
    request.path = std::move(path);
    return sync_wait_on_runtime(
        app.runtime(), [&]() -> task<Result<http::HttpResponse>> {
          co_return ok(co_await server.router().route(std::move(request)));
        }());
  };

  auto health = invoke("/api/health");
  ASSERT_TRUE(health.has_value());
  EXPECT_EQ(health->status, http::HttpStatus::Ok);
  auto health_body = parse_json(response_text(*health));
  ASSERT_TRUE(health_body.has_value());
  EXPECT_EQ(health_body->get_object().at("status").as<std::string>(),
            "healthy");

  auto status = invoke("/api/status");
  ASSERT_TRUE(status.has_value());
  EXPECT_EQ(status->status, http::HttpStatus::Ok);
  auto status_body = parse_json(response_text(*status));
  ASSERT_TRUE(status_body.has_value());
  EXPECT_EQ(status_body->get_object().at("runtime").as<std::string>(),
            "running");
  EXPECT_TRUE(status_body->get_object().at("workflow_enabled").get<bool>());
  EXPECT_EQ(status_body->get_object()
                .at("active_workflow_runs")
                .as<std::int64_t>(),
            0);
  EXPECT_GT(status_body->get_object().at("shards").as<std::int64_t>(), 0);
  EXPECT_FALSE(status_body->get_object().at("timestamp").as<std::string>().empty());

  auto prometheus = invoke("/metrics");
  ASSERT_TRUE(prometheus.has_value());
  EXPECT_EQ(prometheus->status, http::HttpStatus::Ok);
  EXPECT_EQ(prometheus->headers.get("Content-Type"),
            "text/plain; version=0.0.4; charset=utf-8");
  EXPECT_FALSE(response_text(*prometheus).empty());
  app.stop();
}

TEST(ApiTest, WorkflowRoutesSupportPaginationPlanSelectionAndArtifacts) {
  SystemConfig config;
  config.api.enabled = false;
  config.admission.allowed_executors = {"command"};
  config.executors.command.policy.allowed_programs = {"/bin/true",
                                                       "/bin/echo"};
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  api_detail::HttpMetricsRegistry metrics;
  api_detail::ApiContext context{
      .app = app,
      .server = server,
      .http_active_requests = active,
      .http_metrics = metrics,
  };
  api_detail::register_workflow_routes(context);
  const auto invoke = [&](http::HttpRequest request)
      -> task<Result<http::HttpResponse>> {
    co_return ok(co_await server.router().route(std::move(request)));
  };
  const auto register_plan = [&](std::string_view workflow,
                                 std::string_view program)
      -> Result<std::string> {
    http::HttpRequest request;
    request.method = http::HttpMethod::POST;
    request.path = "/api/v1/workflows/plans";
    const auto body = std::format(
        R"({{"workflow_id":"{}","schema_version":1,"nodes":[{{"id":"command","executor":"command","config":{{"program":"{}","arguments":[],"env":[],"input_env":[]}},"outputs":["result"]}}]}})",
        workflow, program);
    request.body.assign(body.begin(), body.end());
    auto response =
        sync_wait_on_runtime(app.runtime(), invoke(std::move(request)));
    if (!response || response->status != http::HttpStatus::Created) {
      return fail(Error::Unknown);
    }
    auto parsed = parse_json(response_text(*response));
    if (!parsed || !parsed->is_object()) {
      return fail(Error::ParseError);
    }
    const auto it = parsed->get_object().find("plan_id");
    if (it == parsed->get_object().end() || !it->second.is_string()) {
      return fail(Error::ParseError);
    }
    return ok(it->second.as<std::string>());
  };

  auto first_plan = register_plan("api-flow", "/bin/true");
  auto second_plan = register_plan("api-flow", "/bin/echo");
  ASSERT_TRUE(first_plan.has_value());
  ASSERT_TRUE(second_plan.has_value());

  http::HttpRequest get_plan;
  get_plan.method = http::HttpMethod::GET;
  get_plan.path =
      std::format("/api/v1/workflows/plans/{}", first_plan.value());
  auto fetched_plan =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(get_plan)));
  ASSERT_TRUE(fetched_plan.has_value());
  ASSERT_EQ(fetched_plan->status, http::HttpStatus::Ok);
  auto fetched_plan_body = parse_json(response_text(*fetched_plan));
  ASSERT_TRUE(fetched_plan_body.has_value());
  EXPECT_EQ(fetched_plan_body->get_object()
                .at("plan")
                .get_object()
                .at("workflow_id")
                .as<std::string>(),
            "api-flow");

  http::HttpRequest list;
  list.method = http::HttpMethod::GET;
  list.path = "/api/v1/workflows/plans";
  list.query_string = "offset=1&limit=1";
  auto listed = sync_wait_on_runtime(app.runtime(), invoke(std::move(list)));
  ASSERT_TRUE(listed.has_value());
  ASSERT_EQ(listed->status, http::HttpStatus::Ok);
  auto list_body = parse_json(response_text(*listed));
  ASSERT_TRUE(list_body.has_value());
  EXPECT_EQ(list_body->get_object().at("total").as<std::int64_t>(), 2);
  EXPECT_EQ(list_body->get_object().at("plans").get_array().size(), 1U);

  http::HttpRequest start;
  start.method = http::HttpMethod::POST;
  start.path = "/api/v1/workflows/api-flow/runs";
  const auto start_body =
      std::format(R"({{"plan_id":"{}"}})", first_plan.value());
  start.body.assign(start_body.begin(), start_body.end());
  auto started = sync_wait_on_runtime(app.runtime(), invoke(std::move(start)));
  ASSERT_TRUE(started.has_value());
  ASSERT_EQ(started->status, http::HttpStatus::Accepted);
  auto started_body = parse_json(response_text(*started));
  ASSERT_TRUE(started_body.has_value());
  EXPECT_EQ(started_body->get_object().at("plan_id").as<std::string>(),
            first_plan.value());

  http::HttpRequest upload;
  upload.method = http::HttpMethod::POST;
  upload.path = "/api/v1/artifacts";
  upload.headers.set("Content-Type", "text/plain");
  upload.body = {'a', 'b', 'c'};
  auto uploaded = sync_wait_on_runtime(app.runtime(), invoke(std::move(upload)));
  ASSERT_TRUE(uploaded.has_value());
  ASSERT_EQ(uploaded->status, http::HttpStatus::Created);
  auto upload_body = parse_json(response_text(*uploaded));
  ASSERT_TRUE(upload_body.has_value());
  const auto artifact_id =
      upload_body->get_object().at("artifact_id").as<std::string>();

  http::HttpRequest download;
  download.method = http::HttpMethod::GET;
  download.path = std::format("/api/v1/artifacts/{}", artifact_id);
  auto downloaded =
      sync_wait_on_runtime(app.runtime(), invoke(std::move(download)));
  ASSERT_TRUE(downloaded.has_value());
  ASSERT_EQ(downloaded->status, http::HttpStatus::Ok);
  EXPECT_EQ(response_text(*downloaded), "abc");

  http::HttpRequest erase;
  erase.method = http::HttpMethod::DELETE;
  erase.path = std::format("/api/v1/artifacts/{}", artifact_id);
  auto erased = sync_wait_on_runtime(app.runtime(), invoke(std::move(erase)));
  ASSERT_TRUE(erased.has_value());
  EXPECT_EQ(erased->status, http::HttpStatus::Ok);
  app.stop();
}

TEST(ApiTest, WorkflowRoutesReturnUnavailableWhenWorkflowSubsystemIsDisabled) {
  SystemConfig config;
  config.api.enabled = false;
  config.workflow.enabled = false;
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());
  ASSERT_EQ(app.workflow_runtime(), nullptr);
  ASSERT_EQ(app.workflow_control_plane(), nullptr);

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  api_detail::HttpMetricsRegistry metrics;
  api_detail::ApiContext context{
      .app = app,
      .server = server,
      .http_active_requests = active,
      .http_metrics = metrics,
  };
  api_detail::register_workflow_routes(context);
  const auto invoke = [&](http::HttpMethod method, std::string path)
      -> http::HttpResponse {
    http::HttpRequest request;
    request.method = method;
    request.path = std::move(path);
    auto response = sync_wait_on_runtime(
        app.runtime(), [&]() -> task<Result<http::HttpResponse>> {
          co_return ok(co_await server.router().route(std::move(request)));
        }());
    EXPECT_TRUE(response.has_value());
    return response ? std::move(*response) : http::HttpResponse::internal_error();
  };

  for (const auto &[method, path] :
       std::vector<std::pair<http::HttpMethod, std::string>>{
           {http::HttpMethod::POST, "/api/v1/workflows/plans"},
           {http::HttpMethod::GET, "/api/v1/workflows/plans"},
           {http::HttpMethod::GET,
            "/api/v1/workflows/plans/missing-plan"},
           {http::HttpMethod::POST, "/api/v1/workflows/flow/runs"},
           {http::HttpMethod::GET, "/api/v1/workflow-runs/run"},
           {http::HttpMethod::GET,
            "/api/v1/workflow-runs/run/outputs/node/port"},
           {http::HttpMethod::GET,
            "/api/v1/workflow-runs/run/evidence"},
           {http::HttpMethod::GET,
            "/api/v1/workflow-runs/run/failures"},
           {http::HttpMethod::POST,
            "/api/v1/workflow-runs/run/repairs"},
           {http::HttpMethod::POST, "/api/v1/artifacts"},
           {http::HttpMethod::GET, "/api/v1/artifacts/artifact"},
           {http::HttpMethod::DELETE, "/api/v1/artifacts/artifact"},
           {http::HttpMethod::POST,
            "/api/v1/workflow-runs/run/pause"},
           {http::HttpMethod::POST,
            "/api/v1/workflow-runs/run/resume"},
           {http::HttpMethod::POST,
            "/api/v1/workflow-runs/run/cancel"},
       }) {
    EXPECT_EQ(invoke(method, path).status,
              http::HttpStatus::ServiceUnavailable)
        << path;
  }
  app.stop();
}

TEST(ApiTest, WorkflowRoutesCoverValidationLifecycleEvidenceAndOutputs) {
  SystemConfig config;
  config.api.enabled = false;
  config.admission.allowed_executors = {"command"};
  config.executors.command.policy.allowed_programs = {
      "/bin/true", "/bin/echo", "/bin/sh"};
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  api_detail::HttpMetricsRegistry metrics;
  api_detail::ApiContext context{
      .app = app,
      .server = server,
      .http_active_requests = active,
      .http_metrics = metrics,
  };
  api_detail::register_workflow_routes(context);

  const auto invoke = [&](http::HttpRequest request) -> http::HttpResponse {
    auto response = sync_wait_on_runtime(
        app.runtime(), [&]() -> task<Result<http::HttpResponse>> {
          co_return ok(co_await server.router().route(std::move(request)));
        }());
    EXPECT_TRUE(response.has_value());
    return response ? std::move(*response) : http::HttpResponse::internal_error();
  };
  const auto request = [](http::HttpMethod method, std::string path,
                          std::string body = {}, std::string query = {}) {
    http::HttpRequest value;
    value.method = method;
    value.path = std::move(path);
    value.query_string = std::move(query);
    value.body.assign(body.begin(), body.end());
    return value;
  };
  const auto register_plan = [&](std::string body) -> JsonValue {
    auto response = invoke(request(http::HttpMethod::POST,
                                   "/api/v1/workflows/plans",
                                   std::move(body)));
    EXPECT_EQ(response.status, http::HttpStatus::Created)
        << response_text(response);
    auto parsed = parse_json(response_text(response));
    EXPECT_TRUE(parsed.has_value());
    return parsed ? std::move(*parsed) : JsonValue::object_t{};
  };
  const auto start_run = [&](std::string workflow, std::string body = {}) {
    auto response = invoke(request(
        http::HttpMethod::POST,
        std::format("/api/v1/workflows/{}/runs", workflow), std::move(body)));
    EXPECT_EQ(response.status, http::HttpStatus::Accepted)
        << response_text(response);
    auto parsed = parse_json(response_text(response));
    EXPECT_TRUE(parsed.has_value());
    if (!parsed || !parsed->is_object()) {
      return std::string{};
    }
    const auto found = parsed->get_object().find("run_id");
    return found != parsed->get_object().end() && found->second.is_string()
               ? found->second.as<std::string>()
               : std::string{};
  };
  const auto snapshot = [&](std::string_view run_id) {
    return invoke(request(
        http::HttpMethod::GET,
        std::format("/api/v1/workflow-runs/{}", run_id)));
  };
  const auto wait_for_state = [&](std::string_view run_id,
                                  std::string_view expected,
                                  std::chrono::seconds timeout =
                                      std::chrono::seconds(3)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    http::HttpResponse latest;
    while (std::chrono::steady_clock::now() < deadline) {
      latest = snapshot(run_id);
      if (latest.status == http::HttpStatus::Ok) {
        auto body = parse_json(response_text(latest));
        if (body && body->get_object().at("state").as<std::string>() ==
                        expected) {
          return latest;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return latest;
  };

  auto malformed = invoke(request(http::HttpMethod::POST,
                                  "/api/v1/workflows/plans", "not-json"));
  EXPECT_EQ(malformed.status, http::HttpStatus::BadRequest);
  auto malformed_body = parse_json(response_text(malformed));
  ASSERT_TRUE(malformed_body.has_value());
  const auto &malformed_error =
      malformed_body->get_object().at("error").get_object();
  EXPECT_EQ(malformed_error.at("kind").as<std::string>(),
            "invalid_argument");
  EXPECT_EQ(malformed_error.at("code").as<std::string>(),
            "invalid_request");
  auto invalid_plan = invoke(request(
      http::HttpMethod::POST, "/api/v1/workflows/plans",
      R"({"workflow_id":"invalid","schema_version":1,"nodes":[{"id":"task","executor":"unknown","outputs":["result"]}]})"));
  EXPECT_NE(invalid_plan.status, http::HttpStatus::Created);

  const auto echo_plan = register_plan(R"({
    "workflow_id":"route-echo","schema_version":1,
    "nodes":[{"id":"command","executor":"command",
      "config":{"program":"/bin/echo","arguments":["api-output"],"env":[],"input_env":[]},
      "outputs":["stdout","stderr","exit_code","result"],"checkpoint":true}],
    "outputs":[{"node":"command","port":"result"}]
  })");
  const auto true_plan = register_plan(R"({
    "workflow_id":"route-true","schema_version":1,
    "nodes":[{"id":"command","executor":"command",
      "config":{"program":"/bin/true","arguments":[],"env":[],"input_env":[]},
      "outputs":["result"]}]
  })");
  const auto echo_plan_id =
      echo_plan.get_object().at("plan_id").as<std::string>();
  const auto true_plan_id =
      true_plan.get_object().at("plan_id").as<std::string>();

  auto invalid_page = invoke(request(http::HttpMethod::GET,
                                     "/api/v1/workflows/plans", {},
                                     "offset=bad&limit=0"));
  ASSERT_EQ(invalid_page.status, http::HttpStatus::Ok);
  auto invalid_page_body = parse_json(response_text(invalid_page));
  ASSERT_TRUE(invalid_page_body.has_value());
  EXPECT_EQ(invalid_page_body->get_object().at("offset").as<std::int64_t>(),
            0);
  EXPECT_EQ(invalid_page_body->get_object().at("limit").as<std::int64_t>(),
            1);
  auto beyond_page = invoke(request(http::HttpMethod::GET,
                                    "/api/v1/workflows/plans", {},
                                    "offset=9999&limit=5000"));
  ASSERT_EQ(beyond_page.status, http::HttpStatus::Ok);
  auto beyond_body = parse_json(response_text(beyond_page));
  ASSERT_TRUE(beyond_body.has_value());
  EXPECT_TRUE(beyond_body->get_object().at("plans").get_array().empty());
  EXPECT_EQ(beyond_body->get_object().at("limit").as<std::int64_t>(), 1000);

  EXPECT_EQ(invoke(request(http::HttpMethod::POST,
                           "/api/v1/workflows/route-echo/runs", "not-json"))
                .status,
            http::HttpStatus::BadRequest);
  EXPECT_EQ(invoke(request(http::HttpMethod::POST,
                           "/api/v1/workflows/route-echo/runs", "[]"))
                .status,
            http::HttpStatus::BadRequest);
  EXPECT_EQ(invoke(request(http::HttpMethod::POST,
                           "/api/v1/workflows/missing/runs"))
                .status,
            http::HttpStatus::NotFound);
  EXPECT_EQ(
      invoke(request(
                 http::HttpMethod::POST,
                 "/api/v1/workflows/route-echo/runs",
                 std::format(R"({{"plan_id":"{}"}})", true_plan_id)))
          .status,
      http::HttpStatus::BadRequest);

  auto start_request = request(
      http::HttpMethod::POST, "/api/v1/workflows/route-echo/runs",
      R"({"source":"api-test","event_type":"manual","payload":{"key":"value"},"principal":{"subject":"tester","roles":["admin",7]},"idempotency_key":"route-key"})");
  start_request.headers.set("Idempotency-Key", "header-fallback");
  auto started = invoke(std::move(start_request));
  ASSERT_EQ(started.status, http::HttpStatus::Accepted)
      << response_text(started);
  auto started_body = parse_json(response_text(started));
  ASSERT_TRUE(started_body.has_value());
  const auto run_id =
      started_body->get_object().at("run_id").as<std::string>();
  EXPECT_EQ(started_body->get_object().at("plan_id").as<std::string>(),
            echo_plan_id);

  auto duplicate = invoke(request(
      http::HttpMethod::POST, "/api/v1/workflows/route-echo/runs",
      R"({"idempotency_key":"route-key"})"));
  ASSERT_EQ(duplicate.status, http::HttpStatus::Accepted);
  auto duplicate_body = parse_json(response_text(duplicate));
  ASSERT_TRUE(duplicate_body.has_value());
  EXPECT_EQ(duplicate_body->get_object().at("run_id").as<std::string>(),
            run_id);

  const auto revised_echo_plan = register_plan(R"({
    "workflow_id":"route-echo","schema_version":1,
    "nodes":[{"id":"command","executor":"command",
      "config":{"program":"/bin/echo","arguments":["api-output-v2"],"env":[],"input_env":[]},
      "outputs":["result"]}]
  })");
  const auto revised_echo_plan_id =
      revised_echo_plan.get_object().at("plan_id").as<std::string>();

  auto idempotency_conflict = invoke(request(
      http::HttpMethod::POST, "/api/v1/workflows/route-echo/runs",
      std::format(
          R"({{"plan_id":"{}","idempotency_key":"route-key"}})",
          revised_echo_plan_id)));
  ASSERT_EQ(idempotency_conflict.status, http::HttpStatus::Conflict)
      << response_text(idempotency_conflict);
  auto conflict_body = parse_json(response_text(idempotency_conflict));
  ASSERT_TRUE(conflict_body.has_value());
  const auto &conflict_error =
      conflict_body->get_object().at("error").get_object();
  EXPECT_EQ(conflict_error.at("kind").as<std::string>(), "already_exists");
  EXPECT_EQ(conflict_error.at("code").as<std::string>(), "already_exists");

  auto completed = wait_for_state(run_id, "succeeded");
  ASSERT_EQ(completed.status, http::HttpStatus::Ok)
      << response_text(completed);
  auto completed_body = parse_json(response_text(completed));
  ASSERT_TRUE(completed_body.has_value());
  ASSERT_EQ(completed_body->get_object().at("tasks").get_array().size(), 1U);
  EXPECT_FALSE(completed_body->get_object()
                   .at("tasks")
                   .get_array()
                   .front()
                   .get_object()
                   .at("attempts")
                   .get_array()
                   .empty());

  for (const auto &[port, expected] :
       std::vector<std::pair<std::string, std::string>>{
           {"result", "api-output\n"}, {"stdout", "api-output\n"}}) {
    auto output = invoke(request(
        http::HttpMethod::GET,
        std::format("/api/v1/workflow-runs/{}/outputs/command/{}", run_id,
                    port)));
    ASSERT_EQ(output.status, http::HttpStatus::Ok) << response_text(output);
    auto body = parse_json(response_text(output));
    ASSERT_TRUE(body.has_value());
    EXPECT_EQ(body->get_object().at("value").as<std::string>(), expected);
  }
  auto exit_code = invoke(request(
      http::HttpMethod::GET,
      std::format("/api/v1/workflow-runs/{}/outputs/command/exit_code",
                  run_id)));
  ASSERT_EQ(exit_code.status, http::HttpStatus::Ok);
  auto exit_body = parse_json(response_text(exit_code));
  ASSERT_TRUE(exit_body.has_value());
  EXPECT_EQ(exit_body->get_object().at("value").as<std::int64_t>(), 0);
  EXPECT_EQ(invoke(request(
                           http::HttpMethod::GET,
                           std::format("/api/v1/workflow-runs/{}/outputs/command/missing",
                                       run_id)))
                .status,
            http::HttpStatus::NotFound);

  auto evidence = invoke(request(
      http::HttpMethod::GET,
      std::format("/api/v1/workflow-runs/{}/evidence", run_id), {},
      "offset=0&limit=2"));
  ASSERT_EQ(evidence.status, http::HttpStatus::Ok);
  auto evidence_body = parse_json(response_text(evidence));
  ASSERT_TRUE(evidence_body.has_value());
  EXPECT_GT(evidence_body->get_object().at("total").as<std::int64_t>(), 0);
  EXPECT_LE(evidence_body->get_object().at("evidence").get_array().size(), 2U);
  ASSERT_FALSE(
      evidence_body->get_object().at("evidence").get_array().empty());
  EXPECT_TRUE(evidence_body->get_object()
                  .at("evidence")
                  .get_array()
                  .front()
                  .get_object()
                  .at("type")
                  .is_string());

  register_plan(R"({
    "workflow_id":"route-failure","schema_version":1,
    "nodes":[{"id":"command","executor":"command",
      "config":{"program":"/bin/sh","arguments":["-c","printf partial; printf diagnostic >&2; exit 7"],"env":[],"input_env":[]},
      "outputs":["result"]}]
  })");
  const auto failed_run = start_run("route-failure");
  auto failed_response = wait_for_state(failed_run, "failed");
  ASSERT_EQ(failed_response.status, http::HttpStatus::Ok)
      << response_text(failed_response);
  auto failed_body = parse_json(response_text(failed_response));
  ASSERT_TRUE(failed_body.has_value());
  const auto &failed_object = failed_body->get_object();
  ASSERT_TRUE(failed_object.contains("failure"));
  EXPECT_FALSE(failed_object.contains("error"));
  const auto &run_failure = failed_object.at("failure").get_object();
  EXPECT_EQ(run_failure.at("kind").as<std::string>(), "unknown");
  EXPECT_EQ(run_failure.at("code").as<std::string>(),
            "command_exit_nonzero");
  const auto &failed_task =
      failed_object.at("tasks").get_array().front().get_object();
  ASSERT_TRUE(failed_task.contains("failure"));
  EXPECT_FALSE(failed_task.contains("last_error"));
  const auto &failed_attempt =
      failed_task.at("attempts").get_array().front().get_object();
  ASSERT_TRUE(failed_attempt.contains("failure"));
  EXPECT_FALSE(failed_attempt.contains("error"));
  const auto &failure_details =
      failed_attempt.at("failure").get_object().at("details").get_object();
  EXPECT_EQ(failure_details.at("exit_code").as<std::int64_t>(), 7);
  EXPECT_EQ(failure_details.at("stdout").as<std::string>(), "partial");
  EXPECT_EQ(failure_details.at("stderr").as<std::string>(), "diagnostic");

  auto failure_report = invoke(request(
      http::HttpMethod::GET,
      std::format("/api/v1/workflow-runs/{}/failures", failed_run)));
  ASSERT_EQ(failure_report.status, http::HttpStatus::Ok)
      << response_text(failure_report);
  auto failure_report_body = parse_json(response_text(failure_report));
  ASSERT_TRUE(failure_report_body.has_value());
  EXPECT_EQ(failure_report_body->get_object()
                .at("failure")
                .get_object()
                .at("code")
                .as<std::string>(),
            "command_exit_nonzero");
  ASSERT_EQ(failure_report_body->get_object().at("tasks").get_array().size(),
            1U);

  auto repair_response = invoke(request(
      http::HttpMethod::POST,
      std::format("/api/v1/workflow-runs/{}/repairs", failed_run),
      R"({
        "reason":"replace failing command",
        "idempotency_key":"route-repair-once",
        "plan":{
          "workflow_id":"route-failure","schema_version":1,
          "nodes":[{"id":"command","executor":"command",
            "config":{"program":"/bin/echo","arguments":["repaired"],"env":[],"input_env":[]},
            "outputs":["result"]}]
        }
      })"));
  ASSERT_EQ(repair_response.status, http::HttpStatus::Accepted)
      << response_text(repair_response);
  auto repair_body = parse_json(response_text(repair_response));
  ASSERT_TRUE(repair_body.has_value());
  const auto repaired_run =
      repair_body->get_object().at("run_id").as<std::string>();
  EXPECT_EQ(repair_body->get_object()
                .at("parent_run_id")
                .as<std::string>(),
            failed_run);
  ASSERT_EQ(repair_body->get_object().at("nodes").get_array().size(), 1U);
  EXPECT_FALSE(repair_body->get_object()
                   .at("nodes")
                   .get_array()
                   .front()
                   .get_object()
                   .at("reused")
                   .get<bool>());
  auto repaired_response = wait_for_state(repaired_run, "succeeded");
  ASSERT_EQ(repaired_response.status, http::HttpStatus::Ok)
      << response_text(repaired_response);
  auto repaired_body = parse_json(response_text(repaired_response));
  ASSERT_TRUE(repaired_body.has_value());
  EXPECT_EQ(repaired_body->get_object()
                .at("parent_run_id")
                .as<std::string>(),
            failed_run);
  EXPECT_EQ(repaired_body->get_object()
                .at("repair_revision")
                .as<std::int64_t>(),
            1);

  auto failure_evidence = invoke(request(
      http::HttpMethod::GET,
      std::format("/api/v1/workflow-runs/{}/evidence", failed_run), {},
      "offset=0&limit=100"));
  ASSERT_EQ(failure_evidence.status, http::HttpStatus::Ok);
  auto failure_evidence_body = parse_json(response_text(failure_evidence));
  ASSERT_TRUE(failure_evidence_body.has_value());
  const auto &failure_records =
      failure_evidence_body->get_object().at("evidence").get_array();
  const auto task_failed = std::ranges::find_if(
      failure_records, [](const JsonValue &record) {
        const auto &object = record.get_object();
        return object.at("type").as<std::string>() == "task_failed";
      });
  ASSERT_NE(task_failed, failure_records.end());
  ASSERT_TRUE(task_failed->get_object()
                  .at("metadata")
                  .get_object()
                  .contains("failure"));

  EXPECT_EQ(snapshot("missing-run").status, http::HttpStatus::NotFound);
  for (std::string_view operation : {"pause", "resume", "cancel"}) {
    EXPECT_EQ(invoke(request(
                             http::HttpMethod::POST,
                             std::format("/api/v1/workflow-runs/missing-run/{}",
                                         operation)))
                  .status,
              http::HttpStatus::NotFound);
  }
  EXPECT_EQ(invoke(request(http::HttpMethod::GET,
                           "/api/v1/artifacts/missing-artifact"))
                .status,
            http::HttpStatus::NotFound);
  EXPECT_EQ(invoke(request(http::HttpMethod::DELETE,
                           "/api/v1/artifacts/missing-artifact"))
                .status,
            http::HttpStatus::NotFound);

  register_plan(R"({
    "workflow_id":"route-pause","schema_version":1,
    "nodes":[
      {"id":"first","executor":"command",
       "config":{"program":"/bin/sh","arguments":["-c","sleep 1; printf first"],"env":[],"input_env":[]},
       "outputs":["result"]},
      {"id":"second","executor":"command",
       "config":{"program":"/bin/echo","arguments":["second"],"env":[],"input_env":[]},
       "outputs":["result"]}
    ],
    "edges":[{"source_node":"first","source_port":"result","target":"second","condition":{"kind":"always"}}]
  })");
  const auto paused_run = start_run("route-pause");
  auto pause = invoke(request(
      http::HttpMethod::POST,
      std::format("/api/v1/workflow-runs/{}/pause", paused_run)));
  ASSERT_EQ(pause.status, http::HttpStatus::Accepted) << response_text(pause);
  auto paused = wait_for_state(paused_run, "paused", std::chrono::seconds(4));
  ASSERT_EQ(paused.status, http::HttpStatus::Ok) << response_text(paused);
  auto resume = invoke(request(
      http::HttpMethod::POST,
      std::format("/api/v1/workflow-runs/{}/resume", paused_run)));
  ASSERT_EQ(resume.status, http::HttpStatus::Accepted) << response_text(resume);
  auto resumed =
      wait_for_state(paused_run, "succeeded", std::chrono::seconds(4));
  ASSERT_EQ(resumed.status, http::HttpStatus::Ok) << response_text(resumed);

  register_plan(R"({
    "workflow_id":"route-cancel","schema_version":1,
    "nodes":[{"id":"slow","executor":"command",
      "config":{"program":"/bin/sh","arguments":["-c","sleep 5"],"env":[],"input_env":[]},
      "outputs":["result"]}]
  })");
  const auto cancelled_run = start_run("route-cancel");
  auto cancel = invoke(request(
      http::HttpMethod::POST,
      std::format("/api/v1/workflow-runs/{}/cancel", cancelled_run)));
  ASSERT_EQ(cancel.status, http::HttpStatus::Accepted) << response_text(cancel);
  auto cancelled =
      wait_for_state(cancelled_run, "cancelled", std::chrono::seconds(3));
  ASSERT_EQ(cancelled.status, http::HttpStatus::Ok) << response_text(cancelled);

  app.stop();
}
