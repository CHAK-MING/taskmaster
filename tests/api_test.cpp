#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/plan_compiler.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"

#include "../src/dagforge/app/api/detail/api_context.hpp"
#include "../src/dagforge/app/api/detail/routes/workflows.hpp"

#include "gtest/gtest.h"

#include <atomic>

using namespace dagforge;

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

  app.config().http_executor.enabled = true;
  app.config().http_executor.allowed_origins = {"https://example.com"};
  ASSERT_TRUE(app.init().has_value());
  auto enabled_plan = make_plan();
  ASSERT_TRUE(enabled_plan.has_value());
  auto enabled =
      app.workflow_control_plane()->register_plan(std::move(*enabled_plan));
  ASSERT_TRUE(enabled.has_value()) << enabled.error().message();

  app.config().http_executor.enabled = false;
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
  config.sandbox.allowed_programs = {"/bin/true", "/bin/echo"};
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

TEST(ApiTest, WorkflowRoutesSupportPaginationPlanSelectionAndArtifacts) {
  SystemConfig config;
  config.api.enabled = false;
  config.admission.allowed_executors = {"command"};
  config.sandbox.allowed_programs = {"/bin/true", "/bin/echo"};
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
