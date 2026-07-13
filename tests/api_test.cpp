#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"
#include "dagforge/app/http/http_server.hpp"
#include "dagforge/core/sync_wait.hpp"

#include "../src/dagforge/app/api/detail/api_context.hpp"

#include "gtest/gtest.h"

#include <atomic>

using namespace dagforge;

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

TEST(ApiTest, AccessPolicyAuthenticatesAndLimitsRequests) {
  SystemConfig config;
  config.api.enabled = false;
  Application app(std::move(config));
  ASSERT_TRUE(app.start().has_value());

  http::HttpServer server(app.runtime());
  std::atomic<std::uint64_t> active{0};
  detail::HttpMetricsRegistry metrics;
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
