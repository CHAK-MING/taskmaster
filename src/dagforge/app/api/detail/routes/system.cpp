#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "system.hpp"
#include "dagforge/app/metrics_exporter.hpp"
#include "dagforge/util/time.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

namespace dagforge::api_detail {

auto register_system_routes(ApiContext &ctx) -> void {
  auto &router = ctx.router();

  router.get("/api/health",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/health",
                 [](http::HttpRequest) -> task<http::HttpResponse> {
                   co_return json_response({{"status", "healthy"}});
                 }));

  router.get("/api/status",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/status",
                 [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
                   const auto *workflow = ctx.app.workflow_runtime();
                   co_return json_response({
                       {"runtime", ctx.app.is_running() ? "running" : "stopped"},
                       {"workflow_enabled", workflow != nullptr},
                       {"active_workflow_runs",
                        workflow != nullptr ? workflow->active_run_count() : 0},
                       {"shards", ctx.app.runtime().shard_count()},
                       {"timestamp", util::format_timestamp()},
                   });
                 }));

  router.get(
      "/metrics",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/metrics",
          [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
            co_return text_response(render_prometheus_metrics(ctx.app),
                                    http::HttpStatus::Ok,
                                    "text/plain; version=0.0.4; charset=utf-8");
          }));
}

} // namespace dagforge::api_detail
