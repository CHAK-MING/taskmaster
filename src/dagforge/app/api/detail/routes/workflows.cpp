#include "workflows.hpp"

#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "workflow_http_adapter.hpp"
#include "workflow_http_contract.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <ranges>
#include <span>
#include <utility>
#include <vector>

namespace dagforge::api_detail {

auto register_workflow_routes(ApiContext &ctx) -> void {
  using namespace workflow_routes_detail;
  using namespace workflow_contract;
  auto &router = ctx.router();

  router.get("/api/v1/capabilities",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/v1/capabilities",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *control = request.require_control_plane();
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto capabilities = control->capabilities();
                   if (!capabilities) {
                     co_return result_error_response(capabilities.error());
                   }
                   co_return typed_json_response(*capabilities);
                 }));

  router.post("/api/v1/workflows/plans",
              ctx.make_instrumented_route(
                  http::HttpMethod::POST, "/api/v1/workflows/plans",
                  [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                    WorkflowHttpRequest request{ctx.app, req};
                    auto *control = request.require_control_plane();
                    if (request.failed()) {
                      co_return request.take_failure();
                    }
                    auto plan = workflow::WorkflowPlanLoader::from_json(
                        req.body_as_string());
                    if (!plan) {
                      co_return error_response(400, plan.error().message());
                    }
                    auto compiled = control->register_plan(std::move(*plan));
                    if (!compiled) {
                      co_return plan_error_response(compiled.error());
                    }
                    const auto summary = plan_summary(**compiled);
                    co_return typed_json_response(
                        glz::obj{"workflow_id", summary.workflow_id, "plan_id",
                                 summary.plan_id, "digest", summary.digest,
                                 "nodes", summary.nodes, "durability_deferred",
                                 compiled->durability_deferred},
                        http::HttpStatus::Created);
                  }));

  router.get("/api/v1/workflows/plans",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/v1/workflows/plans",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *control = request.require_control_plane();
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   const auto page = request.page();
                   auto registered = control->list_plans();
                   std::ranges::sort(registered, {}, [](const auto &plan) {
                     return plan->plan_id.str();
                   });
                   const auto begin = std::min(page.offset, registered.size());
                   const auto end =
                       std::min(begin + page.limit, registered.size());
                   std::vector<PlanSummary> plans;
                   plans.reserve(end - begin);
                   for (std::size_t index = begin; index < end; ++index) {
                     plans.push_back(plan_summary(*registered[index]));
                   }
                   co_return typed_json_response(
                       glz::obj{"plans", plans, "total", registered.size(),
                                "offset", page.offset, "limit", page.limit});
                 }));

  router.get("/api/v1/workflows/plans/{plan_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/v1/workflows/plans/{plan_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *control = request.require_control_plane();
                   auto plan_id =
                       request.require_path_id<WorkflowPlanId>("plan_id");
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto execution = control->get_plan(*plan_id);
                   if (!execution) {
                     co_return result_error_response(execution.error());
                   }
                   const auto plan = workflow::source_plan(**execution);
                   co_return typed_json_response(
                       glz::obj{"plan_id", (*execution)->plan_id, "digest",
                                (*execution)->digest, "plan", plan});
                 }));

  router.post(
      "/api/v1/workflows/{workflow_id}/runs",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflows/{workflow_id}/runs",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *control = request.require_control_plane();
            auto *runtime = request.require_runtime();
            auto workflow_id =
                request.require_path_id<WorkflowId>("workflow_id");
            auto body = request.parse_json_or_default<StartRunRequest>(
                "Invalid JSON body");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto plan = body->plan_id ? control->get_plan(*body->plan_id)
                                      : control->get_latest(*workflow_id);
            if (!plan) {
              co_return result_error_response(plan.error());
            }
            if ((*plan)->workflow_id != *workflow_id) {
              co_return error_response(400,
                                       "plan_id does not belong to workflow");
            }
            auto idempotency_key =
                request.idempotency_key(std::move(body->idempotency_key));
            workflow::WorkflowValue payload;
            if (body->payload) {
              payload = std::move(*body->payload);
            }
            auto started = runtime->start(
                *plan, workflow::TriggerEnvelope{
                           .workflow_id = std::move(*workflow_id),
                           .source = std::move(body->source),
                           .event_type = std::move(body->event_type),
                           .payload = std::move(payload),
                           .idempotency_key = std::move(idempotency_key),
                           .principal = std::move(body->principal),
                           .trace = std::move(body->trace),
                           .occurred_at = std::chrono::system_clock::now(),
                       });
            if (!started) {
              co_return result_error_response(started.error());
            }
            co_return typed_json_response(
                glz::obj{"run_id", *started, "workflow_id",
                         (*plan)->workflow_id, "plan_id", (*plan)->plan_id},
                http::HttpStatus::Accepted);
          }));

  router.get("/api/v1/workflow-runs/{run_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/v1/workflow-runs/{run_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *runtime = request.require_runtime();
                   auto run_id =
                       request.require_path_id<WorkflowRunId>("run_id");
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto snapshot = co_await runtime->snapshot(*run_id);
                   if (!snapshot) {
                     co_return result_error_response(snapshot.error());
                   }
                   co_return typed_json_response(**snapshot);
                 }));

  router.get(
      "/api/v1/workflow-runs/{run_id}/failures",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/workflow-runs/{run_id}/failures",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            auto run_id = request.require_path_id<WorkflowRunId>("run_id");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto report = co_await runtime->failure_report(*run_id);
            if (!report) {
              co_return result_error_response(report.error());
            }
            co_return typed_json_response(*report);
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/repairs",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflow-runs/{run_id}/repairs",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *control = request.require_control_plane();
            auto *runtime = request.require_runtime();
            auto parent_run_id =
                request.require_path_id<WorkflowRunId>("run_id");
            auto body = request.require_json<RepairRunRequest>(
                "Invalid repair JSON body");
            if (request.failed()) {
              co_return request.take_failure();
            }
            if (!body->plan) {
              co_return error_response(400, "Repair body requires plan");
            }
            auto compiled = control->register_plan(std::move(*body->plan));
            if (!compiled) {
              co_return plan_error_response(compiled.error());
            }
            auto idempotency_key =
                request.idempotency_key(std::move(body->idempotency_key));
            auto started = runtime->repair(
                *compiled, *parent_run_id,
                workflow::RepairRequest{
                    .reason = std::move(body->reason),
                    .idempotency_key = std::move(idempotency_key),
                });
            if (!started) {
              co_return result_error_response(started.error());
            }
            co_return typed_json_response(*started, http::HttpStatus::Accepted);
          }));

  router.get("/api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET,
                 "/api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *runtime = request.require_runtime();
                   auto run_id = request.require_path_id<WorkflowRunId>(
                       "run_id", "Missing output path parameter");
                   auto node_id = request.require_path_id<WorkflowNodeId>(
                       "node_id", "Missing output path parameter");
                   auto port = request.require_path_id<WorkflowPortId>(
                       "port", "Missing output path parameter");
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto value = co_await runtime->output(
                       *run_id,
                       workflow::OutputRef{.node_id = std::move(*node_id),
                                           .port = std::move(*port)});
                   if (!value) {
                     co_return result_error_response(value.error());
                   }
                   const auto response = workflow_value_response(**value);
                   co_return typed_json_response(glz::obj{"value", response});
                 }));

  router.get(
      "/api/v1/workflow-runs/{run_id}/evidence",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/workflow-runs/{run_id}/evidence",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            auto run_id = request.require_path_id<WorkflowRunId>("run_id");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto records = runtime->evidence(*run_id);
            const auto page = request.page();
            const auto begin = std::min(page.offset, records.size());
            const auto end = std::min(begin + page.limit, records.size());
            std::vector<EvidenceResponseRecord> evidence;
            evidence.reserve(end - begin);
            for (std::size_t index = begin; index < end; ++index) {
              evidence.push_back(evidence_response(records[index]));
            }
            co_return typed_json_response(
                glz::obj{"evidence", evidence, "total", records.size(),
                         "offset", page.offset, "limit", page.limit});
          }));

  router.post(
      "/api/v1/artifacts",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/artifacts",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            if (request.failed()) {
              co_return request.take_failure();
            }
            const auto media_type =
                req.header("Content-Type").value_or("application/octet-stream");
            const auto data = std::span<const std::byte>{
                reinterpret_cast<const std::byte *>(req.body.data()),
                req.body.size()};
            auto stored = runtime->artifact_store().put(data, media_type);
            if (!stored) {
              co_return result_error_response(stored.error());
            }
            co_return typed_json_response(
                glz::obj{"artifact_id", stored->artifact_id, "media_type",
                         stored->media_type, "size_bytes", stored->size_bytes,
                         "digest", stored->digest, "durability_deferred",
                         stored->durability_deferred},
                http::HttpStatus::Created);
          }));

  router.get("/api/v1/artifacts/{artifact_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/v1/artifacts/{artifact_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *runtime = request.require_runtime();
                   auto artifact_id =
                       request.require_path_id<ArtifactId>("artifact_id");
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto artifact = runtime->artifact_store().get(*artifact_id);
                   if (!artifact) {
                     co_return result_error_response(artifact.error());
                   }
                   http::HttpResponse response{.status = http::HttpStatus::Ok};
                   response.headers.set("Content-Type",
                                        artifact->ref.media_type);
                   response.headers.set("ETag", artifact->ref.digest);
                   response.body.resize(artifact->data.size());
                   std::memcpy(response.body.data(), artifact->data.data(),
                               artifact->data.size());
                   co_return response;
                 }));

  router.del("/api/v1/artifacts/{artifact_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::DELETE, "/api/v1/artifacts/{artifact_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   WorkflowHttpRequest request{ctx.app, req};
                   auto *runtime = request.require_runtime();
                   auto artifact_id =
                       request.require_path_id<ArtifactId>("artifact_id");
                   if (request.failed()) {
                     co_return request.take_failure();
                   }
                   auto erased = runtime->artifact_store().erase(*artifact_id);
                   if (!erased) {
                     co_return result_error_response(erased.error());
                   }
                   co_return typed_json_response(
                       glz::obj{"status", "deleted", "logical_deleted",
                                erased->logical_deleted, "cleanup_deferred",
                                erased->cleanup_deferred, "durability_deferred",
                                erased->durability_deferred});
                 }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/pause",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflow-runs/{run_id}/pause",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            auto run_id = request.require_path_id<WorkflowRunId>("run_id");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto result = co_await runtime->pause(*run_id);
            if (!result) {
              co_return result_error_response(result.error());
            }
            co_return typed_json_response(glz::obj{"status", "pausing"},
                                          http::HttpStatus::Accepted);
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/resume",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflow-runs/{run_id}/resume",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            auto run_id = request.require_path_id<WorkflowRunId>("run_id");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto result = co_await runtime->resume(*run_id);
            if (!result) {
              co_return result_error_response(result.error());
            }
            co_return typed_json_response(glz::obj{"status", "running"},
                                          http::HttpStatus::Accepted);
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/cancel",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflow-runs/{run_id}/cancel",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            WorkflowHttpRequest request{ctx.app, req};
            auto *runtime = request.require_runtime();
            auto run_id = request.require_path_id<WorkflowRunId>("run_id");
            if (request.failed()) {
              co_return request.take_failure();
            }
            auto result = co_await runtime->cancel(*run_id);
            if (!result) {
              co_return result_error_response(result.error());
            }
            co_return typed_json_response(glz::obj{"status", "stopping"},
                                          http::HttpStatus::Accepted);
          }));
}

} // namespace dagforge::api_detail
