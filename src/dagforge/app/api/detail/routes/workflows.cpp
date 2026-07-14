#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "workflows.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_plan_loader.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <chrono>
#include <charconv>
#include <cstring>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge::api_detail {
namespace workflow_routes_detail {

struct PageRequest {
  std::size_t offset{0};
  std::size_t limit{100};
};

[[nodiscard]] inline auto query_number(std::string_view query,
                                       std::string_view key)
    -> std::optional<std::size_t> {
  while (!query.empty()) {
    const auto separator = query.find('&');
    const auto item = query.substr(0, separator);
    const auto equals = item.find('=');
    if (equals != std::string_view::npos && item.substr(0, equals) == key) {
      std::size_t value = 0;
      const auto token = item.substr(equals + 1);
      const auto [end, error] =
          std::from_chars(token.data(), token.data() + token.size(), value);
      if (error == std::errc{} && end == token.data() + token.size()) {
        return value;
      }
      return std::nullopt;
    }
    if (separator == std::string_view::npos) {
      break;
    }
    query.remove_prefix(separator + 1);
  }
  return std::nullopt;
}

[[nodiscard]] inline auto page_request(const http::HttpRequest &request)
    -> PageRequest {
  const auto query = std::string_view{request.query_string};
  return PageRequest{
      .offset = query_number(query, "offset").value_or(0),
      .limit = std::clamp<std::size_t>(
          query_number(query, "limit").value_or(100), 1, 1000),
  };
}

[[nodiscard]] inline auto member(const JsonValue &value, std::string_view key)
    -> const JsonValue * {
  if (!value.is_object()) {
    return nullptr;
  }
  const auto &object = value.get_object();
  const auto it = object.find(std::string{key});
  return it == object.end() ? nullptr : std::addressof(it->second);
}

[[nodiscard]] inline auto string_member(const JsonValue &value,
                                        std::string_view key,
                                        std::string fallback = {})
    -> std::string {
  const auto *item = member(value, key);
  return item && item->is_string() ? item->as<std::string>()
                                   : std::move(fallback);
}

[[nodiscard]] inline auto principal_from_json(const JsonValue &body)
    -> workflow::Principal {
  workflow::Principal principal;
  const auto *value = member(body, "principal");
  if (!value || !value->is_object()) {
    return principal;
  }
  principal.subject = string_member(*value, "subject");
  if (const auto *roles = member(*value, "roles"); roles && roles->is_array()) {
    for (const auto &role : roles->get_array()) {
      if (role.is_string()) {
        principal.roles.push_back(role.as<std::string>());
      }
    }
  }
  return principal;
}

inline auto add_timestamp(
    JsonValue &value, std::string_view key,
    std::chrono::system_clock::time_point timestamp) -> void {
  if (timestamp == std::chrono::system_clock::time_point{}) {
    return;
  }
  value[std::string{key}] =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          timestamp.time_since_epoch())
          .count();
}

[[nodiscard]] inline auto value_json(const workflow::WorkflowValue &value)
    -> JsonValue {
  if (std::holds_alternative<std::monostate>(value)) {
    return JsonValue{nullptr};
  }
  if (const auto *boolean = std::get_if<bool>(&value)) {
    JsonValue result;
    result = *boolean;
    return result;
  }
  if (const auto *integer = std::get_if<std::int64_t>(&value)) {
    JsonValue result;
    result = *integer;
    return result;
  }
  if (const auto *real = std::get_if<double>(&value)) {
    JsonValue result;
    result = *real;
    return result;
  }
  if (const auto *text = std::get_if<std::string>(&value)) {
    JsonValue result;
    result = *text;
    return result;
  }
  if (const auto *value_json = std::get_if<JsonValue>(&value)) {
    return *value_json;
  }
  const auto &artifact = std::get<workflow::ArtifactRef>(value);
  return json{{"type", "artifact"},
              {"artifact_id", artifact.artifact_id.str()},
              {"media_type", artifact.media_type},
              {"size_bytes", artifact.size_bytes},
              {"digest", artifact.digest}};
}

[[nodiscard]] inline auto snapshot_json(const workflow::RunSnapshot &snapshot)
    -> JsonValue {
  json tasks = json::array_t{};
  for (const auto &task : snapshot.tasks) {
    json attempts = json::array_t{};
    for (const auto &attempt : task.attempts) {
      json attempt_json{
          {"attempt_id", attempt.attempt_id.str()},
          {"number", attempt.number},
          {"state", std::string{workflow::to_string_view(attempt.state)}},
          {"error", attempt.error},
      };
      if (attempt.exit_code) {
        attempt_json["exit_code"] = *attempt.exit_code;
      }
      if (attempt.termination_reason) {
        attempt_json["termination_reason"] = std::string{
            workflow::to_string_view(*attempt.termination_reason)};
      }
      if (attempt.failure_class) {
        attempt_json["failure_class"] =
            std::string{workflow::to_string_view(*attempt.failure_class)};
      }
      add_timestamp(attempt_json, "created_at_ms", attempt.created_at);
      add_timestamp(attempt_json, "started_at_ms", attempt.started_at);
      add_timestamp(attempt_json, "finished_at_ms", attempt.finished_at);
      attempts.get_array().push_back(std::move(attempt_json));
    }
    json task_json{
        {"node_id", task.node_id.str()},
        {"state", std::string{workflow::to_string_view(task.state)}},
        {"attempt_count", task.attempt_count},
        {"last_error", task.last_error},
        {"attempts", std::move(attempts)},
    };
    if (task.active_attempt_id) {
      task_json["active_attempt_id"] = task.active_attempt_id->str();
    }
    if (task.skip_reason) {
      task_json["skip_reason"] =
          std::string{workflow::to_string_view(*task.skip_reason)};
    }
    if (task.next_attempt_at) {
      add_timestamp(task_json, "next_attempt_at_ms", *task.next_attempt_at);
    }
    add_timestamp(task_json, "started_at_ms", task.started_at);
    add_timestamp(task_json, "finished_at_ms", task.finished_at);
    tasks.get_array().push_back(std::move(task_json));
  }
  json result{{"run_id", snapshot.run_id.str()},
              {"workflow_id", snapshot.workflow_id.str()},
              {"plan_id", snapshot.plan_id.str()},
              {"state", std::string{workflow::to_string_view(snapshot.state)}},
              {"error", snapshot.error},
              {"stop_reason", snapshot.stop_reason},
              {"tasks", std::move(tasks)}};
  if (snapshot.stop_intent) {
    result["stop_intent"] =
        std::string{workflow::to_string_view(*snapshot.stop_intent)};
  }
  add_timestamp(result, "created_at_ms", snapshot.created_at);
  add_timestamp(result, "started_at_ms", snapshot.started_at);
  add_timestamp(result, "finished_at_ms", snapshot.finished_at);
  return result;
}

[[nodiscard]] inline auto evidence_json(
    const std::vector<workflow::EvidenceRecord> &records) -> JsonValue {
  json out = json::array_t{};
  for (const auto &record : records) {
    json item{{"evidence_id", record.evidence_id.str()},
              {"run_id", record.run_id.str()},
              {"node_id", record.node_id.str()},
              {"type", static_cast<std::uint64_t>(record.type)},
              {"actor", record.actor.subject},
              {"metadata", record.metadata},
              {"content_digest", record.content_digest}};
    if (record.artifact) {
      item["artifact"] = value_json(*record.artifact);
    }
    out.get_array().push_back(std::move(item));
  }
  return out;
}

[[nodiscard]] inline auto unavailable() -> http::HttpResponse {
  return error_response(503, "AI workflow runtime is disabled");
}

} // namespace workflow_routes_detail

auto register_workflow_routes(ApiContext &ctx) -> void {
  using namespace workflow_routes_detail;
  auto &router = ctx.router();

  router.post(
      "/api/v1/workflows/plans",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflows/plans",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *control = ctx.app.workflow_control_plane();
            if (!control) {
              co_return unavailable();
            }
            auto plan = workflow::WorkflowPlanLoader::from_json(
                req.body_as_string());
            if (!plan) {
              co_return error_response(400, plan.error().message());
            }
            auto compiled = control->register_plan(std::move(*plan));
            if (!compiled) {
              co_return to_result_response(compiled.error()).value();
            }
            co_return json_response(
                {{"workflow_id", (*compiled)->workflow_id.str()},
                 {"plan_id", (*compiled)->plan_id.str()},
                 {"digest", (*compiled)->digest},
                 {"nodes", (*compiled)->nodes.size()}},
                http::HttpStatus::Created);
          }));

  router.get(
      "/api/v1/workflows/plans",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/workflows/plans",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *control = ctx.app.workflow_control_plane();
            if (!control) {
              co_return unavailable();
            }
            auto page = page_request(req);
            auto registered = control->list_plans();
            std::ranges::sort(registered, {}, [](const auto &plan) {
              return plan->plan_id.str();
            });
            json plans = json::array_t{};
            const auto begin = std::min(page.offset, registered.size());
            const auto end = std::min(begin + page.limit, registered.size());
            for (std::size_t index = begin; index < end; ++index) {
              const auto &plan = registered[index];
              plans.get_array().push_back(
                  json{{"workflow_id", plan->workflow_id.str()},
                       {"plan_id", plan->plan_id.str()},
                       {"digest", plan->digest},
                       {"nodes", plan->nodes.size()}});
            }
            co_return json_response({{"plans", std::move(plans)},
                                     {"total", registered.size()},
                                     {"offset", page.offset},
                                     {"limit", page.limit}});
          }));

  router.post(
      "/api/v1/workflows/{workflow_id}/runs",
      ctx.make_instrumented_route(
          http::HttpMethod::POST,
          "/api/v1/workflows/{workflow_id}/runs",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *control = ctx.app.workflow_control_plane();
            auto *runtime = ctx.app.workflow_runtime();
            if (!control || !runtime) {
              co_return unavailable();
            }
            auto workflow_id = req.path_param("workflow_id");
            if (!workflow_id) {
              co_return error_response(400, "Missing workflow_id");
            }
            JsonValue body = JsonValue::object_t{};
            if (!req.body.empty()) {
              auto parsed = parse_json(req.body_as_string());
              if (!parsed || !parsed->is_object()) {
                co_return error_response(400, "Invalid JSON body");
              }
              body = std::move(*parsed);
            }
            const auto requested_plan = string_member(body, "plan_id");
            auto plan = requested_plan.empty()
                            ? control->get_latest(WorkflowId{*workflow_id})
                            : control->get_plan(
                                  WorkflowPlanId{requested_plan});
            if (!plan) {
              co_return to_result_response(plan.error()).value();
            }
            if ((*plan)->workflow_id != WorkflowId{*workflow_id}) {
              co_return error_response(400,
                                       "plan_id does not belong to workflow");
            }
            std::string idempotency_key =
                string_member(body, "idempotency_key");
            if (idempotency_key.empty()) {
              if (auto header = req.header("Idempotency-Key"); header) {
                idempotency_key = std::move(*header);
              }
            }
            workflow::WorkflowValue payload;
            if (const auto *value = member(body, "payload")) {
              payload = *value;
            }
            auto started = runtime->start(
                *plan,
                workflow::TriggerEnvelope{
                    .workflow_id = WorkflowId{*workflow_id},
                    .source = string_member(body, "source", "api"),
                    .event_type =
                        string_member(body, "event_type", "request"),
                    .payload = std::move(payload),
                    .idempotency_key = std::move(idempotency_key),
                    .principal = principal_from_json(body),
                    .occurred_at = std::chrono::system_clock::now(),
                });
            if (!started) {
              co_return to_result_response(started.error()).value();
            }
            co_return json_response({{"run_id", started->str()},
                                     {"workflow_id", *workflow_id},
                                     {"plan_id", (*plan)->plan_id.str()}},
                                    http::HttpStatus::Accepted);
          }));

  router.get(
      "/api/v1/workflow-runs/{run_id}",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/workflow-runs/{run_id}",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            if (!runtime || !run_id) {
              co_return runtime ? error_response(400, "Missing run_id")
                                : unavailable();
            }
            auto snapshot =
                co_await runtime->snapshot(WorkflowRunId{*run_id});
            if (!snapshot) {
              co_return to_result_response(snapshot.error()).value();
            }
            co_return json_response(snapshot_json(**snapshot));
          }));

  router.get(
      "/api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}",
      ctx.make_instrumented_route(
          http::HttpMethod::GET,
          "/api/v1/workflow-runs/{run_id}/outputs/{node_id}/{port}",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            auto node_id = req.path_param("node_id");
            auto port = req.path_param("port");
            if (!runtime || !run_id || !node_id || !port) {
              co_return runtime
                            ? error_response(400, "Missing output path parameter")
                            : unavailable();
            }
            auto value = co_await runtime->output(
                WorkflowRunId{*run_id},
                workflow::OutputRef{.node_id = WorkflowNodeId{*node_id},
                                    .port = WorkflowPortId{*port}});
            if (!value) {
              co_return to_result_response(value.error()).value();
            }
            co_return json_response({{"value", value_json(**value)}});
          }));

  router.get(
      "/api/v1/workflow-runs/{run_id}/evidence",
      ctx.make_instrumented_route(
          http::HttpMethod::GET,
          "/api/v1/workflow-runs/{run_id}/evidence",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            if (!runtime || !run_id) {
              co_return runtime ? error_response(400, "Missing run_id")
                                : unavailable();
            }
            auto records = runtime->evidence(WorkflowRunId{*run_id});
            const auto page = page_request(req);
            const auto begin = std::min(page.offset, records.size());
            const auto end = std::min(begin + page.limit, records.size());
            std::vector<workflow::EvidenceRecord> selected;
            selected.reserve(end - begin);
            for (std::size_t index = begin; index < end; ++index) {
              selected.push_back(records[index]);
            }
            co_return json_response({{"evidence", evidence_json(selected)},
                                     {"total", records.size()},
                                     {"offset", page.offset},
                                     {"limit", page.limit}});
          }));

  router.post(
      "/api/v1/artifacts",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/artifacts",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            if (!runtime) {
              co_return unavailable();
            }
            const auto media_type =
                req.header("Content-Type").value_or("application/octet-stream");
            const auto data = std::span<const std::byte>{
                reinterpret_cast<const std::byte *>(req.body.data()),
                req.body.size()};
            auto stored = runtime->artifact_store().put(data, media_type);
            if (!stored) {
              co_return to_result_response(stored.error()).value();
            }
            co_return json_response(
                {{"artifact_id", stored->artifact_id.str()},
                 {"media_type", stored->media_type},
                 {"size_bytes", stored->size_bytes},
                 {"digest", stored->digest}},
                http::HttpStatus::Created);
          }));

  router.get(
      "/api/v1/artifacts/{artifact_id}",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/artifacts/{artifact_id}",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto artifact_id = req.path_param("artifact_id");
            if (!runtime || !artifact_id) {
              co_return runtime ? error_response(400, "Missing artifact_id")
                                : unavailable();
            }
            auto artifact =
                runtime->artifact_store().get(ArtifactId{*artifact_id});
            if (!artifact) {
              co_return to_result_response(artifact.error()).value();
            }
            http::HttpResponse response{.status = http::HttpStatus::Ok};
            response.headers.set("Content-Type", artifact->ref.media_type);
            response.headers.set("ETag", artifact->ref.digest);
            response.body.resize(artifact->data.size());
            std::memcpy(response.body.data(), artifact->data.data(),
                        artifact->data.size());
            co_return response;
          }));

  router.del(
      "/api/v1/artifacts/{artifact_id}",
      ctx.make_instrumented_route(
          http::HttpMethod::DELETE, "/api/v1/artifacts/{artifact_id}",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto artifact_id = req.path_param("artifact_id");
            if (!runtime || !artifact_id) {
              co_return runtime ? error_response(400, "Missing artifact_id")
                                : unavailable();
            }
            auto erased =
                runtime->artifact_store().erase(ArtifactId{*artifact_id});
            if (!erased) {
              co_return to_result_response(erased.error()).value();
            }
            co_return json_response({{"status", "deleted"}});
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/pause",
      ctx.make_instrumented_route(
          http::HttpMethod::POST,
          "/api/v1/workflow-runs/{run_id}/pause",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            if (!runtime || !run_id) {
              co_return runtime ? error_response(400, "Missing run_id")
                                : unavailable();
            }
            auto result = co_await runtime->pause(WorkflowRunId{*run_id});
            if (!result) {
              co_return to_result_response(result.error()).value();
            }
            co_return json_response({{"status", "pausing"}},
                                    http::HttpStatus::Accepted);
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/resume",
      ctx.make_instrumented_route(
          http::HttpMethod::POST,
          "/api/v1/workflow-runs/{run_id}/resume",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            if (!runtime || !run_id) {
              co_return runtime ? error_response(400, "Missing run_id")
                                : unavailable();
            }
            auto result = co_await runtime->resume(WorkflowRunId{*run_id});
            if (!result) {
              co_return to_result_response(result.error()).value();
            }
            co_return json_response({{"status", "running"}},
                                    http::HttpStatus::Accepted);
          }));

  router.post(
      "/api/v1/workflow-runs/{run_id}/cancel",
      ctx.make_instrumented_route(
          http::HttpMethod::POST,
          "/api/v1/workflow-runs/{run_id}/cancel",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *runtime = ctx.app.workflow_runtime();
            auto run_id = req.path_param("run_id");
            if (!runtime || !run_id) {
              co_return runtime ? error_response(400, "Missing run_id")
                                : unavailable();
            }
            auto result =
                co_await runtime->cancel(WorkflowRunId{*run_id});
            if (!result) {
              co_return to_result_response(result.error()).value();
            }
            co_return json_response({{"status", "stopping"}},
                                    http::HttpStatus::Accepted);
          }));
}

} // namespace dagforge::api_detail
