#pragma once

#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "dagforge/workflow/workflow_control_plane.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge::api_detail {
namespace workflow_routes_detail {

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
  return std::visit(
      [](const auto &typed) -> JsonValue {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::same_as<T, std::monostate>) {
          return JsonValue{nullptr};
        } else if constexpr (std::same_as<T, bool> ||
                             std::same_as<T, std::int64_t> ||
                             std::same_as<T, double> ||
                             std::same_as<T, std::string>) {
          return JsonValue{typed};
        } else if constexpr (std::same_as<T, JsonValue>) {
          return typed;
        } else if constexpr (std::same_as<T, workflow::ArtifactRef>) {
          return json{{"type", "artifact"},
                      {"artifact_id", typed.artifact_id.str()},
                      {"media_type", typed.media_type},
                      {"size_bytes", typed.size_bytes},
                      {"digest", typed.digest}};
        } else if constexpr (std::same_as<T, workflow::EvaluationResult>) {
          return json{{"type", "evaluation"},
                      {"passed", typed.passed},
                      {"score", typed.score},
                      {"reason", typed.reason},
                      {"evidence", typed.evidence}};
        } else if constexpr (std::same_as<T, workflow::ToolResult>) {
          return json{{"type", "tool_result"},
                      {"name", typed.name},
                      {"success", typed.success},
                      {"output", typed.output},
                      {"error", typed.error}};
        } else if constexpr (std::same_as<T, workflow::ModelResponse>) {
          json tool_calls = json::array_t{};
          for (const auto &call : typed.tool_calls) {
            tool_calls.get_array().push_back(
                json{{"name", call.name}, {"arguments", call.arguments}});
          }
          json out{{"type", "model_response"},
                   {"text", typed.message.content},
                   {"provider_request_id", typed.provider_request_id},
                   {"input_tokens", typed.usage.input_tokens},
                   {"output_tokens", typed.usage.output_tokens},
                   {"tool_calls", std::move(tool_calls)}};
          if (typed.structured_output) {
            out["structured_output"] = *typed.structured_output;
          }
          return out;
        } else if constexpr (std::same_as<T, workflow::MessageList>) {
          json messages = json::array_t{};
          for (const auto &message : typed) {
            messages.get_array().push_back(
                json{{"role", message.role}, {"content", message.content}});
          }
          return messages;
        }
        return JsonValue{nullptr};
      },
      value);
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

inline auto register_workflow_routes(ApiContext &ctx) -> void {
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

  router.post(
      "/api/v1/workflows/plans/toml",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/v1/workflows/plans/toml",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto *control = ctx.app.workflow_control_plane();
            if (!control) {
              co_return unavailable();
            }
            auto plan = workflow::WorkflowPlanLoader::from_toml(
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
                 {"digest", (*compiled)->digest}},
                http::HttpStatus::Created);
          }));

  router.get(
      "/api/v1/workflows/plans",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/v1/workflows/plans",
          [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
            auto *control = ctx.app.workflow_control_plane();
            if (!control) {
              co_return unavailable();
            }
            json plans = json::array_t{};
            for (const auto &plan : control->list_plans()) {
              plans.get_array().push_back(
                  json{{"workflow_id", plan->workflow_id.str()},
                       {"plan_id", plan->plan_id.str()},
                       {"digest", plan->digest},
                       {"nodes", plan->nodes.size()}});
            }
            co_return json_response({{"plans", std::move(plans)}});
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
            auto plan = control->get_latest(WorkflowId{*workflow_id});
            if (!plan) {
              co_return to_result_response(plan.error()).value();
            }

            JsonValue body = JsonValue::object_t{};
            if (!req.body.empty()) {
              auto parsed = parse_json(req.body_as_string());
              if (!parsed || !parsed->is_object()) {
                co_return error_response(400, "Invalid JSON body");
              }
              body = std::move(*parsed);
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
            co_return json_response(
                {{"evidence",
                  evidence_json(runtime->evidence(WorkflowRunId{*run_id}))}});
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
