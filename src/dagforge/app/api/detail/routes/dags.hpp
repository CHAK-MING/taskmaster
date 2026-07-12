#pragma once

#include "../api_context.hpp"
#include "../dto_mapper.hpp"

namespace dagforge::api_detail {

inline auto register_dag_routes(ApiContext &ctx) -> void {
  auto &router = ctx.router();

  router.get("/api/dags",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/dags",
                 [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
                   api_dto::DagsResponseDto resp_dto;
                   for (const auto &dag_info :
                        ctx.app.dag_manager().list_dags()) {
                     resp_dto.dags.emplace_back(to_dto(dag_info));
                   }
                   co_return typed_json_response(resp_dto);
                 }));

  router.get("/api/dags/{dag_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/dags/{dag_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   auto dag_id = req.path_param("dag_id");
                   if (!dag_id) {
                     co_return error_response(400, "Missing dag_id");
                   }

                   co_return ctx.app.dag_manager()
                       .get_dag(DAGId{*dag_id})
                       .transform([](const auto &dag) {
                         return typed_json_response(to_dto(dag));
                       })
                       .or_else(to_result_response)
                       .value();
                 }));

  router.get("/api/dags/{dag_id}/tasks",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/dags/{dag_id}/tasks",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   auto dag_id = req.path_param("dag_id");
                   if (!dag_id) {
                     co_return error_response(400, "Missing dag_id");
                   }

                   co_return ctx.app.dag_manager()
                       .get_dag(DAGId{*dag_id})
                       .transform([](const auto &dag) {
                         api_dto::TaskIdsResponseDto dto;
                         dto.tasks.reserve(dag.tasks.size());
                         for (const auto &task : dag.tasks) {
                           dto.tasks.emplace_back(task.task_id.str());
                         }
                         return typed_json_response(dto);
                       })
                       .or_else(to_result_response)
                       .value();
                 }));

  router.get("/api/dags/{dag_id}/tasks/{task_id}",
             ctx.make_instrumented_route(
                 http::HttpMethod::GET, "/api/dags/{dag_id}/tasks/{task_id}",
                 [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                   auto dag_id = req.path_param("dag_id");
                   auto task_id = req.path_param("task_id");
                   if (!dag_id || !task_id) {
                     co_return error_response(400, "Missing dag_id or task_id");
                   }

                   co_return ctx.app.dag_manager()
                       .get_dag(DAGId{*dag_id})
                       .and_then(
                           [&](const auto &dag) -> Result<http::HttpResponse> {
                             auto *task = dag.find_task(TaskId{*task_id});
                             if (!task) {
                               return fail(Error::NotFound);
                             }
                             return ok(typed_json_response(to_dto(*task)));
                           })
                       .or_else(to_result_response)
                       .value();
                 }));

  router.post(
      "/api/dags/{dag_id}/trigger",
      ctx.make_instrumented_route(
          http::HttpMethod::POST, "/api/dags/{dag_id}/trigger",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_id = req.path_param("dag_id");
            if (!dag_id) {
              co_return error_response(400, "Missing dag_id");
            }

            std::optional<std::chrono::system_clock::time_point> execution_date;
            auto body = req.body_as_string();
            if (!body.empty()) {
              auto trigger_req =
                  parse_json_as_allow_unknown<api_dto::TriggerRequestDto>(body);
              if (!trigger_req) {
                co_return error_response(400, "Invalid JSON body");
              }
              if (trigger_req->execution_date) {
                auto parsed_execution_date =
                    parse_execution_date_arg(*trigger_req->execution_date);
                if (!parsed_execution_date) {
                  co_return error_response(
                      400, "Invalid execution_date, expected now "
                           "| YYYY-MM-DD | YYYY-MM-DDTHH:MM:SSZ");
                }
                execution_date = *parsed_execution_date;
              }
            }

            const auto request_started_at = std::chrono::steady_clock::now();
            auto res = co_await ctx.app.trigger_run(
                DAGId{*dag_id}, TriggerType::Manual, execution_date);
            const auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - request_started_at)
                    .count();
            if (res) {
              dagforge::log::debug("HTTP trigger completed dag_id={} "
                                   "dag_run_id={} latency_ms={}",
                                   *dag_id, *res, elapsed_ms);
            } else {
              dagforge::log::warn(
                  "HTTP trigger failed dag_id={} latency_ms={} error={}",
                  *dag_id, elapsed_ms, res.error().message());
            }
            co_return res
                .transform([](const auto &run_id) {
                  return json_response(
                      {{"dag_run_id", run_id.str()}, {"status", "triggered"}},
                      http::HttpStatus::Created);
                })
                .or_else(to_result_response)
                .value();
          }));

  router.post("/api/dags/{dag_id}/pause",
              ctx.make_instrumented_route(
                  http::HttpMethod::POST, "/api/dags/{dag_id}/pause",
                  [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                    auto dag_id = req.path_param("dag_id");
                    if (!dag_id) {
                      co_return error_response(400, "Missing dag_id");
                    }
                    auto res =
                        co_await ctx.app.set_dag_paused(DAGId{*dag_id}, true);
                    co_return res
                        .transform([&]() {
                          return json_response(
                              {{"dag_id", *dag_id}, {"is_paused", true}});
                        })
                        .or_else(to_result_response)
                        .value();
                  }));

  router.post("/api/dags/{dag_id}/unpause",
              ctx.make_instrumented_route(
                  http::HttpMethod::POST, "/api/dags/{dag_id}/unpause",
                  [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
                    auto dag_id = req.path_param("dag_id");
                    if (!dag_id) {
                      co_return error_response(400, "Missing dag_id");
                    }
                    auto res =
                        co_await ctx.app.set_dag_paused(DAGId{*dag_id}, false);
                    co_return res
                        .transform([&]() {
                          return json_response(
                              {{"dag_id", *dag_id}, {"is_paused", false}});
                        })
                        .or_else(to_result_response)
                        .value();
                  }));

  router.get(
      "/api/dags/{dag_id}/history",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/dags/{dag_id}/history",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_id = req.path_param("dag_id");
            if (!dag_id) {
              co_return error_response(400, "Missing dag_id");
            }

            auto res =
                co_await ctx.list_dag_run_views_async(DAGId{*dag_id}, 50);
            if (!res) {
              co_return to_result_response(res.error()).value();
            }

            api_dto::DagRunHistoryResponseDto dto;
            dto.runs.reserve(res->size());
            for (const auto &run : *res) {
              dto.runs.emplace_back(api_dto::DagRunSummaryDto{
                  .dag_run_id = run.entry.dag_run_id.str(),
                  .state = enum_to_string(run.state),
                  .started_at = util::format_iso8601(run.entry.started_at),
                  .finished_at = util::format_iso8601(run.entry.finished_at),
              });
            }
            co_return typed_json_response(dto);
          }));
}

} // namespace dagforge::api_detail
