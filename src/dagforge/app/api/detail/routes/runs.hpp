#pragma once

#include "../api_context.hpp"
#include "../dto_mapper.hpp"
#include "dagforge/util/string_hash.hpp"

#include <unordered_map>

namespace dagforge::api_detail {

inline auto register_run_routes(ApiContext &ctx) -> void {
  auto &router = ctx.router();

  router.get(
      "/api/history",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/history",
          [&ctx](http::HttpRequest) -> task<http::HttpResponse> {
            auto res = co_await ctx.list_run_views_async(50);
            if (!res) {
              co_return to_result_response(res.error()).value();
            }

            api_dto::HistoryResponseDto dto;
            dto.runs.reserve(res->size());
            for (const auto &run : *res) {
              dto.runs.emplace_back(to_dto(run.entry, run.state));
            }
            co_return typed_json_response(dto);
          }));

  router.get(
      "/api/history/{dag_run_id}",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/history/{dag_run_id}",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_run_id = req.path_param("dag_run_id");
            if (!dag_run_id) {
              co_return error_response(400, "Missing dag_run_id");
            }

            auto res = co_await ctx.get_run_view_async(DAGRunId{*dag_run_id});
            if (!res) {
              co_return to_result_response(res.error()).value();
            }
            co_return typed_json_response(to_dto(res->entry, res->state));
          }));

  router.get(
      "/api/runs/{dag_run_id}/tasks",
      ctx.make_instrumented_route(
          http::HttpMethod::GET, "/api/runs/{dag_run_id}/tasks",
          [&ctx](http::HttpRequest req) -> task<http::HttpResponse> {
            auto dag_run_id = req.path_param("dag_run_id");
            if (!dag_run_id) {
              co_return error_response(400, "Missing dag_run_id");
            }

            auto run_id = DAGRunId{*dag_run_id};
            auto run_res = co_await ctx.get_run_history_async(run_id);
            if (!run_res) {
              co_return to_result_response(run_res.error()).value();
            }

            auto tasks_res = co_await ctx.get_task_instances_async(run_id);
            co_return tasks_res
                .transform([&](const auto &tasks) {
                  api_dto::RunTasksResponseDto dto{
                      .dag_run_id = *dag_run_id,
                      .tasks = {},
                  };
                  std::unordered_map<std::string, TaskInstanceInfo, StringHash,
                                     StringEqual>
                      latest_by_task_id;
                  latest_by_task_id.reserve(tasks.size());
                  for (const auto &t : tasks) {
                    auto it = latest_by_task_id.find(t.task_id.str());
                    if (it == latest_by_task_id.end() ||
                        t.attempt > it->second.attempt) {
                      latest_by_task_id[t.task_id.str()] = t;
                    }
                  }

                  dto.tasks.reserve(latest_by_task_id.size());
                  for (const auto &[task_id, t] : latest_by_task_id) {
                    const auto duration_ms =
                        (t.started_at !=
                             std::chrono::system_clock::time_point{} &&
                         t.finished_at !=
                             std::chrono::system_clock::time_point{} &&
                         t.finished_at >= t.started_at)
                            ? std::chrono::duration_cast<
                                  std::chrono::milliseconds>(t.finished_at -
                                                             t.started_at)
                                  .count()
                            : 0;
                    dto.tasks.emplace_back(api_dto::TaskInstanceDto{
                        .task_id = task_id,
                        .state = enum_to_string(t.state),
                        .attempt = t.attempt,
                        .exit_code = t.exit_code,
                        .duration_ms = duration_ms,
                        .started_at = util::format_iso8601(t.started_at),
                        .finished_at = util::format_iso8601(t.finished_at),
                        .error = t.error_message,
                    });
                  }
                  return typed_json_response(dto);
                })
                .or_else(to_result_response)
                .value();
          }));
}

} // namespace dagforge::api_detail
