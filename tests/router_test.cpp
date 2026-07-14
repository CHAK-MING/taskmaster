#include "dagforge/http/router.hpp"

#include "test_utils.hpp"
#include "gtest/gtest.h"

using namespace dagforge;

TEST(RouterTest, StaticRouteDispatchesHandler) {
  http::Router router;
  router.get("/health", [](http::HttpRequest req) -> task<http::HttpResponse> {
    co_return http::HttpResponse::json(R"({"status":"ok"})");
  });

  http::HttpRequest req;
  req.method = http::HttpMethod::GET;
  req.path = "/health";

  auto resp = dagforge::test::run_coro(router.route(std::move(req)));
  EXPECT_EQ(resp.status, http::HttpStatus::Ok);
  EXPECT_EQ(std::string(resp.body.begin(), resp.body.end()),
            R"({"status":"ok"})");
}

TEST(RouterTest, DynamicRouteExtractsPathParams) {
  http::Router router;
  router.get("/api/dags/{dag_id}/runs/{run_id}",
             [](http::HttpRequest req) -> task<http::HttpResponse> {
               auto dag_id = req.path_param("dag_id");
               auto run_id = req.path_param("run_id");
               EXPECT_TRUE(dag_id.has_value());
               EXPECT_TRUE(run_id.has_value());
               co_return http::HttpResponse::json(
                   std::format(R"({{"dag_id":"{}","run_id":"{}"}})",
                               dag_id.value_or(""), run_id.value_or("")));
             });

  http::HttpRequest req;
  req.method = http::HttpMethod::GET;
  req.path = "/api/dags/demo/runs/run_42";

  auto resp = dagforge::test::run_coro(router.route(std::move(req)));
  EXPECT_EQ(resp.status, http::HttpStatus::Ok);
  EXPECT_EQ(std::string(resp.body.begin(), resp.body.end()),
            R"({"dag_id":"demo","run_id":"run_42"})");
}

TEST(RouterTest, UnknownRouteReturnsNotFound) {
  http::Router router;
  router.get("/health", [](http::HttpRequest) -> task<http::HttpResponse> {
    co_return http::HttpResponse::ok();
  });

  http::HttpRequest req;
  req.method = http::HttpMethod::GET;
  req.path = "/missing";

  auto resp = dagforge::test::run_coro(router.route(std::move(req)));
  EXPECT_EQ(resp.status, http::HttpStatus::NotFound);
}

TEST(RouterTest, MethodMismatchReturnsMethodNotAllowed) {
  http::Router router;
  router.get("/health", [](http::HttpRequest) -> task<http::HttpResponse> {
    co_return http::HttpResponse::ok();
  });

  http::HttpRequest req;
  req.method = http::HttpMethod::POST;
  req.path = "/health";

  auto resp = dagforge::test::run_coro(router.route(std::move(req)));
  EXPECT_EQ(resp.status, http::HttpStatus::MethodNotAllowed);
}

TEST(RouterTest, DynamicMethodMismatchReturnsMethodNotAllowed) {
  http::Router router;
  router.get("/api/dags/{dag_id}/runs/{run_id}",
             [](http::HttpRequest req) -> task<http::HttpResponse> {
               co_return http::HttpResponse::json(
                   std::format(R"({{"dag_id":"{}","run_id":"{}"}})",
                               req.path_param("dag_id").value_or(""),
                               req.path_param("run_id").value_or("")));
             });

  http::HttpRequest req;
  req.method = http::HttpMethod::DELETE;
  req.path = "/api/dags/demo/runs/run_42";

  auto resp = dagforge::test::run_coro(router.route(std::move(req)));
  EXPECT_EQ(resp.status, http::HttpStatus::MethodNotAllowed);
}
