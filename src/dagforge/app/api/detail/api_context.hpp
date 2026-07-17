#pragma once

#include "dagforge/app/application.hpp"
#include "dagforge/core/scope_exit.hpp"
#include "dagforge/http/http_server.hpp"
#include "dagforge/http/router.hpp"
#include "http_metrics_registry.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <format>
#include <span>
#include <string>
#include <utility>

#include <openssl/crypto.h>

namespace dagforge::api_detail {

inline constexpr std::array<std::uint64_t, 12> kHttpDurationBucketsNs{
    1'000'000,     5'000'000,     10'000'000,    25'000'000,
    50'000'000,    100'000'000,   250'000'000,   500'000'000,
    1'000'000'000, 2'500'000'000, 5'000'000'000, 10'000'000'000};

struct ApiContext {
  Application &app;
  http::HttpServer &server;
  std::atomic<std::uint64_t> &http_active_requests;
  HttpMetricsRegistry &http_metrics;
  std::string bearer_token;
  std::uint64_t max_request_body_bytes{1024ULL * 1024ULL};
  std::size_t max_concurrent_requests{128};

  [[nodiscard]] auto router() -> http::Router & { return server.router(); }

  template <typename Handler>
  auto make_instrumented_route(http::HttpMethod method, std::string endpoint,
                               Handler handler) -> http::RouteHandler {
    auto route_metrics = http_metrics.register_route(
        method, endpoint,
        std::span<const std::uint64_t>{kHttpDurationBucketsNs.data(),
                                       kHttpDurationBucketsNs.size()});
    return [this, route_metrics, handler = std::move(handler)](
               http::HttpRequest req) mutable -> task<http::HttpResponse> {
      const auto started = std::chrono::steady_clock::now();
      const auto record = [&](const http::HttpResponse &response) {
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started)
                .count();
        route_metrics.record(response.status, static_cast<std::uint64_t>(
                                                  elapsed > 0 ? elapsed : 0));
      };
      const auto error_response = [](http::HttpStatus status,
                                     std::string_view message) {
        http::HttpResponse response{.status = status};
        response.headers.set("Content-Type", "application/json");
        const auto body = std::format("{{\"error\":\"{}\"}}", message);
        response.body.assign(body.begin(), body.end());
        return response;
      };

      if (!bearer_token.empty()) {
        const auto authorization = req.header("Authorization");
        constexpr std::string_view kPrefix = "Bearer ";
        const auto supplied = authorization && authorization->starts_with(kPrefix)
                                  ? std::string_view{*authorization}.substr(
                                        kPrefix.size())
                                  : std::string_view{};
        const bool authenticated = supplied.size() == bearer_token.size() &&
                                   CRYPTO_memcmp(supplied.data(),
                                                 bearer_token.data(),
                                                 bearer_token.size()) == 0;
        if (!authenticated) {
          auto response =
              error_response(http::HttpStatus::Unauthorized, "unauthorized");
          response.headers.set("WWW-Authenticate", "Bearer");
          record(response);
          co_return response;
        }
      }

      if (req.body.size() > max_request_body_bytes) {
        auto response = error_response(http::HttpStatus::PayloadTooLarge,
                                       "request body too large");
        record(response);
        co_return response;
      }

      const auto previous =
          http_active_requests.fetch_add(1, std::memory_order_acq_rel);
      if (previous >= max_concurrent_requests) {
        http_active_requests.fetch_sub(1, std::memory_order_acq_rel);
        auto response = error_response(http::HttpStatus::TooManyRequests,
                                       "too many concurrent requests");
        record(response);
        co_return response;
      }
      const auto guard = dagforge::scope_exit([this] {
        http_active_requests.fetch_sub(1, std::memory_order_acq_rel);
      });
      auto response = co_await handler(std::move(req));
      record(response);
      co_return response;
    };
  }
};

} // namespace dagforge::api_detail
