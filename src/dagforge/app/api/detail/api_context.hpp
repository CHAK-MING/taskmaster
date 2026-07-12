#pragma once

#include "dagforge/app/application.hpp"
#include "dagforge/app/http/http_server.hpp"
#include "dagforge/app/http/router.hpp"
#include "dagforge/app/http/websocket.hpp"
#include "dagforge/app/metrics_registry.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <experimental/scope>
#include <span>
#include <string>
#include <utility>

namespace dagforge::api_detail {

inline constexpr std::array<std::uint64_t, 12> kHttpDurationBucketsNs{
    1'000'000,     5'000'000,     10'000'000,    25'000'000,
    50'000'000,    100'000'000,   250'000'000,   500'000'000,
    1'000'000'000, 2'500'000'000, 5'000'000'000, 10'000'000'000};

struct ApiContext {
  Application &app;
  http::HttpServer &server;
  http::WebSocketHub &ws_hub;
  std::atomic<std::uint64_t> &http_active_requests;
  detail::HttpMetricsRegistry &http_metrics;

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
      http_active_requests.fetch_add(1, std::memory_order_relaxed);
      const auto guard = std::experimental::scope_exit([this] {
        http_active_requests.fetch_sub(1, std::memory_order_relaxed);
      });
      const auto started = std::chrono::steady_clock::now();
      auto response = co_await handler(std::move(req));
      const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::steady_clock::now() - started)
                               .count();
      route_metrics.record(response.status, static_cast<std::uint64_t>(
                                                elapsed > 0 ? elapsed : 0));
      co_return response;
    };
  }
};

} // namespace dagforge::api_detail
