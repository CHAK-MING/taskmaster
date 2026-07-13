#include "dagforge/app/metrics_exporter.hpp"

#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"
#include "dagforge/workflow/workflow_runtime.hpp"

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace dagforge {
namespace {

namespace prom = prometheus;

template <typename Value>
[[nodiscard]] constexpr auto metric_value(Value value) noexcept -> double {
  return static_cast<double>(value);
}

template <typename Family, typename Value>
auto set_gauge(Family &family, const prom::Labels &labels, Value value) -> void {
  family.Add(labels).Set(metric_value(value));
}

template <typename Family, typename Value>
auto set_counter(Family &family, const prom::Labels &labels, Value value) -> void {
  family.Add(labels).Increment(metric_value(value));
}

auto set_histogram(prom::Histogram &histogram,
                   const metrics::Histogram::Snapshot &snapshot) -> void {
  std::vector<double> buckets;
  buckets.reserve(snapshot.bucket_counts.size());
  for (const auto count : snapshot.bucket_counts) {
    buckets.push_back(metric_value(count));
  }
  histogram.ObserveMultiple(buckets,
                            metric_value(snapshot.sum_ns) / 1'000'000'000.0);
}

} // namespace

auto render_prometheus_metrics(const Application &app) -> std::string {
  prom::Registry registry;

  const auto &runtime = app.runtime();
  auto &runtime_running = prom::BuildGauge()
                              .Name("dagforge_runtime_running")
                              .Help("Whether the DAGForge runtime is running")
                              .Register(registry);
  set_gauge(runtime_running, {}, app.is_running() ? 1 : 0);

  auto &runtime_shards = prom::BuildGauge()
                             .Name("dagforge_runtime_shards")
                             .Help("Configured runtime shard count")
                             .Register(registry);
  set_gauge(runtime_shards, {}, runtime.shard_count());

  auto &workflow_active = prom::BuildGauge()
                              .Name("dagforge_workflow_active_runs")
                              .Help("Active AI workflow runs")
                              .Register(registry);
  const auto *workflow = app.workflow_runtime();
  set_gauge(workflow_active, {},
            workflow != nullptr ? workflow->active_run_count() : 0);

  if (const auto *api = app.api_server()) {
    auto &active_requests = prom::BuildGauge()
                                .Name("dagforge_http_active_requests")
                                .Help("Current active HTTP requests")
                                .Register(registry);
    set_gauge(active_requests, {}, api->http_active_requests());

    auto &requests = prom::BuildCounter()
                         .Name("dagforge_http_requests_total")
                         .Help("HTTP requests by route and status")
                         .Register(registry);
    for (const auto &[method, endpoint, status, count] :
         api->http_request_counts()) {
      set_counter(requests,
                  {{"method", method},
                   {"endpoint", endpoint},
                   {"status", status}},
                  count);
    }

    auto &duration_family =
        prom::BuildHistogram()
            .Name("dagforge_http_request_duration_seconds")
            .Help("HTTP request duration by route")
            .Register(registry);
    for (const auto &[endpoint, snapshot] :
         api->http_request_duration_snapshots()) {
      auto &duration = duration_family.Add(
          prom::Labels{{"endpoint", endpoint}},
          prom::Histogram::BucketBoundaries{0.001, 0.005, 0.01, 0.025,
                                            0.05, 0.1, 0.25, 0.5, 1.0,
                                            2.5, 5.0, 10.0});
      set_histogram(duration, snapshot);
    }
  }

  prom::TextSerializer serializer;
  return serializer.Serialize(registry.Collect());
}

} // namespace dagforge
