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

  const auto compute = runtime.compute_pool_snapshot();
  auto &compute_threads = prom::BuildGauge()
                              .Name("dagforge_compute_pool_threads")
                              .Help("Configured compute worker threads")
                              .Register(registry);
  auto &compute_queue_capacity =
      prom::BuildGauge()
          .Name("dagforge_compute_queue_capacity")
          .Help("Maximum queued compute tasks")
          .Register(registry);
  auto &compute_queue_depth = prom::BuildGauge()
                                  .Name("dagforge_compute_queue_depth")
                                  .Help("Current queued compute tasks")
                                  .Register(registry);
  auto &compute_active = prom::BuildGauge()
                             .Name("dagforge_compute_active_tasks")
                             .Help("Current running compute tasks")
                             .Register(registry);
  set_gauge(compute_threads, {}, compute.thread_count);
  set_gauge(compute_queue_capacity, {}, compute.queue_capacity);
  set_gauge(compute_queue_depth, {}, compute.queued_tasks);
  set_gauge(compute_active, {}, compute.active_tasks);

  auto &compute_submitted = prom::BuildCounter()
                                .Name("dagforge_compute_submitted_total")
                                .Help("Accepted compute tasks")
                                .Register(registry);
  auto &compute_completed = prom::BuildCounter()
                                .Name("dagforge_compute_completed_total")
                                .Help("Completed compute tasks")
                                .Register(registry);
  auto &compute_rejected = prom::BuildCounter()
                               .Name("dagforge_compute_rejected_total")
                               .Help("Rejected compute tasks")
                               .Register(registry);
  auto &compute_cancelled = prom::BuildCounter()
                                .Name("dagforge_compute_cancelled_total")
                                .Help("Cancelled compute tasks")
                                .Register(registry);
  auto &compute_timed_out = prom::BuildCounter()
                                .Name("dagforge_compute_timed_out_total")
                                .Help("Compute tasks missing their start deadline")
                                .Register(registry);
  auto &compute_failed = prom::BuildCounter()
                             .Name("dagforge_compute_failed_total")
                             .Help("Compute work or callbacks that threw")
                             .Register(registry);
  set_counter(compute_submitted, {}, compute.submitted_total);
  set_counter(compute_completed, {}, compute.completed_total);
  set_counter(compute_rejected, {}, compute.rejected_total);
  set_counter(compute_cancelled, {}, compute.cancelled_total);
  set_counter(compute_timed_out, {}, compute.timed_out_total);
  set_counter(compute_failed, {}, compute.failed_total);

  auto &queue_wait_family = prom::BuildHistogram()
                                .Name("dagforge_compute_queue_wait_seconds")
                                .Help("Compute queue wait time")
                                .Register(registry);
  auto &execution_family = prom::BuildHistogram()
                               .Name("dagforge_compute_execution_seconds")
                               .Help("Compute execution time")
                               .Register(registry);
  auto &queue_wait = queue_wait_family.Add(
      {}, {0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001,
           0.00025, 0.0005, 0.001, 0.005, 0.025, 0.1});
  auto &execution = execution_family.Add(
      {}, {0.00001, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.005,
           0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 10.0});
  set_histogram(queue_wait, compute.queue_wait_time);
  set_histogram(execution, compute.execution_time);

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
          {{"endpoint", endpoint}},
          {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
           2.5, 5.0, 10.0});
      set_histogram(duration, snapshot);
    }
  }

  prom::TextSerializer serializer;
  return serializer.Serialize(registry.Collect());
}

} // namespace dagforge
