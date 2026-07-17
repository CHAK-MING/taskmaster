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

template <typename Snapshot>
auto set_histogram(prom::Histogram &histogram, const Snapshot &snapshot)
    -> void {
  std::vector<double> buckets;
  buckets.reserve(snapshot.bucket_counts.size());
  for (const auto count : snapshot.bucket_counts) {
    buckets.push_back(metric_value(count));
  }
  histogram.ObserveMultiple(buckets,
                            metric_value(snapshot.sum_ns) / 1'000'000'000.0);
}

template <typename Snapshot>
[[nodiscard]] auto histogram_boundaries(const Snapshot &snapshot)
    -> prom::Histogram::BucketBoundaries {
  prom::Histogram::BucketBoundaries boundaries;
  boundaries.reserve(snapshot.bounds_ns.size());
  for (const auto bound : snapshot.bounds_ns) {
    boundaries.push_back(metric_value(bound) / 1'000'000'000.0);
  }
  return boundaries;
}

[[nodiscard]] auto
series_labels(const workflow::WorkflowMetricSeriesSnapshot &series,
              bool include_executor = false) -> prom::Labels {
  prom::Labels labels;
  if (include_executor) {
    labels.emplace("executor_class", series.executor_class);
  }
  if (!series.result.empty()) {
    labels.emplace("result", series.result);
  }
  if (!series.error_type.empty()) {
    labels.emplace("error_type", series.error_type);
  }
  return labels;
}

[[nodiscard]] auto
persistence_labels(const workflow::WorkflowPersistenceMetricSnapshot &series)
    -> prom::Labels {
  prom::Labels labels{{"store", series.store},
                      {"operation", series.operation},
                      {"result", series.result}};
  if (!series.error_type.empty()) {
    labels.emplace("error_type", series.error_type);
  }
  return labels;
}

[[nodiscard]] auto empty_workflow_duration()
    -> workflow::WorkflowDurationMetricSnapshot {
  return workflow::WorkflowDurationMetricSnapshot{
      .bounds_ns =
          {
              5'000'000ULL,
              10'000'000ULL,
              25'000'000ULL,
              50'000'000ULL,
              100'000'000ULL,
              250'000'000ULL,
              500'000'000ULL,
              1'000'000'000ULL,
              2'500'000'000ULL,
              5'000'000'000ULL,
              10'000'000'000ULL,
              30'000'000'000ULL,
              60'000'000'000ULL,
              120'000'000'000ULL,
              300'000'000'000ULL,
          },
      .bucket_counts = std::vector<std::uint64_t>(16, 0),
  };
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

  auto &ready = prom::BuildGauge()
                    .Name("dagforge_ready")
                    .Help("Whether this DAGForge instance can accept traffic")
                    .Register(registry);
  set_gauge(ready, {}, app.readiness().ready ? 1 : 0);

  auto &workflow_active = prom::BuildGauge()
                              .Name("dagforge_workflow_runs_active")
                              .Help("Current active workflow Runs")
                              .Register(registry);
  auto &workflow_active_compat =
      prom::BuildGauge()
          .Name("dagforge_workflow_active_runs")
          .Help("Deprecated alias for dagforge_workflow_runs_active")
          .Register(registry);
  const auto *workflow = app.workflow_runtime();
  set_gauge(workflow_active, {},
            workflow != nullptr ? workflow->active_run_count() : 0);
  set_gauge(workflow_active_compat, {},
            workflow != nullptr ? workflow->active_run_count() : 0);

  auto &runs_paused = prom::BuildGauge()
                          .Name("dagforge_workflow_runs_paused")
                          .Help("Current paused workflow Runs")
                          .Register(registry);
  auto &runs_stopping = prom::BuildGauge()
                            .Name("dagforge_workflow_runs_stopping")
                            .Help("Current workflow Runs stopping")
                            .Register(registry);
  auto &tasks_ready = prom::BuildGauge()
                          .Name("dagforge_workflow_tasks_ready")
                          .Help("Current workflow Tasks ready for dispatch")
                          .Register(registry);
  auto &tasks_retry_waiting =
      prom::BuildGauge()
          .Name("dagforge_workflow_tasks_retry_waiting")
          .Help("Current workflow Tasks waiting for retry")
          .Register(registry);
  auto &tasks_active =
      prom::BuildGauge()
          .Name("dagforge_workflow_tasks_active")
          .Help("Current active workflow Tasks by executor class")
          .Register(registry);
  auto &attempts_active =
      prom::BuildGauge()
          .Name("dagforge_workflow_attempts_active")
          .Help("Current active workflow Attempts by executor class")
          .Register(registry);

  auto &runs_total = prom::BuildCounter()
                         .Name("dagforge_workflow_runs_total")
                         .Help("Completed workflow Runs by result")
                         .Register(registry);
  auto &run_duration = prom::BuildHistogram()
                           .Name("dagforge_workflow_run_duration_seconds")
                           .Help("Workflow Run duration in seconds by result")
                           .Register(registry);
  auto &tasks_total =
      prom::BuildCounter()
          .Name("dagforge_workflow_tasks_total")
          .Help("Completed workflow Tasks by executor class and result")
          .Register(registry);
  auto &task_duration =
      prom::BuildHistogram()
          .Name("dagforge_workflow_task_duration_seconds")
          .Help(
              "Workflow Task duration in seconds by executor class and result")
          .Register(registry);
  auto &task_queue_duration =
      prom::BuildHistogram()
          .Name("dagforge_workflow_task_queue_duration_seconds")
          .Help("Workflow Task ready-to-dispatch duration in seconds by "
                "executor class")
          .Register(registry);
  auto &attempts_total =
      prom::BuildCounter()
          .Name("dagforge_workflow_attempts_total")
          .Help("Completed workflow Attempts by executor class and result")
          .Register(registry);
  auto &attempt_duration =
      prom::BuildHistogram()
          .Name("dagforge_workflow_attempt_duration_seconds")
          .Help("Workflow Attempt duration in seconds by executor class and "
                "result")
          .Register(registry);
  auto &retries_total = prom::BuildCounter()
                            .Name("dagforge_workflow_retries_total")
                            .Help("Workflow retry Attempts by executor class")
                            .Register(registry);
  auto &repair_runs_total = prom::BuildCounter()
                                .Name("dagforge_workflow_repair_runs_total")
                                .Help("Completed Repair Runs by result")
                                .Register(registry);
  auto &repair_run_duration =
      prom::BuildHistogram()
          .Name("dagforge_workflow_repair_run_duration_seconds")
          .Help("Repair Run duration in seconds by result")
          .Register(registry);
  auto &repair_nodes_total = prom::BuildCounter()
                                 .Name("dagforge_workflow_repair_nodes_total")
                                 .Help("Repair Run Node decisions")
                                 .Register(registry);
  auto &persistence_total =
      prom::BuildCounter()
          .Name("dagforge_workflow_persistence_operations_total")
          .Help(
              "Workflow persistence operations by store, operation, and result")
          .Register(registry);
  auto &persistence_duration =
      prom::BuildHistogram()
          .Name("dagforge_workflow_persistence_operation_duration_seconds")
          .Help("Workflow persistence operation duration in seconds")
          .Register(registry);
  auto &durability_deferred =
      prom::BuildCounter()
          .Name("dagforge_workflow_durability_deferred_total")
          .Help("Workflow persistence operations with deferred durability")
          .Register(registry);

  const auto empty_duration = empty_workflow_duration();
  const prom::Labels successful_run{{"result", "succeeded"}};
  set_counter(runs_total, successful_run, 0);
  set_histogram(
      run_duration.Add(successful_run, histogram_boundaries(empty_duration)),
      empty_duration);
  const prom::Labels successful_command{{"executor_class", "command"},
                                        {"result", "succeeded"}};
  set_counter(tasks_total, successful_command, 0);
  set_histogram(task_duration.Add(successful_command,
                                  histogram_boundaries(empty_duration)),
                empty_duration);
  set_histogram(task_queue_duration.Add({{"executor_class", "command"}},
                                        histogram_boundaries(empty_duration)),
                empty_duration);
  set_counter(attempts_total, successful_command, 0);
  set_histogram(attempt_duration.Add(successful_command,
                                     histogram_boundaries(empty_duration)),
                empty_duration);
  set_counter(retries_total, {{"executor_class", "command"}}, 0);
  set_counter(repair_runs_total, successful_run, 0);
  set_histogram(repair_run_duration.Add(successful_run,
                                        histogram_boundaries(empty_duration)),
                empty_duration);
  const prom::Labels successful_checkpoint{
      {"store", "checkpoint"}, {"operation", "write"}, {"result", "succeeded"}};
  set_counter(persistence_total, successful_checkpoint, 0);
  set_histogram(persistence_duration.Add(successful_checkpoint,
                                         histogram_boundaries(empty_duration)),
                empty_duration);
  set_counter(durability_deferred,
              {{"store", "checkpoint"}, {"operation", "write"}}, 0);

  if (workflow != nullptr) {
    const auto snapshot = workflow->metrics_snapshot();
    set_gauge(runs_paused, {}, snapshot.runs_paused);
    set_gauge(runs_stopping, {}, snapshot.runs_stopping);
    set_gauge(tasks_ready, {}, snapshot.tasks_ready);
    set_gauge(tasks_retry_waiting, {}, snapshot.tasks_retry_waiting);
    for (const auto &[executor, value] : snapshot.tasks_active) {
      set_gauge(tasks_active, {{"executor_class", executor}}, value);
    }
    for (const auto &[executor, value] : snapshot.attempts_active) {
      set_gauge(attempts_active, {{"executor_class", executor}}, value);
    }
    for (const auto &[executor, value] : snapshot.retries) {
      set_counter(retries_total, {{"executor_class", executor}}, value);
    }
    for (const auto &series : snapshot.runs) {
      const auto labels = series_labels(series);
      set_counter(runs_total, labels, series.total);
      auto &histogram =
          run_duration.Add(labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
    }
    for (const auto &series : snapshot.tasks) {
      const auto labels = series_labels(series, true);
      set_counter(tasks_total, labels, series.total);
      auto &histogram =
          task_duration.Add(labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
    }
    for (const auto &series : snapshot.task_queue) {
      const auto labels = series_labels(series, true);
      auto &histogram = task_queue_duration.Add(
          labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
    }
    for (const auto &series : snapshot.attempts) {
      const auto labels = series_labels(series, true);
      set_counter(attempts_total, labels, series.total);
      auto &histogram =
          attempt_duration.Add(labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
    }
    for (const auto &series : snapshot.repair_runs) {
      const auto labels = series_labels(series);
      set_counter(repair_runs_total, labels, series.total);
      auto &histogram = repair_run_duration.Add(
          labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
    }
    set_counter(repair_nodes_total, {{"decision", "reused"}},
                snapshot.repair_nodes_reused);
    set_counter(repair_nodes_total, {{"decision", "invalidated"}},
                snapshot.repair_nodes_invalidated);
    for (const auto &series : snapshot.persistence) {
      const auto labels = persistence_labels(series);
      set_counter(persistence_total, labels, series.total);
      auto &histogram = persistence_duration.Add(
          labels, histogram_boundaries(series.duration));
      set_histogram(histogram, series.duration);
      if (series.result == "deferred") {
        set_counter(durability_deferred,
                    {{"store", series.store}, {"operation", series.operation}},
                    series.total);
      }
    }
  } else {
    set_gauge(runs_paused, {}, 0);
    set_gauge(runs_stopping, {}, 0);
    set_gauge(tasks_ready, {}, 0);
    set_gauge(tasks_retry_waiting, {}, 0);
  }

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
