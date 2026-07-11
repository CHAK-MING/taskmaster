#include "dagforge/app/metrics_exporter.hpp"
#include "dagforge/app/api/api_server.hpp"
#include "dagforge/app/application.hpp"
#include "dagforge/app/services/execution_service.hpp"
#include "dagforge/app/services/persistence_service.hpp"
#include "dagforge/app/services/scheduler_service.hpp"
#include "dagforge/executor/executor.hpp"
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>

#include <array>
#include <concepts>
#include <cstdint>
#include <initializer_list>
#include <map>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dagforge {
namespace {

namespace prom = prometheus;

[[nodiscard]] auto
labels(std::initializer_list<std::pair<std::string, std::string>> values)
    -> prom::Labels {
  prom::Labels out;
  for (const auto &[key, value] : values) {
    out.emplace(key, value);
  }
  return out;
}

[[nodiscard]] auto executor_type_label(ExecutorType type) -> std::string_view {
  switch (type) {
  case ExecutorType::Shell:
    return "shell";
  case ExecutorType::Docker:
    return "docker";
  case ExecutorType::Sensor:
    return "sensor";
  case ExecutorType::Noop:
    return "noop";
  case ExecutorType::Lua:
    return "lua";
  }
  return "unknown";
}

[[nodiscard]] auto xcom_metric_source_label(
    ExecutionService::XComMetricSource source) -> std::string_view {
  switch (source) {
  case ExecutionService::XComMetricSource::Cache:
    return "cache";
  case ExecutionService::XComMetricSource::RunBatch:
    return "run_batch";
  case ExecutionService::XComMetricSource::TaskBatch:
    return "task_batch";
  case ExecutionService::XComMetricSource::Single:
    return "single";
  case ExecutionService::XComMetricSource::BranchCache:
    return "branch_cache";
  case ExecutionService::XComMetricSource::BranchSingle:
    return "branch_single";
  case ExecutionService::XComMetricSource::Count:
    break;
  }
  return "unknown";
}

[[nodiscard]] auto dag_run_terminal_state_label(std::size_t index)
    -> std::string_view {
  static constexpr std::array<std::string_view, 4> labels{
      "success", "failed", "skipped", "cancelled"};
  return index < labels.size() ? labels[index] : "unknown";
}

[[nodiscard]] auto task_failure_error_type_label(std::size_t index)
    -> std::string_view {
  static constexpr std::array<std::string_view, 9> labels{
      "timeout",
      "dependency_blocked",
      "invalid_config",
      "shell_exit_126",
      "shell_exit_127",
      "immediate_fail",
      "exit_error",
      "executor_error",
      "unknown",
  };
  return index < labels.size() ? labels[index] : "unknown";
}

[[nodiscard]] constexpr auto to_metric_value(std::integral auto value) noexcept
    -> double {
  return static_cast<double>(value);
}

[[nodiscard]] constexpr auto to_metric_value(std::floating_point auto value) noexcept
    -> double {
  return static_cast<double>(value);
}

template <typename MetricFamily, typename Value>
auto set_counter(MetricFamily &family, const prom::Labels &metric_labels,
                 Value value) -> void {
  family.Add(metric_labels).Increment(to_metric_value(value));
}

template <typename MetricFamily, typename Value>
auto set_gauge(MetricFamily &family, const prom::Labels &metric_labels,
               Value value) -> void {
  family.Add(metric_labels).Set(to_metric_value(value));
}

auto set_histogram(prom::Histogram &histogram,
                   const metrics::Histogram::Snapshot &snapshot) -> void {
  std::vector<double> bucket_increments;
  bucket_increments.reserve(snapshot.bucket_counts.size());
  for (auto count : snapshot.bucket_counts) {
    bucket_increments.push_back(to_metric_value(count));
  }
  const double sum_seconds =
      to_metric_value(snapshot.sum_ns) / 1'000'000'000.0;
  histogram.ObserveMultiple(bucket_increments, sum_seconds);
}

auto set_histogram_bytes(prom::Histogram &histogram,
                         const metrics::Histogram::Snapshot &snapshot) -> void {
  std::vector<double> bucket_increments;
  bucket_increments.reserve(snapshot.bucket_counts.size());
  for (auto count : snapshot.bucket_counts) {
    bucket_increments.push_back(to_metric_value(count));
  }
  histogram.ObserveMultiple(bucket_increments,
                            to_metric_value(snapshot.sum_ns));
}

auto add_histogram_family(prom::Registry &registry, std::string_view name,
                          std::string_view help)
    -> prom::Family<prom::Histogram> & {
  return prom::BuildHistogram()
      .Name(std::string{name})
      .Help(std::string{help})
      .Register(registry);
}

[[nodiscard]] auto bucket_boundaries(std::initializer_list<double> buckets)
    -> prom::Histogram::BucketBoundaries {
  return prom::Histogram::BucketBoundaries{buckets.begin(), buckets.end()};
}

} // namespace

auto render_prometheus_metrics(const Application &app) -> std::string {
  prom::Registry registry;

  const auto shard_count = app.runtime().shard_count();

  auto &active_coroutines = prom::BuildGauge()
                                .Name("dagforge_active_coroutines_total")
                                .Help("Current number of alive coroutines")
                                .Register(registry);
  for (unsigned sid = 0; sid < shard_count; ++sid) {
    const auto value = (sid == 0) ? app.active_coroutines() : 0;
    set_gauge(active_coroutines, labels({{"shard", std::to_string(sid)}}),
              value);
  }

  auto &mysql_batch_write_ops = prom::BuildCounter()
                                    .Name("dagforge_mysql_batch_write_ops")
                                    .Help("Batch writes committed")
                                    .Register(registry);
  set_counter(mysql_batch_write_ops, {}, app.mysql_batch_write_ops());

  auto &event_bus_queue_length =
      prom::BuildGauge()
          .Name("dagforge_event_bus_queue_length")
          .Help("Tasks queued for cross-shard execution")
          .Register(registry);
  set_gauge(event_bus_queue_length, {}, app.event_bus_queue_length());

  auto &dropped_persistence_events =
      prom::BuildCounter()
          .Name("dagforge_dropped_persistence_events_total")
          .Help("Persistence events dropped due to failures")
          .Register(registry);
  set_counter(dropped_persistence_events, {}, app.dropped_persistence_events());

  auto &trigger_batch_queue_depth =
      prom::BuildGauge()
          .Name("dagforge_trigger_batch_queue_depth")
          .Help("Current trigger batch queue depth")
          .Register(registry);
  set_gauge(trigger_batch_queue_depth, {}, app.trigger_batch_queue_depth());

  auto &trigger_batch_last_size = prom::BuildGauge()
                                      .Name("dagforge_trigger_batch_last_size")
                                      .Help("Last committed trigger batch size")
                                      .Register(registry);
  set_gauge(trigger_batch_last_size, {}, app.trigger_batch_last_size());

  auto &trigger_batch_last_linger_us =
      prom::BuildGauge()
          .Name("dagforge_trigger_batch_last_linger_us")
          .Help("Last trigger batch linger duration in microseconds")
          .Register(registry);
  set_gauge(trigger_batch_last_linger_us, {},
            app.trigger_batch_last_linger_us());

  auto &trigger_batch_last_flush_ms =
      prom::BuildGauge()
          .Name("dagforge_trigger_batch_last_flush_ms")
          .Help("Last trigger batch flush duration in milliseconds")
          .Register(registry);
  set_gauge(trigger_batch_last_flush_ms, {}, app.trigger_batch_last_flush_ms());

  auto &trigger_batch_requests_total =
      prom::BuildCounter()
          .Name("dagforge_trigger_batch_requests_total")
          .Help("Total trigger requests accepted into the batch queue")
          .Register(registry);
  set_counter(trigger_batch_requests_total, {},
              app.trigger_batch_requests_total());

  auto &trigger_batch_commits_total =
      prom::BuildCounter()
          .Name("dagforge_trigger_batch_commits_total")
          .Help("Total trigger batches committed")
          .Register(registry);
  set_counter(trigger_batch_commits_total, {},
              app.trigger_batch_commits_total());

  auto &trigger_batch_fallback_total =
      prom::BuildCounter()
          .Name("dagforge_trigger_batch_fallback_total")
          .Help("Total trigger requests processed via fallback path")
          .Register(registry);
  set_counter(trigger_batch_fallback_total, {},
              app.trigger_batch_fallback_total());

  auto &trigger_batch_rejected_total =
      prom::BuildCounter()
          .Name("dagforge_trigger_batch_rejected_total")
          .Help("Total trigger requests rejected from batch queue")
          .Register(registry);
  set_counter(trigger_batch_rejected_total, {},
              app.trigger_batch_rejected_total());

  auto &trigger_batch_wakeup_lag_us =
      prom::BuildGauge()
          .Name("dagforge_trigger_batch_wakeup_lag_us")
          .Help("Last trigger batch wake-up lag in microseconds")
          .Register(registry);
  set_gauge(trigger_batch_wakeup_lag_us, {}, app.trigger_batch_wakeup_lag_us());

  auto &shard_stall_age_ms =
      prom::BuildGauge()
          .Name("dagforge_shard_stall_age_ms")
          .Help("Milliseconds since shard heartbeat was last observed")
          .Register(registry);
  for (unsigned sid = 0; sid < shard_count; ++sid) {
    set_gauge(shard_stall_age_ms, labels({{"shard", std::to_string(sid)}}),
              app.shard_stall_age_ms(sid));
  }

  auto &shard_memory_used_bytes = prom::BuildGauge()
                                      .Name("dagforge_shard_memory_used_bytes")
                                      .Help("Current bytes used in shard arena")
                                      .Register(registry);
  auto &shard_memory_capacity_bytes =
      prom::BuildGauge()
          .Name("dagforge_shard_memory_capacity_bytes")
          .Help("Configured shard arena capacity in bytes")
          .Register(registry);
  auto &shard_memory_allocations_total =
      prom::BuildCounter()
          .Name("dagforge_shard_memory_allocations_total")
          .Help("Total shard arena allocation requests")
          .Register(registry);
  auto &shard_memory_oom_fallbacks_total =
      prom::BuildCounter()
          .Name("dagforge_shard_memory_oom_fallbacks_total")
          .Help("Total shard arena fallback allocations")
          .Register(registry);
  for (unsigned sid = 0; sid < shard_count; ++sid) {
    const auto shard_label = labels({{"shard", std::to_string(sid)}});
    const auto &shard = app.runtime().shard(static_cast<shard_id>(sid));
    set_gauge(shard_memory_used_bytes, shard_label,
              shard.memory_used_bytes());
    set_gauge(shard_memory_capacity_bytes, shard_label,
              shard.memory_capacity_bytes());
    set_counter(shard_memory_allocations_total, shard_label,
                shard.memory_allocations_total());
    set_counter(shard_memory_oom_fallbacks_total, shard_label,
                shard.memory_oom_fallbacks_total());
  }

  auto &cross_shard_messages_total =
      prom::BuildCounter()
          .Name("dagforge_cross_shard_messages_total")
          .Help("Total cross-shard messages sent")
          .Register(registry);
  auto &cross_shard_queue_overflow_total =
      prom::BuildCounter()
          .Name("dagforge_cross_shard_queue_overflow_total")
          .Help("Total cross-shard queue overflow events")
          .Register(registry);
  auto &cross_shard_latency_seconds =
      add_histogram_family(registry, "dagforge_cross_shard_latency_seconds",
                           "Cross-shard message delivery latency in seconds");
  const auto cross_shard_latency_buckets =
      bucket_boundaries({0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001,
                         0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01});
  for (unsigned source = 0; source < shard_count; ++source) {
    for (unsigned target = 0; target < shard_count; ++target) {
      if (source == target) {
        continue;
      }
      const auto metric_labels = labels({{"source", std::to_string(source)},
                                         {"target", std::to_string(target)}});
      set_counter(
          cross_shard_messages_total, metric_labels,
          app.runtime().cross_shard_messages_total(
              static_cast<shard_id>(source), static_cast<shard_id>(target)));
      set_counter(
          cross_shard_queue_overflow_total, metric_labels,
          app.runtime().cross_shard_queue_overflow_total(
              static_cast<shard_id>(source), static_cast<shard_id>(target)));
      auto &hist = cross_shard_latency_seconds.Add(metric_labels,
                                                   cross_shard_latency_buckets);
      set_histogram(hist, app.runtime().cross_shard_latency_snapshot(
                              static_cast<shard_id>(source),
                              static_cast<shard_id>(target)));
    }
  }

  auto &io_context_pending_handlers =
      prom::BuildGauge()
          .Name("dagforge_io_context_pending_handlers")
          .Help("Approximate queued handlers per shard")
          .Register(registry);
  auto &io_context_timer_depth =
      prom::BuildGauge()
          .Name("dagforge_io_context_timer_depth")
          .Help("Approximate active timers per shard")
          .Register(registry);
  auto &io_context_poll_duration = add_histogram_family(
      registry, "dagforge_io_context_poll_duration_seconds",
      "Shard io_context poll cycle duration in seconds");
  const auto io_context_poll_buckets =
      bucket_boundaries({0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
                         0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0});
  for (unsigned sid = 0; sid < shard_count; ++sid) {
    const auto shard_label = labels({{"shard", std::to_string(sid)}});
    const auto pending_handlers = app.runtime().pending_cross_shard_queue_length(
        static_cast<shard_id>(sid));
    set_gauge(io_context_pending_handlers, shard_label, pending_handlers);
    set_gauge(io_context_timer_depth, shard_label,
              app.runtime().io_context_timer_depth(static_cast<shard_id>(sid)));
    auto &hist =
        io_context_poll_duration.Add(shard_label, io_context_poll_buckets);
    set_histogram(hist, app.runtime().io_context_poll_duration_snapshot(
                            static_cast<shard_id>(sid)));
  }

  if (const auto *api = app.api_server()) {
    auto &http_active_connections =
        prom::BuildGauge()
            .Name("dagforge_http_active_connections")
            .Help("Current active HTTP requests")
            .Register(registry);
    set_gauge(http_active_connections, {}, api->http_active_requests());

    auto &http_requests_total =
        prom::BuildCounter()
            .Name("dagforge_http_requests_total")
            .Help("Total HTTP requests by route and status")
            .Register(registry);
    for (const auto &[method, endpoint, status, count] :
         api->http_request_counts()) {
      set_counter(
          http_requests_total,
          labels(
              {{"method", method}, {"endpoint", endpoint}, {"status", status}}),
          count);
    }

    auto &http_request_duration_seconds =
        add_histogram_family(registry, "dagforge_http_request_duration_seconds",
                             "HTTP request duration in seconds");
    const auto http_bucket_labels = bucket_boundaries(
        {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0});
    for (const auto &[endpoint, snapshot] :
         api->http_request_duration_snapshots()) {
      auto &hist = http_request_duration_seconds.Add(
          labels({{"endpoint", endpoint}}), http_bucket_labels);
      set_histogram(hist, snapshot);
    }

    auto &websocket_connections_active =
        prom::BuildGauge()
            .Name("dagforge_websocket_connections_active")
            .Help("Current active WebSocket connections")
            .Register(registry);
    auto &websocket_messages_sent_total =
        prom::BuildCounter()
            .Name("dagforge_websocket_messages_sent_total")
            .Help("Total WebSocket messages sent")
            .Register(registry);
    auto &websocket_messages_received_total =
        prom::BuildCounter()
            .Name("dagforge_websocket_messages_received_total")
            .Help("Total WebSocket messages received")
            .Register(registry);
    set_gauge(websocket_connections_active, {},
              api->websocket_hub().connection_count());
    set_counter(websocket_messages_sent_total, {},
                api->websocket_hub().messages_sent_total());
    set_counter(websocket_messages_received_total, {},
                api->websocket_hub().messages_received_total());

    auto &websocket_message_size_bytes =
        add_histogram_family(registry, "dagforge_websocket_message_size_bytes",
                             "WebSocket message size in bytes");
    const auto websocket_size_buckets = bucket_boundaries(
        {64.0, 128.0, 256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0,
         32768.0, 65536.0, 131072.0, 262144.0, 524288.0, 1048576.0, 4194304.0,
         8388608.0});
    for (const auto &[direction, snapshot] :
         api->websocket_hub().message_size_snapshots()) {
      auto &hist = websocket_message_size_bytes.Add(
          labels({{"direction", direction}}), websocket_size_buckets);
      set_histogram_bytes(hist, snapshot);
    }
  }

  if (const auto *execution = app.execution_service()) {
    auto &task_runs_total = prom::BuildCounter()
                                .Name("dagforge_task_runs_total")
                                .Help("Total tasks started")
                                .Register(registry);
    set_counter(task_runs_total, {}, execution->task_runs_total());

    auto &task_successes_total = prom::BuildCounter()
                                     .Name("dagforge_task_successes_total")
                                     .Help("Total successful tasks")
                                     .Register(registry);
    set_counter(task_successes_total, {}, execution->task_successes_total());

    auto &task_failures_total = prom::BuildCounter()
                                    .Name("dagforge_task_failures_total")
                                    .Help("Total failed tasks")
                                    .Register(registry);
    set_counter(task_failures_total, {}, execution->task_failures_total());

    auto &task_skipped_total = prom::BuildCounter()
                                   .Name("dagforge_task_skipped_total")
                                   .Help("Total skipped tasks")
                                   .Register(registry);
    set_counter(task_skipped_total, {}, execution->task_skipped_total());

    auto &task_duration_seconds =
        add_histogram_family(registry, "dagforge_task_duration_seconds",
                             "Task runtime duration in seconds");
    auto &task_duration = task_duration_seconds.Add(
        {}, bucket_boundaries({0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
                               1.0, 2.5, 5.0, 10.0}));
    const auto task_duration_snapshot = execution->task_duration_histogram();
    set_histogram(task_duration, task_duration_snapshot);

    auto &dag_runs_total = prom::BuildCounter()
                               .Name("dagforge_dag_runs_total")
                               .Help("Total completed DAG runs by dag and state")
                               .Register(registry);
    auto &dag_run_duration_seconds = add_histogram_family(
        registry, "dagforge_dag_run_duration_seconds",
        "DAG run duration in seconds by dag and terminal state");
    const auto dag_run_duration_bucket_boundaries = bucket_boundaries(
        {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
         10.0,  30.0,  60.0,  300.0, 900.0, 3600.0});
    for (const auto &snapshot : app.dag_run_metrics()) {
      for (std::size_t i = 0; i < snapshot.terminal_counts.size(); ++i) {
        const auto count = snapshot.terminal_counts[i];
        const auto &hist = snapshot.terminal_duration_histograms[i];
        if (count == 0 && hist.count == 0) {
          continue;
        }

        const auto metric_labels = labels(
            {{"dag_id", std::string(snapshot.dag_id.value())},
             {"state", std::string(dag_run_terminal_state_label(i))}});
        set_counter(dag_runs_total, metric_labels, count);
        auto &dag_run_duration =
            dag_run_duration_seconds.Add(metric_labels,
                                         dag_run_duration_bucket_boundaries);
        set_histogram(dag_run_duration, hist);
      }
    }

    auto &xcom_prefetch_requests_total = prom::BuildCounter()
                                             .Name("dagforge_xcom_prefetch_requests_total")
                                             .Help("Total XCom prefetch requests by source")
                                             .Register(registry);
    auto &xcom_prefetch_hits_total = prom::BuildCounter()
                                         .Name("dagforge_xcom_prefetch_hits_total")
                                         .Help("Total XCom prefetch hits by source")
                                         .Register(registry);
    for (auto source : {ExecutionService::XComMetricSource::Cache,
                        ExecutionService::XComMetricSource::RunBatch,
                        ExecutionService::XComMetricSource::TaskBatch,
                        ExecutionService::XComMetricSource::Single,
                        ExecutionService::XComMetricSource::BranchCache,
                        ExecutionService::XComMetricSource::BranchSingle}) {
      const auto label =
          labels({{"source", std::string(xcom_metric_source_label(source))}});
      set_counter(xcom_prefetch_requests_total, label,
                  execution->xcom_prefetch_requests_total(source));
      set_counter(xcom_prefetch_hits_total, label,
                  execution->xcom_prefetch_hits_total(source));
    }

    auto &xcom_push_entries_total =
        prom::BuildCounter()
            .Name("dagforge_xcom_push_entries_total")
            .Help("Total XCom entries extracted from task outputs")
            .Register(registry);
    set_counter(xcom_push_entries_total, {},
                execution->xcom_push_entries_total());

    auto &xcom_push_extraction_failures_total =
        prom::BuildCounter()
            .Name("dagforge_xcom_push_extraction_failures_total")
            .Help("Total XCom extraction failures")
            .Register(registry);
    set_counter(xcom_push_extraction_failures_total, {},
                execution->xcom_push_extraction_failures_total());

    auto &task_failure_error_type_totals =
        prom::BuildCounter()
            .Name("dagforge_task_failures_by_error_type_total")
            .Help("Task failures grouped by error type")
            .Register(registry);
    const auto failure_error_type_totals =
        execution->task_failure_error_type_totals();
    for (std::size_t i = 0; i < failure_error_type_totals.size(); ++i) {
      const auto count = failure_error_type_totals[i];
      if (count == 0) {
        continue;
      }
      set_counter(task_failure_error_type_totals,
                  labels({{"error_type",
                           std::string(task_failure_error_type_label(i))}}),
                  count);
    }

    const auto dispatch_bucket_boundaries = bucket_boundaries(
        {0.000001, 0.000005, 0.00001, 0.000025, 0.00005, 0.0001, 0.00025,
         0.0005,   0.001,    0.0025,  0.005,    0.01,    0.025,  0.05,
         0.1,      0.25,     0.5,     1.0,      2.5,     5.0,    10.0});

    auto &dispatch_duration_seconds = add_histogram_family(
        registry, "dagforge_executor_dispatch_duration_seconds",
        "Dispatch coroutine duration in seconds");
    auto &dispatch_duration =
        dispatch_duration_seconds.Add({}, dispatch_bucket_boundaries);
    set_histogram(dispatch_duration, execution->dispatch_duration_histogram());

    auto &dispatch_scan_duration_seconds = add_histogram_family(
        registry, "dagforge_executor_dispatch_scan_duration_seconds",
        "Dispatch scan duration in seconds");
    auto &dispatch_scan_duration =
        dispatch_scan_duration_seconds.Add({}, dispatch_bucket_boundaries);
    set_histogram(dispatch_scan_duration,
                  execution->dispatch_scan_duration_histogram());

    auto &dispatch_ready_snapshot_duration_seconds = add_histogram_family(
        registry, "dagforge_executor_dispatch_ready_snapshot_duration_seconds",
        "Ready task snapshot duration in seconds");
    auto &dispatch_ready_snapshot_duration =
        dispatch_ready_snapshot_duration_seconds.Add(
            {}, dispatch_bucket_boundaries);
    set_histogram(dispatch_ready_snapshot_duration,
                  execution->dispatch_ready_snapshot_duration_histogram());

    auto &dispatch_candidate_materialize_duration_seconds =
        add_histogram_family(
            registry,
            "dagforge_executor_dispatch_candidate_materialize_duration_seconds",
            "Ready task materialization duration in seconds");
    auto &dispatch_candidate_materialize_duration =
        dispatch_candidate_materialize_duration_seconds.Add(
            {}, dispatch_bucket_boundaries);
    set_histogram(
        dispatch_candidate_materialize_duration,
        execution->dispatch_candidate_materialize_duration_histogram());

    auto &dispatch_dependency_check_duration_seconds = add_histogram_family(
        registry,
        "dagforge_executor_dispatch_dependency_check_duration_seconds",
        "Dispatch dependency check duration in seconds");
    auto &dispatch_dependency_check_duration =
        dispatch_dependency_check_duration_seconds.Add(
            {}, dispatch_bucket_boundaries);
    set_histogram(dispatch_dependency_check_duration,
                  execution->dispatch_dependency_check_duration_histogram());

    auto &dispatch_launch_duration_seconds = add_histogram_family(
        registry, "dagforge_executor_dispatch_launch_duration_seconds",
        "Dispatch task launch duration in seconds");
    auto &dispatch_launch_duration =
        dispatch_launch_duration_seconds.Add({}, dispatch_bucket_boundaries);
    set_histogram(dispatch_launch_duration,
                  execution->dispatch_launch_duration_histogram());

    auto &dispatch_spawn_duration_seconds = add_histogram_family(
        registry, "dagforge_executor_dispatch_spawn_duration_seconds",
        "Dispatch task spawn duration in seconds");
    auto &dispatch_spawn_duration =
        dispatch_spawn_duration_seconds.Add({}, dispatch_bucket_boundaries);
    set_histogram(dispatch_spawn_duration,
                  execution->dispatch_spawn_duration_histogram());

    auto &queue_depth = prom::BuildGauge()
                            .Name("dagforge_executor_queue_size")
                            .Help("Queued run count awaiting dispatch")
                            .Register(registry);
    set_gauge(queue_depth, {}, execution->queue_depth_total());

    auto &dispatch_invocations_total =
        prom::BuildCounter()
            .Name("dagforge_executor_dispatch_invocations_total")
            .Help("Total dispatch coroutine invocations")
            .Register(registry);
    set_counter(dispatch_invocations_total, {},
                execution->dispatch_invocations());

    auto &dispatch_scan_invocations_total =
        prom::BuildCounter()
            .Name("dagforge_executor_dispatch_scan_invocations_total")
            .Help("Total dispatch scan invocations per shard")
            .Register(registry);
    set_counter(dispatch_scan_invocations_total, {},
                execution->dispatch_scan_invocations());

    auto &executor_active_count = prom::BuildGauge()
                                      .Name("dagforge_executor_active_count")
                                      .Help("Active tasks per executor type")
                                      .Register(registry);
    auto &executor_start_total =
        prom::BuildCounter()
            .Name("dagforge_executor_start_total")
            .Help("Total task starts per executor type")
            .Register(registry);
    for (ExecutorType type : {ExecutorType::Shell, ExecutorType::Docker,
                              ExecutorType::Sensor, ExecutorType::Noop,
                              ExecutorType::Lua}) {
      const auto label =
          labels({{"executor_type", std::string(executor_type_label(type))}});
      set_gauge(executor_active_count, label,
                execution->executor_active_count(type));
      set_counter(executor_start_total, label,
                  execution->executor_start_total(type));
    }
  }

  if (const auto *scheduler = app.scheduler_service()) {
    auto &scheduler_queue_depth = prom::BuildGauge()
                                      .Name("dagforge_scheduler_queue_depth")
                                      .Help("Scheduled cron task count")
                                      .Register(registry);
    set_gauge(scheduler_queue_depth, {}, scheduler->queue_depth());

    auto &missed_schedules_total =
        prom::BuildCounter()
            .Name("dagforge_scheduler_missed_schedules_total")
            .Help("Total catch-up schedules fired")
            .Register(registry);
    set_counter(missed_schedules_total, {},
                scheduler->missed_schedules_total());

    auto &cron_parse_errors_total =
        prom::BuildCounter()
            .Name("dagforge_cron_parse_errors_total")
            .Help("Total cron parse errors")
            .Register(registry);
    set_counter(cron_parse_errors_total, {},
                scheduler->cron_parse_errors_total());
  }

  if (const auto *persistence = app.persistence_service()) {
    auto &db_query_duration_seconds =
        add_histogram_family(registry, "dagforge_db_query_duration_seconds",
                             "Database query duration in seconds");
    auto &db_query = db_query_duration_seconds.Add(
        {}, bucket_boundaries(
                {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5}));
    set_histogram(db_query, persistence->db_query_duration_histogram());

    auto &db_connection_acquire_seconds = add_histogram_family(
        registry, "dagforge_db_connection_acquire_seconds",
        "MySQL connection acquisition duration in seconds");
    auto &db_connection_acquire = db_connection_acquire_seconds.Add(
        {},
        bucket_boundaries({0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
                           0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0}));
    set_histogram(db_connection_acquire,
                  persistence->db_connection_acquire_histogram());

    auto &db_errors_total = prom::BuildCounter()
                                .Name("dagforge_db_errors_total")
                                .Help("Total database errors")
                                .Register(registry);
    set_counter(db_errors_total, {}, persistence->db_errors_total());

    auto &db_connection_acquire_failures_total =
        prom::BuildCounter()
            .Name("dagforge_db_connection_acquire_failures_total")
            .Help("Total MySQL connection acquisition failures")
            .Register(registry);
    set_counter(db_connection_acquire_failures_total, {},
                persistence->db_connection_acquire_failures_total());

    auto &db_transactions_total = prom::BuildCounter()
                                      .Name("dagforge_db_transactions_total")
                                      .Help("Total database transactions")
                                      .Register(registry);
    set_counter(db_transactions_total, {},
                persistence->db_transactions_total());

    auto &task_update_batch_flush_seconds = add_histogram_family(
        registry, "dagforge_task_update_batch_flush_seconds",
        "Task update batch flush duration in seconds");
    auto &task_update_batch_flush = task_update_batch_flush_seconds.Add(
        {},
        bucket_boundaries({0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
                           0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0}));
    set_histogram(task_update_batch_flush,
                  persistence->task_update_batch_flush_histogram());

    auto &trigger_batch_writer_acquire_failures_total =
        prom::BuildCounter()
            .Name("dagforge_trigger_batch_writer_acquire_failures_total")
            .Help("Total trigger batch writer connection acquisition failures")
            .Register(registry);
    set_counter(trigger_batch_writer_acquire_failures_total, {},
                persistence->trigger_batch_writer_acquire_failures_total());

    auto &task_update_batch_writer_acquire_failures_total =
        prom::BuildCounter()
            .Name("dagforge_task_update_batch_writer_acquire_failures_total")
            .Help("Total task update batch writer connection acquisition "
                  "failures")
            .Register(registry);
    set_counter(task_update_batch_writer_acquire_failures_total, {},
                persistence->task_update_batch_writer_acquire_failures_total());

    auto &task_update_batch_queue_depth =
        prom::BuildGauge()
            .Name("dagforge_task_update_batch_queue_depth")
            .Help("Current task update batch queue depth")
            .Register(registry);
    set_gauge(task_update_batch_queue_depth, {},
              app.task_update_batch_queue_depth());

    auto &task_update_batch_last_size =
        prom::BuildGauge()
            .Name("dagforge_task_update_batch_last_size")
            .Help("Last committed task update batch size")
            .Register(registry);
    set_gauge(task_update_batch_last_size, {},
              app.task_update_batch_last_size());

    auto &task_update_batch_last_linger_us =
        prom::BuildGauge()
            .Name("dagforge_task_update_batch_last_linger_us")
            .Help("Last task update batch linger duration in microseconds")
            .Register(registry);
    set_gauge(task_update_batch_last_linger_us, {},
              app.task_update_batch_last_linger_us());

    auto &task_update_batch_last_flush_ms =
        prom::BuildGauge()
            .Name("dagforge_task_update_batch_last_flush_ms")
            .Help("Last task update batch flush duration in milliseconds")
            .Register(registry);
    set_gauge(task_update_batch_last_flush_ms, {},
              app.task_update_batch_last_flush_ms());

    auto &task_update_batch_requests_total =
        prom::BuildCounter()
            .Name("dagforge_task_update_batch_requests_total")
            .Help("Total task updates accepted into the batch queue")
            .Register(registry);
    set_counter(task_update_batch_requests_total, {},
                app.task_update_batch_requests_total());

    auto &task_update_batch_commits_total =
        prom::BuildCounter()
            .Name("dagforge_task_update_batch_commits_total")
            .Help("Total task update batches committed")
            .Register(registry);
    set_counter(task_update_batch_commits_total, {},
                app.task_update_batch_commits_total());

    auto &task_update_batch_fallback_total =
        prom::BuildCounter()
            .Name("dagforge_task_update_batch_fallback_total")
            .Help("Total task updates processed via fallback path")
            .Register(registry);
    set_counter(task_update_batch_fallback_total, {},
                app.task_update_batch_fallback_total());

    auto &task_update_batch_rejected_total =
        prom::BuildCounter()
            .Name("dagforge_task_update_batch_rejected_total")
            .Help("Total task updates rejected from batch queue")
            .Register(registry);
    set_counter(task_update_batch_rejected_total, {},
                app.task_update_batch_rejected_total());

    auto &task_update_batch_wakeup_lag_us =
        prom::BuildGauge()
            .Name("dagforge_task_update_batch_wakeup_lag_us")
            .Help("Last task update batch wake-up lag in microseconds")
            .Register(registry);
    set_gauge(task_update_batch_wakeup_lag_us, {},
              app.task_update_batch_wakeup_lag_us());
  }

  prom::TextSerializer serializer;
  return serializer.Serialize(registry.Collect());
}

} // namespace dagforge
