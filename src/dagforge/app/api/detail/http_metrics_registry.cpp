#include "http_metrics_registry.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <string>
#include <utility>

namespace dagforge::api_detail {
namespace {

inline constexpr std::size_t kTrackedStatusCodes = 600;

} // namespace

struct HttpMetricsRegistry::RouteCounterEntry {
  std::string method;
  std::string endpoint;
  std::array<std::atomic<std::uint64_t>, kTrackedStatusCodes> status_counts{};

  RouteCounterEntry(std::string method_in, std::string endpoint_in)
      : method(std::move(method_in)), endpoint(std::move(endpoint_in)) {}

  auto record(http::HttpStatus status) noexcept -> void {
    const auto code =
        static_cast<std::size_t>(static_cast<std::uint16_t>(status));
    if (code < status_counts.size()) {
      status_counts[code].fetch_add(1, std::memory_order_relaxed);
    }
  }
};

struct HttpMetricsRegistry::EndpointHistogramEntry {
  std::string endpoint;
  metrics::Histogram histogram;

  EndpointHistogramEntry(
      std::string endpoint_in,
      std::span<const std::uint64_t> duration_buckets)
      : endpoint(std::move(endpoint_in)), histogram(duration_buckets) {}
};

HttpMetricsRegistry::RouteHandle::RouteHandle(
    RouteCounterEntry *counter, metrics::Histogram *histogram)
    : counter_(counter), histogram_(histogram) {}

auto HttpMetricsRegistry::RouteHandle::record(
    http::HttpStatus status, std::uint64_t elapsed_ns) const noexcept -> void {
  if (counter_ != nullptr) {
    counter_->record(status);
  }
  if (histogram_ != nullptr) {
    histogram_->observe_ns(elapsed_ns);
  }
}

HttpMetricsRegistry::HttpMetricsRegistry() = default;
HttpMetricsRegistry::~HttpMetricsRegistry() = default;

auto HttpMetricsRegistry::register_route(
    http::HttpMethod method, std::string endpoint,
    std::span<const std::uint64_t> duration_buckets) -> RouteHandle {
  auto *histogram = ensure_endpoint_histogram(endpoint, duration_buckets);
  auto counter = std::make_unique<RouteCounterEntry>(
      std::string{http_method_name(method)}, std::move(endpoint));
  auto *counter_ptr = counter.get();
  route_entries_.push_back(std::move(counter));
  return RouteHandle{counter_ptr, histogram};
}

auto HttpMetricsRegistry::request_counts() const
    -> std::vector<
        std::tuple<std::string, std::string, std::string, std::uint64_t>> {
  std::vector<
      std::tuple<std::string, std::string, std::string, std::uint64_t>>
      out;
  for (const auto &entry : route_entries_) {
    for (std::size_t code = 0; code < entry->status_counts.size(); ++code) {
      const auto count =
          entry->status_counts[code].load(std::memory_order_relaxed);
      if (count != 0) {
        out.emplace_back(entry->method, entry->endpoint, std::to_string(code),
                         count);
      }
    }
  }
  return out;
}

auto HttpMetricsRegistry::request_duration_snapshots() const
    -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> {
  std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> out;
  out.reserve(endpoint_entries_.size());
  for (const auto &entry : endpoint_entries_) {
    out.emplace_back(entry->endpoint, entry->histogram.snapshot());
  }
  return out;
}

auto HttpMetricsRegistry::http_method_name(http::HttpMethod method) noexcept
    -> std::string_view {
  switch (method) {
  case http::HttpMethod::GET:
    return "GET";
  case http::HttpMethod::POST:
    return "POST";
  case http::HttpMethod::PUT:
    return "PUT";
  case http::HttpMethod::DELETE:
    return "DELETE";
  case http::HttpMethod::PATCH:
    return "PATCH";
  case http::HttpMethod::OPTIONS:
    return "OPTIONS";
  case http::HttpMethod::HEAD:
    return "HEAD";
  }
  return "UNKNOWN";
}

auto HttpMetricsRegistry::ensure_endpoint_histogram(
    const std::string &endpoint,
    std::span<const std::uint64_t> duration_buckets) -> metrics::Histogram * {
  if (const auto it = endpoint_index_.find(endpoint);
      it != endpoint_index_.end()) {
    return &endpoint_entries_[it->second]->histogram;
  }

  auto entry =
      std::make_unique<EndpointHistogramEntry>(endpoint, duration_buckets);
  auto *histogram = &entry->histogram;
  endpoint_index_.emplace(endpoint, endpoint_entries_.size());
  endpoint_entries_.push_back(std::move(entry));
  return histogram;
}

} // namespace dagforge::api_detail
