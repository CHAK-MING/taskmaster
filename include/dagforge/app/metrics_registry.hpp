#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/client/http/http_types.hpp"
#include "dagforge/core/metrics.hpp"
#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#endif

namespace dagforge::detail {

class HttpMetricsRegistry {
public:
  struct RouteCounterEntry;

  class RouteHandle {
  public:
    RouteHandle() = default;

    auto record(http::HttpStatus status, std::uint64_t elapsed_ns) const noexcept
        -> void {
      if (counter_ != nullptr) {
        counter_->record(status);
      }
      if (histogram_ != nullptr) {
        histogram_->observe_ns(elapsed_ns);
      }
    }

  private:
    friend class HttpMetricsRegistry;

    RouteHandle(RouteCounterEntry *counter, metrics::Histogram *histogram)
        : counter_(counter), histogram_(histogram) {}

    RouteCounterEntry *counter_{nullptr};
    metrics::Histogram *histogram_{nullptr};
  };

  auto register_route(http::HttpMethod method, std::string endpoint,
                      std::span<const std::uint64_t> duration_buckets)
      -> RouteHandle {
    auto *histogram = ensure_endpoint_histogram(endpoint, duration_buckets);
    auto counter = std::make_unique<RouteCounterEntry>(
        std::string{http_method_name(method)}, std::move(endpoint));
    auto *counter_ptr = counter.get();
    route_entries_.push_back(std::move(counter));
    return RouteHandle{counter_ptr, histogram};
  }

  [[nodiscard]] auto request_counts() const
      -> std::vector<
          std::tuple<std::string, std::string, std::string, std::uint64_t>> {
    std::vector<
        std::tuple<std::string, std::string, std::string, std::uint64_t>>
        out;
    for (const auto &entry : route_entries_) {
      if (!entry) {
        continue;
      }
      for (std::size_t code = 0; code < entry->status_counts.size(); ++code) {
        const auto count =
            entry->status_counts[code].load(std::memory_order_relaxed);
        if (count == 0) {
          continue;
        }
        out.emplace_back(entry->method, entry->endpoint, std::to_string(code),
                         count);
      }
    }
    return out;
  }

  [[nodiscard]] auto request_duration_snapshots() const
      -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> {
    std::vector<std::pair<std::string, metrics::Histogram::Snapshot>> out;
    out.reserve(endpoint_entries_.size());
    for (const auto &entry : endpoint_entries_) {
      if (!entry) {
        continue;
      }
      out.emplace_back(entry->endpoint, entry->histogram.snapshot());
    }
    return out;
  }

private:
  static constexpr std::size_t kTrackedStatusCodes = 600;

public:
  struct RouteCounterEntry {
    std::string method;
    std::string endpoint;
    std::array<std::atomic<std::uint64_t>, kTrackedStatusCodes> status_counts{};

    RouteCounterEntry(std::string method_in, std::string endpoint_in)
        : method(std::move(method_in)), endpoint(std::move(endpoint_in)) {}

    auto record(http::HttpStatus status) noexcept -> void {
      const auto code = static_cast<std::size_t>(
          static_cast<std::uint16_t>(status));
      if (code >= status_counts.size()) {
        return;
      }
      status_counts[code].fetch_add(1, std::memory_order_relaxed);
    }
  };

private:

  struct EndpointHistogramEntry {
    std::string endpoint;
    metrics::Histogram histogram;

    EndpointHistogramEntry(std::string endpoint_in,
                           std::span<const std::uint64_t> duration_buckets)
        : endpoint(std::move(endpoint_in)), histogram(duration_buckets) {}
  };

  static auto http_method_name(http::HttpMethod method) noexcept
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

  auto ensure_endpoint_histogram(
      const std::string &endpoint, std::span<const std::uint64_t> duration_buckets)
      -> metrics::Histogram * {
    if (auto it = endpoint_index_.find(endpoint); it != endpoint_index_.end()) {
      return &endpoint_entries_[it->second]->histogram;
    }

    auto entry = std::make_unique<EndpointHistogramEntry>(endpoint, duration_buckets);
    auto *histogram = &entry->histogram;
    const auto index = endpoint_entries_.size();
    endpoint_entries_.push_back(std::move(entry));
    endpoint_index_.emplace(endpoint_entries_[index]->endpoint, index);
    return histogram;
  }

  std::vector<std::unique_ptr<RouteCounterEntry>> route_entries_;
  std::vector<std::unique_ptr<EndpointHistogramEntry>> endpoint_entries_;
  std::unordered_map<std::string, std::size_t> endpoint_index_;
};

} // namespace dagforge::detail
