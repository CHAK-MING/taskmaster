#pragma once

#include "dagforge/http/http_types.hpp"
#include "dagforge/core/metrics.hpp"

#include <cstdint>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dagforge::api_detail {

class HttpMetricsRegistry {
  struct RouteCounterEntry;
  struct EndpointHistogramEntry;

public:
  class RouteHandle {
  public:
    RouteHandle() = default;

    auto record(http::HttpStatus status,
                std::uint64_t elapsed_ns) const noexcept -> void;

  private:
    friend class HttpMetricsRegistry;

    RouteHandle(RouteCounterEntry *counter, metrics::Histogram *histogram);

    RouteCounterEntry *counter_{nullptr};
    metrics::Histogram *histogram_{nullptr};
  };

  HttpMetricsRegistry();
  ~HttpMetricsRegistry();

  HttpMetricsRegistry(const HttpMetricsRegistry &) = delete;
  auto operator=(const HttpMetricsRegistry &) -> HttpMetricsRegistry & = delete;

  auto register_route(http::HttpMethod method, std::string endpoint,
                      std::span<const std::uint64_t> duration_buckets)
      -> RouteHandle;

  [[nodiscard]] auto request_counts() const
      -> std::vector<
          std::tuple<std::string, std::string, std::string, std::uint64_t>>;
  [[nodiscard]] auto request_duration_snapshots() const
      -> std::vector<std::pair<std::string, metrics::Histogram::Snapshot>>;

private:
  static auto http_method_name(http::HttpMethod method) noexcept
      -> std::string_view;
  auto ensure_endpoint_histogram(
      const std::string &endpoint,
      std::span<const std::uint64_t> duration_buckets) -> metrics::Histogram *;

  std::vector<std::unique_ptr<RouteCounterEntry>> route_entries_;
  std::vector<std::unique_ptr<EndpointHistogramEntry>> endpoint_entries_;
  std::unordered_map<std::string, std::size_t> endpoint_index_;
};

} // namespace dagforge::api_detail
