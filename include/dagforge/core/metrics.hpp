#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/contract.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <span>
#include <utility>
#include <vector>
#endif

namespace dagforge::metrics {

class Counter {
public:
  constexpr Counter() = default;

  auto add(std::uint64_t value) noexcept -> void {
    value_.fetch_add(value, std::memory_order_relaxed);
  }

  auto inc() noexcept -> void { add(1); }

  [[nodiscard]] auto load() const noexcept -> std::uint64_t {
    return value_.load(std::memory_order_relaxed);
  }

private:
  std::atomic<std::uint64_t> value_{0};
};

class Histogram {
public:
  struct Snapshot {
    std::vector<std::uint64_t> bounds_ns;
    std::vector<std::uint64_t> bucket_counts;
    std::uint64_t count{0};
    std::uint64_t sum_ns{0};
  };

  Histogram() = default;

  explicit Histogram(std::initializer_list<std::uint64_t> bounds_ns)
      : Histogram(std::span<const std::uint64_t>{bounds_ns.begin(),
                                                 bounds_ns.size()}) {}

  explicit Histogram(std::span<const std::uint64_t> bounds_ns)
      : bounds_ns_(bounds_ns.begin(), bounds_ns.end()),
        bucket_counts_(bounds_ns_.size() + 1) {
    validate_bounds(bounds_ns_);
  }

  auto observe_ns(std::uint64_t value_ns) noexcept -> void {
    const auto bucket =
        std::lower_bound(bounds_ns_.begin(), bounds_ns_.end(), value_ns) -
        bounds_ns_.begin();
    bucket_counts_[static_cast<std::size_t>(bucket)].fetch_add(
        1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_ns_.fetch_add(value_ns, std::memory_order_relaxed);
  }

  template <typename Rep, typename Period>
  auto observe(std::chrono::duration<Rep, Period> value) noexcept -> void {
    using Nanoseconds = std::chrono::duration<long double, std::nano>;
    const auto value_ns = Nanoseconds{value}.count();
    if (value_ns <= 0.0L) {
      observe_ns(0);
      return;
    }
    constexpr auto kMaximum =
        static_cast<long double>(std::numeric_limits<std::uint64_t>::max());
    observe_ns(value_ns >= kMaximum ? std::numeric_limits<std::uint64_t>::max()
                                    : static_cast<std::uint64_t>(value_ns));
  }

  // Snapshot reads are race-safe but intentionally not transactional. Under
  // concurrent observation, buckets, count, and sum may reflect nearby points
  // in time rather than one globally synchronized instant.
  [[nodiscard]] auto snapshot() const -> Snapshot {
    Snapshot out;
    out.bounds_ns = bounds_ns_;
    out.bucket_counts.reserve(bucket_counts_.size());
    for (const auto &bucket : bucket_counts_) {
      out.bucket_counts.push_back(bucket.load(std::memory_order_relaxed));
    }
    out.count = count_.load(std::memory_order_relaxed);
    out.sum_ns = sum_ns_.load(std::memory_order_relaxed);
    return out;
  }

private:
  static auto validate_bounds(std::span<const std::uint64_t> bounds) -> void {
    for (std::size_t index = 1; index < bounds.size(); ++index) {
      if (bounds[index - 1] >= bounds[index]) {
        contract_violation(
            "histogram bucket bounds must be strictly increasing");
      }
    }
  }

  std::vector<std::uint64_t> bounds_ns_;
  std::vector<std::atomic<std::uint64_t>> bucket_counts_{};
  std::atomic<std::uint64_t> count_{0};
  std::atomic<std::uint64_t> sum_ns_{0};
};

} // namespace dagforge::metrics
