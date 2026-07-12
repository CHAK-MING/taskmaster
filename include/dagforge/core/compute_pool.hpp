#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#include "dagforge/core/metrics.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <stop_token>
#endif

namespace dagforge {

enum class ComputePriority : std::uint8_t {
  Low,
  Normal,
  High,
  Critical,
};

enum class ComputeShutdownMode : std::uint8_t {
  Drain,
  CancelPending,
};

struct ComputeOptions {
  ComputePriority priority{ComputePriority::Normal};
  std::optional<std::chrono::steady_clock::time_point> start_deadline{};
};

struct ComputePoolConfig {
  std::size_t thread_count{0};
  std::size_t queue_capacity{1024};
  bool pin_threads_to_cores{false};
  unsigned cpu_affinity_offset{0};
};

struct ComputePoolSnapshot {
  std::size_t thread_count{0};
  std::size_t queue_capacity{0};
  std::size_t queued_tasks{0};
  std::size_t active_tasks{0};
  std::uint64_t submitted_total{0};
  std::uint64_t completed_total{0};
  std::uint64_t rejected_total{0};
  std::uint64_t cancelled_total{0};
  std::uint64_t timed_out_total{0};
  std::uint64_t failed_total{0};
  metrics::Histogram::Snapshot queue_wait_time{};
  metrics::Histogram::Snapshot execution_time{};
};

class ComputeTaskHandle {
public:
  ComputeTaskHandle() = default;

  [[nodiscard]] auto valid() const noexcept -> bool {
    return static_cast<bool>(stop_source_);
  }

  [[nodiscard]] auto stop_requested() const noexcept -> bool {
    return stop_source_ && stop_source_->stop_requested();
  }

  auto request_stop() const noexcept -> bool {
    return stop_source_ && stop_source_->request_stop();
  }

private:
  explicit ComputeTaskHandle(std::shared_ptr<std::stop_source> stop_source)
      : stop_source_(std::move(stop_source)) {}

  std::shared_ptr<std::stop_source> stop_source_{};

  friend class ComputePool;
};

class ComputePool {
public:
  using Work = std::move_only_function<void(std::stop_token)>;
  using OnDiscard = std::move_only_function<void(Error)>;

  explicit ComputePool(ComputePoolConfig config = {});
  ~ComputePool() noexcept;

  ComputePool(const ComputePool &) = delete;
  auto operator=(const ComputePool &) -> ComputePool & = delete;
  ComputePool(ComputePool &&) = delete;
  auto operator=(ComputePool &&) -> ComputePool & = delete;

  [[nodiscard]] auto start() -> Result<void>;
  auto stop(ComputeShutdownMode mode = ComputeShutdownMode::CancelPending)
      noexcept -> void;

  [[nodiscard]] auto is_running() const noexcept -> bool;

  [[nodiscard]] auto submit(ComputeOptions options, Work work,
                            OnDiscard on_discard = {})
      -> Result<ComputeTaskHandle>;

  [[nodiscard]] auto snapshot() const -> ComputePoolSnapshot;

private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace dagforge
