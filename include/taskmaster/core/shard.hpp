#pragma once

#include "taskmaster/core/io_ring.hpp"
#include "taskmaster/core/lockfree_queue.hpp"

#include <array>
#include <atomic>
#include <coroutine>
#include <deque>
#include <memory_resource>
#include <optional>
#include <unordered_set>

namespace taskmaster {

class Shard {
public:
  static constexpr std::size_t ARENA_SIZE = 64 * 1024;  // 64KB per shard

  explicit Shard(shard_id id);
  ~Shard();

  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  [[nodiscard]] auto id() const noexcept -> shard_id {
    return id_;
  }
  [[nodiscard]] auto wake_fd() const noexcept -> int {
    return wake_fd_;
  }
  [[nodiscard]] auto ring() noexcept -> IoRing& {
    return ring_;
  }
  [[nodiscard]] auto memory_resource() noexcept -> std::pmr::memory_resource* {
    return std::pmr::get_default_resource();
  }

  auto schedule_local(std::coroutine_handle<> h) -> void;
  auto schedule_next(std::coroutine_handle<> h) -> void;
  auto schedule_remote(std::coroutine_handle<> h) -> bool;

  auto submit_io(IoRequest req) -> void;
  auto process_ready() -> bool;
  auto process_io() -> void;
  auto drain_pending() -> void;

  [[nodiscard]] auto has_work() const noexcept -> bool;
  [[nodiscard]] auto is_sleeping() const noexcept -> bool;
  auto set_sleeping(bool v) noexcept -> void;

  auto track_io_data(io_data* data) -> void;
  auto untrack_io_data(io_data* data) -> void;

private:
  shard_id id_;
  int wake_fd_ = -1;
  IoRing ring_;

  alignas(64) std::array<std::byte, ARENA_SIZE> arena_;
  std::pmr::monotonic_buffer_resource upstream_{arena_.data(), arena_.size()};
  std::pmr::unsynchronized_pool_resource pool_{&upstream_};

  std::optional<std::coroutine_handle<>> run_next_;
  std::deque<std::coroutine_handle<>> local_queue_;
  BoundedMPSCQueue<std::coroutine_handle<>> remote_queue_{4096};
  std::deque<IoRequest> io_queue_;
  std::unordered_set<io_data*> pending_io_;

  std::atomic<bool> sleeping_{false};
};

}  // namespace taskmaster
