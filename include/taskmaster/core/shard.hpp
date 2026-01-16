#pragma once

#include "taskmaster/io/async_fd.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/core/lockfree_queue.hpp"

#include <array>
#include <atomic>
#include <coroutine>
#include <deque>
#include <memory_resource>
#include <optional>
#include <unordered_set>

namespace taskmaster {

using shard_id = unsigned;
using io::kInvalidShard;
using io::kWakeEventToken;
using io::kMsgRingWakeToken;
using io::kCqeFMore;
using io::CompletionData;
using io::IoRequest;
using io::IoOpType;
using io::KernelTimespec;

class Shard {
public:
  static constexpr std::size_t kArenaSize = 64 * 1024;  // 64KB per shard

  explicit Shard(shard_id id);
  ~Shard();

  Shard(const Shard&) = delete;
  Shard& operator=(const Shard&) = delete;

  [[nodiscard]] auto id() const noexcept -> shard_id {
    return id_;
  }
  [[nodiscard]] auto wake_fd() const noexcept -> int {
    return wake_fd_.fd();
  }
  [[nodiscard]] auto wake_event() noexcept -> io::AsyncEventFd& {
    return wake_fd_;
  }
  [[nodiscard]] auto ctx() noexcept -> io::IoContext& {
    return ctx_;
  }
  [[nodiscard]] auto ring_fd() const noexcept -> int {
    return ctx_.ring_fd();
  }
  [[nodiscard]] auto memory_resource() noexcept -> std::pmr::memory_resource* {
    return &pool_;
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

  auto track_io_data(CompletionData* data) -> void;
  auto untrack_io_data(CompletionData* data) -> void;

private:
  shard_id id_;

  alignas(64) std::array<std::byte, kArenaSize> arena_;
  std::pmr::monotonic_buffer_resource upstream_{arena_.data(), arena_.size()};
  std::pmr::unsynchronized_pool_resource pool_{&upstream_};

  io::IoContext ctx_;
  io::AsyncEventFd wake_fd_;

  std::optional<std::coroutine_handle<>> run_next_;
  std::deque<std::coroutine_handle<>> local_queue_;
  // Reusable buffer for batch processing to avoid allocation
  std::deque<std::coroutine_handle<>> batch_buffer_;
  
  // Align remote queue to cache line to prevent false sharing
  alignas(64) BoundedMPSCQueue<std::coroutine_handle<>> remote_queue_{4096};
  
  std::deque<IoRequest> io_queue_;
  std::unordered_set<CompletionData*> pending_io_;

  alignas(64) std::atomic<bool> sleeping_{false};
};

}  // namespace taskmaster
