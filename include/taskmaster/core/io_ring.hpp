#pragma once

#include <chrono>
#include <cstdint>

#include <liburing.h>

namespace taskmaster {

using shard_id = unsigned;
inline constexpr shard_id INVALID_SHARD = ~0u;
inline constexpr std::uint32_t RING_SIZE = 256;
inline constexpr std::uintptr_t WAKE_EVENT_TOKEN = 0x1;

struct io_data {
  void* coroutine = nullptr;
  std::int32_t result = 0;
  std::uint32_t flags = 0;
  __kernel_timespec ts{};
  shard_id owner_shard = INVALID_SHARD;
};

enum class IoOpType : std::uint8_t {
  Read,
  Write,
  Poll,
  PollTimeout,
  Timeout,
  Close,
  Cancel,
  Nop
};

struct IoRequest {
  IoOpType op{IoOpType::Nop};
  io_data* data{nullptr};
  int fd{-1};
  void* buf{nullptr};
  std::uint32_t len{0};
  std::uint64_t offset{0};
  std::uint32_t poll_mask{0};
  __kernel_timespec ts{};
  __kernel_timespec* ts_ptr{nullptr};
  bool has_link_timeout{false};
  std::uint64_t cancel_user_data{0};
};

class IoRing {
public:
  IoRing();
  ~IoRing();

  IoRing(const IoRing&) = delete;
  IoRing& operator=(const IoRing&) = delete;

  [[nodiscard]] auto valid() const noexcept -> bool {
    return initialized_;
  }

  auto prepare(const IoRequest& req) -> bool;
  auto submit(bool force = false) -> int;

  template <typename Callback>
  auto process_completions(Callback&& cb) -> unsigned {
    if (!initialized_)
      return 0;

    io_uring_cqe* cqe = nullptr;
    unsigned head, count = 0;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      cb(io_uring_cqe_get_data(cqe), cqe->res, cqe->flags);
      ++count;
    }
    io_uring_cq_advance(&ring_, count);
    return count;
  }

  auto wait(std::chrono::milliseconds timeout) -> void;
  auto setup_wake_poll(int fd) -> void;

private:
  io_uring ring_{};
  bool initialized_ = false;
  std::uint32_t pending_count_ = 0;
  static constexpr std::uint32_t BATCH_SIZE = 8;
};

}  // namespace taskmaster
