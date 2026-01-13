#pragma once

#include "taskmaster/core/lockfree_queue.hpp"
#include "taskmaster/core/shard.hpp"

#include <cassert>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <span>
#include <thread>
#include <variant>
#include <vector>

namespace taskmaster {

struct SmpMessage {
  std::variant<std::coroutine_handle<>, std::move_only_function<void()>>
      payload;

  explicit SmpMessage(std::coroutine_handle<> h) : payload(h) {
  }
  explicit SmpMessage(std::move_only_function<void()> f)
      : payload(std::move(f)) {
  }

  auto execute() -> void {
    std::visit(
        [](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
            if (arg && !arg.done()) {
              arg.resume();
            }
          } else {
            arg();
          }
        },
        payload);
  }
};

class Runtime {
public:
  explicit Runtime(unsigned num_shards = 0);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto schedule(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto get_shard_id() const noexcept -> unsigned;
  [[nodiscard]] auto is_current_shard() const noexcept -> bool;

  auto schedule_on(shard_id target, std::coroutine_handle<> handle) -> void;
  auto schedule_external(std::coroutine_handle<> handle) -> void;

  [[nodiscard]] auto shard_count() const noexcept -> unsigned {
    return num_shards_;
  }

  [[nodiscard]] auto current_shard() const noexcept -> shard_id;
  [[nodiscard]] auto current_context() noexcept -> io::IoContext&;

private:
  auto submit_to(shard_id target, std::coroutine_handle<> handle) -> void;

  auto run_shard(shard_id id) -> void;
  auto process_smp_messages(shard_id id) -> bool;
  auto process_completions(Shard& shard) -> bool;
  auto wait_for_work(Shard& shard) -> void;
  auto wake_shard(shard_id id) -> void;

  unsigned num_shards_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::vector<std::jthread> threads_;
  std::vector<std::vector<
      std::unique_ptr<SPSCQueue<SmpMessage, std::allocator<SmpMessage>>>>>
      smp_queues_;

  alignas(kCacheLineSize) std::atomic<bool> running_{false};
  alignas(kCacheLineSize) std::atomic<bool> stop_requested_{false};
};

// Thread-local for internal use only
namespace detail {
inline thread_local shard_id current_shard_id = kInvalidShard;
inline thread_local Runtime* current_runtime = nullptr;
}  // namespace detail

class yield_awaiter {
public:
  yield_awaiter() noexcept = default;

  yield_awaiter(const yield_awaiter&) = delete;
  yield_awaiter& operator=(const yield_awaiter&) = delete;
  yield_awaiter(yield_awaiter&&) = delete;
  yield_awaiter& operator=(yield_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }

  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void {
    detail::current_runtime->schedule(handle);
  }

  auto await_resume() const noexcept -> void {
  }
};

[[nodiscard]] inline auto async_yield() noexcept -> yield_awaiter {
  return yield_awaiter{};
}

[[nodiscard]] inline auto current_io_context() noexcept -> io::IoContext& {
  assert(detail::current_runtime != nullptr);
  assert(detail::current_shard_id != kInvalidShard);
  return detail::current_runtime->current_context();
}

[[nodiscard]] inline auto async_read(int fd, void* buf,
                                     std::uint32_t len) noexcept {
  return current_io_context().async_read(fd, io::buffer(buf, len));
}

[[nodiscard]] inline auto async_read(int fd,
                                     std::span<std::byte> buf) noexcept {
  return current_io_context().async_read(fd, io::buffer(buf));
}

[[nodiscard]] inline auto async_write(int fd, const void* buf,
                                      std::uint32_t len) noexcept {
  return current_io_context().async_write(fd, io::buffer(buf, len));
}

[[nodiscard]] inline auto async_write(int fd,
                                      std::span<const std::byte> buf) noexcept {
  return current_io_context().async_write(fd, io::buffer(buf));
}

[[nodiscard]] inline auto async_poll(int fd, std::uint32_t mask) noexcept {
  return current_io_context().async_poll(fd, mask);
}

[[nodiscard]] inline auto
async_poll_timeout(int fd, std::uint32_t mask,
                   std::chrono::milliseconds timeout) noexcept {
  return current_io_context().async_poll_timeout(fd, mask, timeout);
}

template <typename Rep, typename Period>
[[nodiscard]] inline auto async_sleep(
    std::chrono::duration<Rep, Period> duration) noexcept {
  return io::async_sleep(
      current_io_context(),
      std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

}  // namespace taskmaster
