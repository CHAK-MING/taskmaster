#pragma once

#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/io_ring.hpp"
#include "taskmaster/core/shard.hpp"

#include <atomic>
#include <chrono>
#include <expected>
#include <functional>
#include <memory>
#include <span>
#include <system_error>
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
            if (arg && arg.done()) {
              arg.destroy();
            }
          } else {
            arg();
          }
        },
        payload);
  }
};

class Runtime : public scheduler {
public:
  explicit Runtime(unsigned num_shards = 0);
  ~Runtime();

  Runtime(const Runtime&) = delete;
  Runtime& operator=(const Runtime&) = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto schedule(std::coroutine_handle<> handle) noexcept -> void override;
  [[nodiscard]] auto get_shard_id() const noexcept -> unsigned override;
  [[nodiscard]] auto is_current_shard() const noexcept -> bool override;

  auto schedule_on(shard_id target, std::coroutine_handle<> handle) -> void;
  auto schedule_external(std::coroutine_handle<> handle) -> void;
  auto submit_io(IoRequest req) -> bool;

  [[nodiscard]] auto shard_count() const noexcept -> unsigned {
    return num_shards_;
  }

  [[nodiscard]] auto current_shard() const noexcept -> shard_id;

  [[nodiscard]] auto alloc_io_data() -> io_data*;
  auto free_io_data(io_data* data) -> void;

private:
  friend class read_awaiter;
  friend class write_awaiter;
  friend class poll_awaiter;
  friend class poll_timeout_awaiter;
  friend class sleep_awaiter;
  friend class close_awaiter;
  friend class cancel_awaiter;

  auto submit_to(shard_id target, std::coroutine_handle<> handle) -> void;

  auto run_shard(shard_id id) -> void;
  auto process_smp_messages(shard_id id) -> bool;
  auto process_completions(Shard& shard) -> bool;
  auto wait_for_work(Shard& shard) -> void;
  auto wake_shard(shard_id id) -> void;

  unsigned num_shards_;
  std::vector<std::unique_ptr<Shard>> shards_;
  std::vector<std::thread> threads_;
  std::vector<std::vector<
      std::unique_ptr<SPSCQueue<SmpMessage, std::allocator<SmpMessage>>>>>
      smp_queues_;

  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
};

// Thread-local for internal use only
namespace detail {
inline thread_local shard_id current_shard_id = INVALID_SHARD;
inline thread_local Runtime* current_runtime = nullptr;
}  // namespace detail

struct PollResult {
  bool ready = false;
  bool timed_out = false;
  std::errc error{};

  [[nodiscard]] explicit operator bool() const noexcept {
    return ready;
  }
  [[nodiscard]] auto has_error() const noexcept -> bool {
    return !ready && !timed_out && error != std::errc{};
  }
};

[[nodiscard]] inline auto decode_result(std::int32_t result) noexcept
    -> std::expected<std::uint32_t, std::errc> {
  if (result < 0)
    return std::unexpected{static_cast<std::errc>(-result)};
  return static_cast<std::uint32_t>(result);
}

[[nodiscard]] inline auto decode_void_result(std::int32_t result) noexcept
    -> std::expected<void, std::errc> {
  if (result < 0)
    return std::unexpected{static_cast<std::errc>(-result)};
  return {};
}

class read_awaiter {
public:
  read_awaiter(int fd, void* buf, std::uint32_t len,
               std::uint64_t offset = 0) noexcept
      : fd_{fd}, buf_{buf}, len_{len}, offset_{offset} {
  }

  read_awaiter(const read_awaiter&) = delete;
  read_awaiter& operator=(const read_awaiter&) = delete;
  read_awaiter(read_awaiter&&) = delete;
  read_awaiter& operator=(read_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept
      -> std::expected<std::uint32_t, std::errc> {
    auto result = decode_result(data_->result);
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  int fd_;
  void* buf_;
  std::uint32_t len_;
  std::uint64_t offset_;
};

class write_awaiter {
public:
  write_awaiter(int fd, const void* buf, std::uint32_t len,
                std::uint64_t offset = 0) noexcept
      : fd_{fd}, buf_{buf}, len_{len}, offset_{offset} {
  }

  write_awaiter(const write_awaiter&) = delete;
  write_awaiter& operator=(const write_awaiter&) = delete;
  write_awaiter(write_awaiter&&) = delete;
  write_awaiter& operator=(write_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept
      -> std::expected<std::uint32_t, std::errc> {
    auto result = decode_result(data_->result);
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  int fd_;
  const void* buf_;
  std::uint32_t len_;
  std::uint64_t offset_;
};

class poll_awaiter {
public:
  poll_awaiter(int fd, std::uint32_t mask) noexcept : fd_{fd}, mask_{mask} {
  }

  poll_awaiter(const poll_awaiter&) = delete;
  poll_awaiter& operator=(const poll_awaiter&) = delete;
  poll_awaiter(poll_awaiter&&) = delete;
  poll_awaiter& operator=(poll_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept
      -> std::expected<std::uint32_t, std::errc> {
    auto result = decode_result(data_->result);
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  int fd_;
  std::uint32_t mask_;
};

class poll_timeout_awaiter {
public:
  poll_timeout_awaiter(int fd, std::uint32_t mask,
                       std::chrono::milliseconds timeout) noexcept
      : fd_{fd}, mask_{mask}, timeout_{timeout} {
  }

  poll_timeout_awaiter(const poll_timeout_awaiter&) = delete;
  poll_timeout_awaiter& operator=(const poll_timeout_awaiter&) = delete;
  poll_timeout_awaiter(poll_timeout_awaiter&&) = delete;
  poll_timeout_awaiter& operator=(poll_timeout_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept -> PollResult {
    PollResult result;
    if (data_->result > 0)
      result = {.ready = true};
    else if (data_->result == -ECANCELED)
      result = {.timed_out = true};
    else if (data_->result < 0)
      result = {.error = static_cast<std::errc>(-data_->result)};
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  int fd_;
  std::uint32_t mask_;
  std::chrono::milliseconds timeout_;
};

class sleep_awaiter {
public:
  explicit sleep_awaiter(std::chrono::milliseconds duration) noexcept
      : duration_{duration} {
  }

  sleep_awaiter(const sleep_awaiter&) = delete;
  sleep_awaiter& operator=(const sleep_awaiter&) = delete;
  sleep_awaiter(sleep_awaiter&&) = delete;
  sleep_awaiter& operator=(sleep_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept -> std::expected<void, std::errc> {
    std::expected<void, std::errc> result;
    if (data_->result == -ETIME)
      result = {};
    else
      result = decode_void_result(data_->result);
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  std::chrono::milliseconds duration_;
};

class close_awaiter {
public:
  explicit close_awaiter(int fd) noexcept : fd_{fd} {
  }

  close_awaiter(const close_awaiter&) = delete;
  close_awaiter& operator=(const close_awaiter&) = delete;
  close_awaiter(close_awaiter&&) = delete;
  close_awaiter& operator=(close_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept -> std::expected<void, std::errc> {
    auto result = decode_void_result(data_->result);
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  int fd_;
};

class cancel_awaiter {
public:
  explicit cancel_awaiter(std::uint64_t user_data) noexcept
      : cancel_user_data_{user_data} {
  }

  cancel_awaiter(const cancel_awaiter&) = delete;
  cancel_awaiter& operator=(const cancel_awaiter&) = delete;
  cancel_awaiter(cancel_awaiter&&) = delete;
  cancel_awaiter& operator=(cancel_awaiter&&) = delete;

  [[nodiscard]] auto await_ready() const noexcept -> bool {
    return false;
  }
  auto await_suspend(std::coroutine_handle<> handle) noexcept -> void;
  [[nodiscard]] auto await_resume() noexcept -> bool {
    auto result = data_->result >= 0;
    detail::current_runtime->free_io_data(data_);
    data_ = nullptr;
    return result;
  }

private:
  io_data* data_{nullptr};
  std::uint64_t cancel_user_data_;
};

[[nodiscard]] inline auto async_read(int fd, void* buf,
                                     std::uint32_t len) noexcept
    -> read_awaiter {
  return read_awaiter{fd, buf, len, 0};
}

[[nodiscard]] inline auto async_read(int fd, std::span<std::byte> buf) noexcept
    -> read_awaiter {
  return read_awaiter{fd, buf.data(), static_cast<std::uint32_t>(buf.size()),
                      0};
}

[[nodiscard]] inline auto async_write(int fd, const void* buf,
                                      std::uint32_t len) noexcept
    -> write_awaiter {
  return write_awaiter{fd, buf, len, 0};
}

[[nodiscard]] inline auto async_write(int fd,
                                      std::span<const std::byte> buf) noexcept
    -> write_awaiter {
  return write_awaiter{fd, buf.data(), static_cast<std::uint32_t>(buf.size()),
                       0};
}

[[nodiscard]] inline auto async_poll(int fd, std::uint32_t mask) noexcept
    -> poll_awaiter {
  return poll_awaiter{fd, mask};
}

[[nodiscard]] inline auto
async_poll_timeout(int fd, std::uint32_t mask,
                   std::chrono::milliseconds timeout) noexcept
    -> poll_timeout_awaiter {
  return poll_timeout_awaiter{fd, mask, timeout};
}

[[nodiscard]] inline auto
async_sleep(std::chrono::milliseconds duration) noexcept -> sleep_awaiter {
  return sleep_awaiter{duration};
}

[[nodiscard]] inline auto async_close(int fd) noexcept -> close_awaiter {
  return close_awaiter{fd};
}

[[nodiscard]] inline auto async_cancel(std::uint64_t user_data) noexcept
    -> cancel_awaiter {
  return cancel_awaiter{user_data};
}

[[nodiscard]] inline auto async_cancel(io_data* data) noexcept
    -> cancel_awaiter {
  return cancel_awaiter{reinterpret_cast<std::uint64_t>(data)};
}

}  // namespace taskmaster
