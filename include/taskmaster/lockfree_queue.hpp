#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

namespace taskmaster {

inline constexpr std::size_t CACHE_LINE_SIZE = 64;

// High-performance bounded lock-free MPMC queue
template <typename T>
class LockFreeQueue {
public:
  explicit LockFreeQueue(std::size_t capacity = 1024)
      : capacity_(std::bit_ceil(std::max(capacity, std::size_t{64}))),
        mask_(capacity_ - 1),
        slots_(std::make_unique<Slot[]>(capacity_)) {
    for (std::size_t i = 0; i < capacity_; ++i) {
      slots_[i].turn.store(i, std::memory_order_relaxed);
    }
  }

  ~LockFreeQueue() {
    // Drain remaining elements
    while (try_pop().has_value()) {}
  }

  LockFreeQueue(const LockFreeQueue&) = delete;
  LockFreeQueue& operator=(const LockFreeQueue&) = delete;
  LockFreeQueue(LockFreeQueue&&) = delete;
  LockFreeQueue& operator=(LockFreeQueue&&) = delete;

  [[nodiscard]] auto push(T value) noexcept -> bool {
    std::size_t head = head_.load(std::memory_order_relaxed);

    for (;;) {
      Slot& slot = slots_[head & mask_];
      std::size_t turn = slot.turn.load(std::memory_order_acquire);
      auto diff = static_cast<std::ptrdiff_t>(turn) - static_cast<std::ptrdiff_t>(head);

      if (diff == 0) {
        // Slot is ready for writing, try to claim it
        if (head_.compare_exchange_weak(head, head + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
          std::construct_at(reinterpret_cast<T*>(&slot.storage), std::move(value));
          slot.turn.store(head + 1, std::memory_order_release);
          return true;
        }
        // CAS failed, head updated, retry
      } else if (diff < 0) {
        // Queue is full
        return false;
      } else {
        // Another producer claimed this slot, reload head
        head = head_.load(std::memory_order_relaxed);
      }
    }
  }

  [[nodiscard]] auto try_pop() noexcept -> std::optional<T> {
    std::size_t tail = tail_.load(std::memory_order_relaxed);

    for (;;) {
      Slot& slot = slots_[tail & mask_];
      std::size_t turn = slot.turn.load(std::memory_order_acquire);
      auto diff = static_cast<std::ptrdiff_t>(turn) - static_cast<std::ptrdiff_t>(tail + 1);

      if (diff == 0) {
        // Slot has data, try to claim it
        if (tail_.compare_exchange_weak(tail, tail + 1,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
          T value = std::move(*reinterpret_cast<T*>(&slot.storage));
          std::destroy_at(reinterpret_cast<T*>(&slot.storage));
          slot.turn.store(tail + capacity_, std::memory_order_release);
          return value;
        }
      } else if (diff < 0) {
        return std::nullopt;
      } else {
        tail = tail_.load(std::memory_order_relaxed);
      }
    }
  }

  auto try_pop_batch(std::vector<T>& out, std::size_t max_count) noexcept -> std::size_t {
    out.clear();
    out.reserve(max_count);

    std::size_t count = 0;
    while (count < max_count) {
      auto value = try_pop();
      if (!value.has_value()) {
        break;
      }
      out.push_back(std::move(*value));
      ++count;
    }

    return count;
  }

  // Alias for compatibility
  auto pop_bulk(std::vector<T>& out, std::size_t max_count) noexcept -> std::size_t {
    return try_pop_batch(out, max_count);
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    return head_.load(std::memory_order_acquire) ==
           tail_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    auto head = head_.load(std::memory_order_acquire);
    auto tail = tail_.load(std::memory_order_acquire);
    return head >= tail ? head - tail : 0;
  }

  [[nodiscard]] auto capacity() const noexcept -> std::size_t {
    return capacity_;
  }

private:
  struct alignas(CACHE_LINE_SIZE) Slot {
    std::atomic<std::size_t> turn{0};
    alignas(alignof(T)) std::byte storage[sizeof(T)];
  };

  const std::size_t capacity_;
  const std::size_t mask_;
  std::unique_ptr<Slot[]> slots_;

  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};
};

} // namespace taskmaster
