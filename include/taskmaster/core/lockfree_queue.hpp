#pragma once

#include <atomic>
#include <new>
#include <concepts>
#include <cstddef>
#include <memory>
#include <optional>
#include <thread>

namespace taskmaster {

inline constexpr std::size_t kCacheLineSize =
#ifdef __cpp_lib_hardware_interference_size
    std::hardware_destructive_interference_size;
#else
    64;
#endif

// Concept for queue element types
template <typename T>
concept QueueElement = std::movable<T> && std::destructible<T>;

template <QueueElement T, typename Allocator = std::allocator<T>>
  requires std::same_as<typename Allocator::value_type, T>
class SPSCQueue {
public:
  explicit SPSCQueue(std::size_t capacity, Allocator alloc = Allocator())
      : allocator_(alloc) {
    capacity_ = capacity < 1 ? 1 : capacity;
    capacity_++;
    slots_ = std::allocator_traits<Allocator>::allocate(
        allocator_, capacity_ + 2 * kPadding);
  }

  ~SPSCQueue() {
    while (front()) {
      pop();
    }
    std::allocator_traits<Allocator>::deallocate(allocator_, slots_,
                                                 capacity_ + 2 * kPadding);
  }

  SPSCQueue(const SPSCQueue&) = delete;
  SPSCQueue& operator=(const SPSCQueue&) = delete;

  template <typename... Args>
    requires std::constructible_from<T, Args...>
  [[nodiscard]] auto try_push(Args&&... args) noexcept -> bool {
    auto write_idx = write_idx_.load(std::memory_order_relaxed);
    auto next_idx = write_idx + 1 == capacity_ ? 0 : write_idx + 1;

    if (next_idx == read_idx_cache_) [[unlikely]] {
      read_idx_cache_ = read_idx_.load(std::memory_order_acquire);
      if (next_idx == read_idx_cache_) [[unlikely]]
        return false;
    }

    std::construct_at(&slots_[write_idx + kPadding],
                      std::forward<Args>(args)...);
    write_idx_.store(next_idx, std::memory_order_release);
    return true;
  }

  [[nodiscard]] auto push(T value) noexcept -> bool {
    return try_push(std::move(value));
  }

  [[nodiscard]] auto front() noexcept -> T* {
    auto read_idx = read_idx_.load(std::memory_order_relaxed);
    if (read_idx == write_idx_cache_) [[unlikely]] {
      write_idx_cache_ = write_idx_.load(std::memory_order_acquire);
      if (read_idx == write_idx_cache_) [[unlikely]]
        return nullptr;
    }
    return &slots_[read_idx + kPadding];
  }

  auto pop() noexcept -> void {
    auto read_idx = read_idx_.load(std::memory_order_relaxed);
    std::destroy_at(&slots_[read_idx + kPadding]);
    auto next_idx = read_idx + 1 == capacity_ ? 0 : read_idx + 1;
    read_idx_.store(next_idx, std::memory_order_release);
  }

  [[nodiscard]] auto try_pop() noexcept -> std::optional<T> {
    auto* p = front();
    if (!p) [[unlikely]]
      return std::nullopt;
    T value = std::move(*p);
    pop();
    return value;
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    return write_idx_.load(std::memory_order_acquire) ==
           read_idx_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    auto diff =
        static_cast<std::ptrdiff_t>(write_idx_.load(std::memory_order_acquire)) -
        static_cast<std::ptrdiff_t>(read_idx_.load(std::memory_order_acquire));
    return diff < 0 ? diff + capacity_ : diff;
  }

  [[nodiscard]] auto capacity() const noexcept -> std::size_t {
    return capacity_ - 1;
  }

private:
  static constexpr std::size_t kPadding = (kCacheLineSize - 1) / sizeof(T) + 1;

  std::size_t capacity_;
  T* slots_;
  [[no_unique_address]] Allocator allocator_;

  alignas(kCacheLineSize) std::atomic<std::size_t> write_idx_{0};
  alignas(kCacheLineSize) std::size_t read_idx_cache_{0};
  alignas(kCacheLineSize) std::atomic<std::size_t> read_idx_{0};
  alignas(kCacheLineSize) std::size_t write_idx_cache_{0};
};

struct MPSCNode {
  std::atomic<MPSCNode*> next{nullptr};
};

// Concept for MPSC queue node types
template <typename T>
concept MPSCNodeDerived = std::derived_from<T, MPSCNode>;

template <MPSCNodeDerived T>
class MPSCQueue {
public:
  MPSCQueue() {
    head_.store(&stub_, std::memory_order_relaxed);
    tail_.store(&stub_, std::memory_order_relaxed);
  }

  MPSCQueue(const MPSCQueue&) = delete;
  MPSCQueue& operator=(const MPSCQueue&) = delete;

  auto push(T* node) noexcept -> void {
    node->next.store(nullptr, std::memory_order_relaxed);
    auto* prev = head_.exchange(node, std::memory_order_acq_rel);
    prev->next.store(node, std::memory_order_release);
  }

  [[nodiscard]] auto try_pop() noexcept -> T* {
    auto* tail = tail_.load(std::memory_order_relaxed);
    auto* next = tail->next.load(std::memory_order_acquire);

    if (tail == &stub_) {
      if (!next)
        return nullptr;
      tail_.store(next, std::memory_order_relaxed);
      tail = next;
      next = tail->next.load(std::memory_order_acquire);
    }

    if (next) {
      tail_.store(next, std::memory_order_relaxed);
      return static_cast<T*>(tail);
    }

    auto* head = head_.load(std::memory_order_acquire);
    if (tail != head)
      return nullptr;

    push(static_cast<T*>(&stub_));

    next = tail->next.load(std::memory_order_acquire);
    if (next) {
      tail_.store(next, std::memory_order_relaxed);
      return static_cast<T*>(tail);
    }
    return nullptr;
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    auto* tail = tail_.load(std::memory_order_relaxed);
    auto* next = tail->next.load(std::memory_order_acquire);
    auto* head = head_.load(std::memory_order_acquire);
    return tail == &stub_ && !next && tail == head;
  }

private:
  alignas(kCacheLineSize) std::atomic<MPSCNode*> head_;
  alignas(kCacheLineSize) std::atomic<MPSCNode*> tail_;
  MPSCNode stub_;
};

template <QueueElement T>
class BoundedMPSCQueue {
public:
  explicit BoundedMPSCQueue(std::size_t capacity)
      : capacity_(std::bit_ceil(capacity)),
        mask_(capacity_ - 1),
        slots_(std::make_unique<Slot[]>(capacity_)) {
    for (std::size_t i = 0; i < capacity_; ++i)
      slots_[i].seq.store(i, std::memory_order_relaxed);
  }

  ~BoundedMPSCQueue() {
    while (try_pop().has_value())
      ;
  }

  BoundedMPSCQueue(const BoundedMPSCQueue&) = delete;
  BoundedMPSCQueue& operator=(const BoundedMPSCQueue&) = delete;
  BoundedMPSCQueue(BoundedMPSCQueue&&) = delete;
  BoundedMPSCQueue& operator=(BoundedMPSCQueue&&) = delete;

  [[nodiscard]] auto push(T value) noexcept -> bool {
    auto pos = head_.load(std::memory_order_relaxed);
    for (;;) {
      auto& slot = slots_[pos & mask_];
      auto seq = slot.seq.load(std::memory_order_acquire);
      auto diff =
          static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) {
        if (head_.compare_exchange_weak(pos, pos + 1,
                                        std::memory_order_acq_rel,
                                        std::memory_order_relaxed)) {
          std::construct_at(slot.ptr(), std::move(value));
          slot.seq.store(pos + 1, std::memory_order_release);
          return true;
        }
      } else if (diff < 0) {
        return false;
      } else {
        pos = head_.load(std::memory_order_relaxed);
      }
    }
  }

  [[nodiscard]] auto try_pop() noexcept -> std::optional<T> {
    auto pos = tail_.load(std::memory_order_relaxed);
    auto& slot = slots_[pos & mask_];
    auto seq = slot.seq.load(std::memory_order_acquire);

    if (seq == pos + 1) {
      T value = std::move(*slot.ptr());
      std::destroy_at(slot.ptr());
      slot.seq.store(pos + capacity_, std::memory_order_release);
      tail_.store(pos + 1, std::memory_order_relaxed);
      return value;
    }
    return std::nullopt;
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    return head_.load(std::memory_order_acquire) ==
           tail_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto capacity() const noexcept -> std::size_t {
    return capacity_;
  }

  auto push_blocking(T value) noexcept -> void {
    while (!push(std::move(value))) {
      std::this_thread::yield();
    }
  }

private:
  struct Slot {
    std::atomic<std::size_t> seq;
    alignas(T) std::byte storage[sizeof(T)];

    auto ptr() noexcept -> T* {
      return std::launder(reinterpret_cast<T*>(storage));
    }
    auto ptr() const noexcept -> const T* {
      return std::launder(reinterpret_cast<const T*>(storage));
    }
  };

  std::size_t capacity_;
  std::size_t mask_;
  std::unique_ptr<Slot[]> slots_;
  alignas(kCacheLineSize) std::atomic<std::size_t> head_{0};
  alignas(kCacheLineSize) std::atomic<std::size_t> tail_{0};
};

}  // namespace taskmaster
