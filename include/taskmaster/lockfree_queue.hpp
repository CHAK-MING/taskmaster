#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>

namespace taskmaster {

#ifdef __cpp_lib_hardware_interference_size
inline constexpr std::size_t CACHE_LINE_SIZE =
    std::hardware_destructive_interference_size;
#else
inline constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

template <typename T, typename Allocator = std::allocator<T>> class SPSCQueue {
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

  SPSCQueue(const SPSCQueue &) = delete;
  SPSCQueue &operator=(const SPSCQueue &) = delete;

  template <typename... Args>
  [[nodiscard]] auto try_push(Args &&...args) noexcept -> bool {
    auto writeIdx = writeIdx_.load(std::memory_order_relaxed);
    auto nextIdx = writeIdx + 1 == capacity_ ? 0 : writeIdx + 1;

    if (nextIdx == readIdxCache_) {
      readIdxCache_ = readIdx_.load(std::memory_order_acquire);
      if (nextIdx == readIdxCache_)
        return false;
    }

    std::construct_at(&slots_[writeIdx + kPadding],
                      std::forward<Args>(args)...);
    writeIdx_.store(nextIdx, std::memory_order_release);
    return true;
  }

  [[nodiscard]] auto push(T value) noexcept -> bool {
    return try_push(std::move(value));
  }

  [[nodiscard]] auto front() noexcept -> T * {
    auto readIdx = readIdx_.load(std::memory_order_relaxed);
    if (readIdx == writeIdxCache_) {
      writeIdxCache_ = writeIdx_.load(std::memory_order_acquire);
      if (readIdx == writeIdxCache_)
        return nullptr;
    }
    return &slots_[readIdx + kPadding];
  }

  auto pop() noexcept -> void {
    auto readIdx = readIdx_.load(std::memory_order_relaxed);
    std::destroy_at(&slots_[readIdx + kPadding]);
    auto nextIdx = readIdx + 1 == capacity_ ? 0 : readIdx + 1;
    readIdx_.store(nextIdx, std::memory_order_release);
  }

  [[nodiscard]] auto try_pop() noexcept -> std::optional<T> {
    auto *p = front();
    if (!p)
      return std::nullopt;
    T value = std::move(*p);
    pop();
    return value;
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    return writeIdx_.load(std::memory_order_acquire) ==
           readIdx_.load(std::memory_order_acquire);
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    auto diff =
        static_cast<std::ptrdiff_t>(writeIdx_.load(std::memory_order_acquire)) -
        static_cast<std::ptrdiff_t>(readIdx_.load(std::memory_order_acquire));
    return diff < 0 ? diff + capacity_ : diff;
  }

  [[nodiscard]] auto capacity() const noexcept -> std::size_t {
    return capacity_ - 1;
  }

private:
  static constexpr std::size_t kPadding = (CACHE_LINE_SIZE - 1) / sizeof(T) + 1;

  std::size_t capacity_;
  T *slots_;
  [[no_unique_address]] Allocator allocator_;

  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> writeIdx_{0};
  alignas(CACHE_LINE_SIZE) std::size_t readIdxCache_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> readIdx_{0};
  alignas(CACHE_LINE_SIZE) std::size_t writeIdxCache_{0};
};

struct MPSCNode {
  std::atomic<MPSCNode *> next{nullptr};
};

template <typename T> class MPSCQueue {
  static_assert(std::is_base_of_v<MPSCNode, T>, "T must derive from MPSCNode");

public:
  MPSCQueue() {
    head_.store(&stub_, std::memory_order_relaxed);
    tail_.store(&stub_, std::memory_order_relaxed);
  }

  MPSCQueue(const MPSCQueue &) = delete;
  MPSCQueue &operator=(const MPSCQueue &) = delete;

  auto push(T *node) noexcept -> void {
    node->next.store(nullptr, std::memory_order_relaxed);
    auto *prev = head_.exchange(node, std::memory_order_acq_rel);
    prev->next.store(node, std::memory_order_release);
  }

  [[nodiscard]] auto try_pop() noexcept -> T * {
    auto *tail = tail_.load(std::memory_order_relaxed);
    auto *next = tail->next.load(std::memory_order_acquire);

    if (tail == &stub_) {
      if (!next)
        return nullptr;
      tail_.store(next, std::memory_order_relaxed);
      tail = next;
      next = tail->next.load(std::memory_order_acquire);
    }

    if (next) {
      tail_.store(next, std::memory_order_relaxed);
      return static_cast<T *>(tail);
    }

    auto *head = head_.load(std::memory_order_acquire);
    if (tail != head)
      return nullptr;

    push(static_cast<T *>(&stub_));

    next = tail->next.load(std::memory_order_acquire);
    if (next) {
      tail_.store(next, std::memory_order_relaxed);
      return static_cast<T *>(tail);
    }
    return nullptr;
  }

  [[nodiscard]] auto empty() const noexcept -> bool {
    auto *tail = tail_.load(std::memory_order_relaxed);
    auto *next = tail->next.load(std::memory_order_acquire);
    auto *head = head_.load(std::memory_order_acquire);
    return tail == &stub_ && !next && tail == head;
  }

private:
  alignas(CACHE_LINE_SIZE) std::atomic<MPSCNode *> head_;
  alignas(CACHE_LINE_SIZE) std::atomic<MPSCNode *> tail_;
  MPSCNode stub_;
};

template <typename T> class BoundedMPSCQueue {
public:
  explicit BoundedMPSCQueue(std::size_t capacity)
      : capacity_(std::bit_ceil(capacity)), mask_(capacity_ - 1),
        slots_(std::make_unique<Slot[]>(capacity_)) {
    for (std::size_t i = 0; i < capacity_; ++i)
      slots_[i].seq.store(i, std::memory_order_relaxed);
  }

  ~BoundedMPSCQueue() {
    while (try_pop().has_value())
      ;
  }

  BoundedMPSCQueue(const BoundedMPSCQueue &) = delete;
  BoundedMPSCQueue &operator=(const BoundedMPSCQueue &) = delete;

  [[nodiscard]] auto push(T value) noexcept -> bool {
    auto pos = head_.load(std::memory_order_relaxed);
    for (;;) {
      auto &slot = slots_[pos & mask_];
      auto seq = slot.seq.load(std::memory_order_acquire);
      auto diff =
          static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) {
        if (head_.compare_exchange_weak(pos, pos + 1,
                                        std::memory_order_relaxed)) {
          std::construct_at(&slot.data, std::move(value));
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
    auto &slot = slots_[pos & mask_];
    auto seq = slot.seq.load(std::memory_order_acquire);

    if (seq == pos + 1) {
      T value = std::move(slot.data);
      std::destroy_at(&slot.data);
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

private:
  struct Slot {
    std::atomic<std::size_t> seq;
    T data;
  };

  std::size_t capacity_;
  std::size_t mask_;
  std::unique_ptr<Slot[]> slots_;
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
  alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};
};

} // namespace taskmaster
