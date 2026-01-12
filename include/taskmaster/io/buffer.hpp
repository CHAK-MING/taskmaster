#pragma once

#include <concepts>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace taskmaster::io {

// ============================================================================
// Buffer Concepts - Compile-time constraints for buffer types
// ============================================================================

/// Concept for types that can be used as mutable buffers
template <typename T>
concept MutableBufferSequence = requires(T& t) {
  { std::data(t) } -> std::convertible_to<void*>;
  { std::size(t) } -> std::convertible_to<std::size_t>;
  requires !std::is_const_v<std::remove_reference_t<decltype(*std::data(t))>>;
};

/// Concept for types that can be used as const buffers
template <typename T>
concept ConstBufferSequence = requires(const T& t) {
  { std::data(t) } -> std::convertible_to<const void*>;
  { std::size(t) } -> std::convertible_to<std::size_t>;
};

// ============================================================================
// Buffer Views - Type-safe buffer representations
// ============================================================================

/// Mutable buffer view (for reading into)
class MutableBuffer {
public:
  constexpr MutableBuffer() noexcept = default;

  constexpr MutableBuffer(void* data, std::size_t size) noexcept
      : data_(static_cast<std::byte*>(data)), size_(size) {}

  template <MutableBufferSequence T>
  constexpr MutableBuffer(T& container) noexcept
      : data_(reinterpret_cast<std::byte*>(std::data(container))),
        size_(std::size(container) * sizeof(*std::data(container))) {}

  [[nodiscard]] constexpr auto data() const noexcept -> std::byte* {
    return data_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return size_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  /// Advance buffer by n bytes
  constexpr auto operator+=(std::size_t n) noexcept -> MutableBuffer& {
    auto advance = (n < size_) ? n : size_;
    data_ += advance;
    size_ -= advance;
    return *this;
  }

  [[nodiscard]] constexpr auto subspan(std::size_t offset,
                                       std::size_t count = -1) const noexcept
      -> MutableBuffer {
    if (offset >= size_) return {};
    auto actual_count = (count == static_cast<std::size_t>(-1) || offset + count > size_)
                            ? size_ - offset
                            : count;
    return {data_ + offset, actual_count};
  }

private:
  std::byte* data_ = nullptr;
  std::size_t size_ = 0;
};

/// Const buffer view (for writing from)
class ConstBuffer {
public:
  constexpr ConstBuffer() noexcept = default;

  constexpr ConstBuffer(const void* data, std::size_t size) noexcept
      : data_(static_cast<const std::byte*>(data)), size_(size) {}

  template <ConstBufferSequence T>
  constexpr ConstBuffer(const T& container) noexcept
      : data_(reinterpret_cast<const std::byte*>(std::data(container))),
        size_(std::size(container) * sizeof(*std::data(container))) {}

  // Allow implicit conversion from MutableBuffer
  constexpr ConstBuffer(MutableBuffer buf) noexcept
      : data_(buf.data()), size_(buf.size()) {}

  [[nodiscard]] constexpr auto data() const noexcept -> const std::byte* {
    return data_;
  }

  [[nodiscard]] constexpr auto size() const noexcept -> std::size_t {
    return size_;
  }

  [[nodiscard]] constexpr auto empty() const noexcept -> bool {
    return size_ == 0;
  }

  constexpr auto operator+=(std::size_t n) noexcept -> ConstBuffer& {
    auto advance = (n < size_) ? n : size_;
    data_ += advance;
    size_ -= advance;
    return *this;
  }

  [[nodiscard]] constexpr auto subspan(std::size_t offset,
                                       std::size_t count = -1) const noexcept
      -> ConstBuffer {
    if (offset >= size_) return {};
    auto actual_count = (count == static_cast<std::size_t>(-1) || offset + count > size_)
                            ? size_ - offset
                            : count;
    return {data_ + offset, actual_count};
  }

private:
  const std::byte* data_ = nullptr;
  std::size_t size_ = 0;
};

// ============================================================================
// Buffer Factory Functions
// ============================================================================

/// Create mutable buffer from container
template <MutableBufferSequence T>
[[nodiscard]] constexpr auto buffer(T& container) noexcept -> MutableBuffer {
  return MutableBuffer{container};
}

/// Create mutable buffer from pointer and size
[[nodiscard]] constexpr auto buffer(void* data, std::size_t size) noexcept
    -> MutableBuffer {
  return {data, size};
}

/// Create const buffer from container
template <ConstBufferSequence T>
[[nodiscard]] constexpr auto buffer(const T& container) noexcept -> ConstBuffer {
  return ConstBuffer{container};
}

/// Create const buffer from pointer and size
[[nodiscard]] constexpr auto buffer(const void* data, std::size_t size) noexcept
    -> ConstBuffer {
  return {data, size};
}

/// Create const buffer from string_view
[[nodiscard]] constexpr auto buffer(std::string_view sv) noexcept -> ConstBuffer {
  return {sv.data(), sv.size()};
}

}  // namespace taskmaster::io
