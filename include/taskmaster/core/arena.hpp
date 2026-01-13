#pragma once

#include <array>
#include <cstddef>
#include <memory_resource>
#include <string>
#include <vector>

namespace taskmaster {

template <std::size_t N = 1024>
class Arena {
public:
  Arena() : resource_(buffer_.data(), buffer_.size()) {}

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  template <typename T>
  [[nodiscard]] auto vector() -> std::pmr::vector<T> {
    return std::pmr::vector<T>(&resource_);
  }

  template <typename T>
  [[nodiscard]] auto vector(std::size_t reserve_size) -> std::pmr::vector<T> {
    std::pmr::vector<T> v(&resource_);
    v.reserve(reserve_size);
    return v;
  }

  [[nodiscard]] auto string() -> std::pmr::string {
    return std::pmr::string(&resource_);
  }

  [[nodiscard]] auto resource() -> std::pmr::memory_resource* {
    return &resource_;
  }

private:
  std::array<std::byte, N> buffer_;
  std::pmr::monotonic_buffer_resource resource_;
};

template <typename T>
[[nodiscard]] auto to_std(std::pmr::vector<T>&& pmr_vec) -> std::vector<T> {
  return std::vector<T>(
      std::make_move_iterator(pmr_vec.begin()),
      std::make_move_iterator(pmr_vec.end()));
}

[[nodiscard]] inline auto to_std(std::pmr::string&& pmr_str) -> std::string {
  return std::string(std::move(pmr_str));
}

}
