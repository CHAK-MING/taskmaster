#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/contract.hpp"

#include <cstddef>
#include <memory_resource>
#include <thread>
#endif

namespace dagforge {

namespace pmr = std::pmr;

using Allocator = pmr::polymorphic_allocator<std::byte>;

namespace detail {
inline thread_local pmr::memory_resource *thread_memory_resource_override =
    nullptr;
}

// A thread-local allocator override for strictly synchronous scopes. The guard
// must not cross a coroutine suspension point because resumption may occur on a
// different thread.
class ThreadMemoryResourceOverride {
public:
  explicit ThreadMemoryResourceOverride(pmr::memory_resource *resource)
      : resource_(resource), previous_(detail::thread_memory_resource_override),
        owner_(std::this_thread::get_id()) {
    if (resource_ == nullptr) {
      contract_violation(
          "ThreadMemoryResourceOverride requires a non-null resource");
    }
    detail::thread_memory_resource_override = resource_;
  }

  ThreadMemoryResourceOverride(const ThreadMemoryResourceOverride &) = delete;
  auto operator=(const ThreadMemoryResourceOverride &)
      -> ThreadMemoryResourceOverride & = delete;
  ThreadMemoryResourceOverride(ThreadMemoryResourceOverride &&) = delete;
  auto operator=(ThreadMemoryResourceOverride &&)
      -> ThreadMemoryResourceOverride & = delete;

  ~ThreadMemoryResourceOverride() noexcept {
    if (std::this_thread::get_id() != owner_) {
      contract_violation("ThreadMemoryResourceOverride must be destroyed on "
                         "its creating thread");
    }
    if (detail::thread_memory_resource_override != resource_) {
      contract_violation("ThreadMemoryResourceOverride guards must be "
                         "destroyed in LIFO order");
    }
    detail::thread_memory_resource_override = previous_;
  }

private:
  pmr::memory_resource *resource_;
  pmr::memory_resource *previous_;
  std::thread::id owner_;
};

} // namespace dagforge
