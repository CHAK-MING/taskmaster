#include "dagforge/core/contract.hpp"
#include "dagforge/core/memory.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/scope_exit.hpp"

#include <gtest/gtest.h>

#include <memory_resource>
#include <source_location>
#include <thread>
#include <utility>

using namespace dagforge;

TEST(FoundationContractTest, ScopeExitRunsExactlyOnce) {
  int calls = 0;
  {
    auto guard = scope_exit([&calls] { ++calls; });
    auto moved = std::move(guard);
    (void)moved;
  }
  EXPECT_EQ(calls, 1);
}

TEST(FoundationContractTest, ReportsOriginBeforeTerminating) {
  EXPECT_DEATH(
      contract_violation("broken invariant", std::source_location::current()),
      "DAGForge contract violation.*broken invariant");
}

TEST(FoundationContractTest, ThreadMemoryOverridesNestAndRestore) {
  std::pmr::monotonic_buffer_resource outer;
  std::pmr::monotonic_buffer_resource inner;
  auto *const default_resource = std::pmr::get_default_resource();

  EXPECT_EQ(current_memory_resource_or_default(), default_resource);
  {
    ThreadMemoryResourceOverride outer_override{&outer};
    EXPECT_EQ(current_memory_resource_or_default(), &outer);
    {
      ThreadMemoryResourceOverride inner_override{&inner};
      EXPECT_EQ(current_memory_resource_or_default(), &inner);
    }
    EXPECT_EQ(current_memory_resource_or_default(), &outer);
  }
  EXPECT_EQ(current_memory_resource_or_default(), default_resource);
}

TEST(FoundationContractTest, ThreadMemoryOverrideRejectsNullResource) {
  EXPECT_DEATH(ThreadMemoryResourceOverride{nullptr},
               "requires a non-null resource");
}

TEST(FoundationContractTest,
     ThreadMemoryOverrideRejectsCrossThreadDestruction) {
  std::pmr::monotonic_buffer_resource resource;
  EXPECT_DEATH(
      {
        auto *guard = new ThreadMemoryResourceOverride{&resource};
        std::thread destroyer{[guard] { delete guard; }};
        destroyer.join();
      },
      "must be destroyed on its creating thread");
}
