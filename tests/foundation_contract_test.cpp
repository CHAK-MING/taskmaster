#include "dagforge/core/contract.hpp"
#include "dagforge/core/scope_exit.hpp"

#include <gtest/gtest.h>

#include <source_location>
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
