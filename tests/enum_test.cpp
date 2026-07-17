#include "dagforge/util/enum.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/workflow/workflow_plan.hpp"
#include "dagforge/workflow/workflow_runtime_types.hpp"

#include <gtest/gtest.h>

namespace dagforge::test {

using workflow::FailurePolicy;
using workflow::RunState;

TEST(EnumTest, UsesProjectOwnedEntriesForNamesAndCodes) {
  EXPECT_EQ(util::enum_to_string_view(FailurePolicy::FailFast), "fail_fast");
  EXPECT_EQ(util::enum_to_string_view(static_cast<FailurePolicy>(255)),
            "unknown");
  EXPECT_EQ(util::enum_to_code(RunState::Paused), 2);

  const auto code = util::try_parse_enum_code<RunState>(2);
  ASSERT_TRUE(code.has_value());
  EXPECT_EQ(*code, RunState::Paused);
  EXPECT_FALSE(util::try_parse_enum_code<RunState>(255).has_value());
}

TEST(EnumTest, MakesTokenMatchingPolicyExplicit) {
  EXPECT_EQ(util::try_parse_enum<FailurePolicy>("fail_fast"),
            FailurePolicy::FailFast);
  EXPECT_FALSE(util::try_parse_enum<FailurePolicy>("FAIL_FAST").has_value());
  EXPECT_EQ(util::try_parse_enum<FailurePolicy>(
                "FAIL_FAST", util::EnumParsePolicy::CaseInsensitive),
            FailurePolicy::FailFast);
  EXPECT_EQ(util::try_parse_enum<FailurePolicy>("Fail Fast",
                                                util::EnumParsePolicy::Relaxed),
            FailurePolicy::FailFast);
  EXPECT_FALSE(util::try_parse_enum<FailurePolicy>(
                   "fail_faster", util::EnumParsePolicy::Relaxed)
                   .has_value());
}

TEST(EnumTest, KeepsGlazeWireNamesDerivedFromTheSameTraits) {
  auto encoded = serialize_json(workflow::TaskState::RetryWaiting);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();
  EXPECT_EQ(*encoded, R"("retry_waiting")");

  auto decoded = parse_json_as<workflow::TaskState>(*encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_EQ(*decoded, workflow::TaskState::RetryWaiting);
}

} // namespace dagforge::test
