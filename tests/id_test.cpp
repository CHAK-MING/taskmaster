#include "dagforge/util/id.hpp"
#include "dagforge/util/typed_id.hpp"

#include <gtest/gtest.h>

#include <string>

namespace dagforge::test {

namespace {
struct TestTag {};
using TestId = TypedId<TestTag>;
} // namespace

TEST(IdTest, MakesValidationPolicyExplicit) {
  EXPECT_TRUE(is_valid_id_text("valid-id"));
  EXPECT_FALSE(is_valid_id_text(""));
  EXPECT_FALSE(is_valid_id_text("bad\nvalue"));
  EXPECT_TRUE(is_valid_id_text("", IdTextPolicy::AllowEmptyNoControl));

  auto valid = TestId::from_validated("validated");
  ASSERT_TRUE(valid.has_value());
  EXPECT_TRUE(valid->valid());
  EXPECT_FALSE(TestId::from_validated("bad\tvalue").has_value());
}

TEST(IdTest, KeepsDomainIdsStronglyTypedAndStable) {
  const WorkflowId workflow{"workflow"};
  const auto run = generate_workflow_run_id(workflow);
  EXPECT_TRUE(run.value().starts_with("workflow__"));
  EXPECT_EQ(run.size(), std::string{"workflow__"}.size() + 36U);

  const auto plan = generate_workflow_plan_id();
  const auto trigger = generate_workflow_trigger_id();
  const auto artifact = generate_artifact_id();
  const auto evidence = generate_evidence_id();
  const auto attempt = generate_attempt_id();
  EXPECT_EQ(plan.size(), 36U);
  EXPECT_EQ(trigger.size(), 36U);
  EXPECT_EQ(artifact.size(), 36U);
  EXPECT_EQ(evidence.size(), 36U);
  EXPECT_EQ(attempt.size(), 36U);
}

} // namespace dagforge::test
