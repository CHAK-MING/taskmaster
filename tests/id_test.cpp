#include "dagforge/util/id.hpp"
#include "dagforge/util/json.hpp"
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

  auto valid = TestId::parse("validated");
  ASSERT_TRUE(valid.has_value());
  EXPECT_TRUE(valid->valid());
  EXPECT_FALSE(TestId::parse("bad\tvalue").has_value());

  const std::string at_limit(TestId::rules().max_bytes, 'x');
  const std::string over_limit(TestId::rules().max_bytes + 1, 'x');
  EXPECT_TRUE(TestId::parse(at_limit).has_value());
  EXPECT_FALSE(TestId::parse(over_limit).has_value());
  EXPECT_EQ(TestId::from_trusted("trusted").value(), "trusted");
}

TEST(IdTest, JsonRejectsInvalidTypedIdsAtTheSerdeSeam) {
  auto valid = parse_json_as<TestId>(R"("valid")");
  ASSERT_TRUE(valid.has_value());
  EXPECT_EQ(valid->value(), "valid");

  EXPECT_FALSE(parse_json_as<TestId>(R"("")").has_value());
  EXPECT_FALSE(parse_json_as<TestId>(R"("bad\nvalue")").has_value());

  const std::string over_limit(TestId::rules().max_bytes + 1, 'x');
  EXPECT_FALSE(
      parse_json_as<TestId>(std::format("\"{}\"", over_limit)).has_value());
  EXPECT_FALSE(serialize_json(TestId{}).has_value());
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
