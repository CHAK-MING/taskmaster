#include "dagforge/workflow/plan_diagnostic.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace dagforge;
using namespace dagforge::workflow;

TEST(PlanDiagnosticTest, SerializesWithoutExecutionFailureDependency) {
  auto details = JsonPayload::from(glz::obj{"language_code", "S0207"});
  ASSERT_TRUE(details.has_value()) << details.error().message();

  const auto diagnostic = make_plan_diagnostic(
      Error::InvalidArgument, "transform_expression_invalid",
      "Transform expression is invalid", "/nodes/0/config/expression",
      WorkflowNodeId{"transform"}, std::string{"transform"},
      std::move(*details));
  auto encoded = serialize_json(diagnostic);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().message();

  auto parsed = parse_json(*encoded);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  const auto &object = parsed->get_object();
  EXPECT_EQ(object.at("kind").as<std::string>(), "invalid_argument");
  EXPECT_EQ(object.at("code").as<std::string>(),
            "transform_expression_invalid");
  EXPECT_EQ(object.at("message").as<std::string>(),
            "Transform expression is invalid");
  EXPECT_EQ(object.at("path").as<std::string>(), "/nodes/0/config/expression");
  EXPECT_EQ(object.at("node_id").as<std::string>(), "transform");
  EXPECT_EQ(object.at("executor").as<std::string>(), "transform");
  EXPECT_EQ(
      object.at("details").get_object().at("language_code").as<std::string>(),
      "S0207");
}

TEST(PlanDiagnosticTest, NeverNormalizesSuccessAsFailure) {
  EXPECT_EQ(normalize_plan_error(make_error_code(Error::Success)),
            Error::Unknown);
}
