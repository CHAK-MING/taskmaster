#include "taskmaster/xcom/template_resolver.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <tuple>

using namespace taskmaster;
using namespace std::chrono;

struct DateTemplateTestCase {
  std::string name;
  std::string input;
  std::string expected;
};

auto operator<<(std::ostream& os, const DateTemplateTestCase& tc)
    -> std::ostream& {
  os << tc.name;
  return os;
}

class MockPersistence : public Persistence {
 public:
  MockPersistence() : Persistence(":memory:") {}
};

class DateTemplateTest : public ::testing::TestWithParam<DateTemplateTestCase> {
 protected:
  void SetUp() override {
    persistence_ = std::make_unique<MockPersistence>();
    resolver_ = std::make_unique<TemplateResolver>(*persistence_);
  }

  std::unique_ptr<MockPersistence> persistence_;
  std::unique_ptr<TemplateResolver> resolver_;
};

TEST_P(DateTemplateTest, ResolvesDateVariables) {
  const auto& tc = GetParam();

  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  tm.tm_hour = 10;
  tm.tm_min = 30;
  tm.tm_sec = 45;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template(tc.input, ctx, {});
  ASSERT_TRUE(result.has_value()) << "Failed for: " << tc.name;
  EXPECT_EQ(*result, tc.expected) << "Mismatch for: " << tc.name;
}

INSTANTIATE_TEST_SUITE_P(
    DateVariables, DateTemplateTest,
    ::testing::Values(
        DateTemplateTestCase{
            "ds_basic",
            "date={{ds}}",
            "date=2026-01-15"},
        DateTemplateTestCase{
            "ds_nodash_basic",
            "date={{ds_nodash}}",
            "date=20260115"},
        DateTemplateTestCase{
            "ts_basic",
            "time={{ts}}",
            "time=2026-01-15T10:30:45"},
        DateTemplateTestCase{
            "ts_nodash_basic",
            "time={{ts_nodash}}",
            "time=20260115T103045"},
        DateTemplateTestCase{
            "multiple_ds",
            "from={{ds}}_to={{ds}}",
            "from=2026-01-15_to=2026-01-15"},
        DateTemplateTestCase{
            "mixed_variables",
            "{{ds}}/{{ds_nodash}}/{{ts}}",
            "2026-01-15/20260115/2026-01-15T10:30:45"},
        DateTemplateTestCase{
            "no_variables",
            "plain text",
            "plain text"},
        DateTemplateTestCase{
            "partial_match_not_replaced",
            "{{ds_invalid}}",
            "{{ds_invalid}}"},
        DateTemplateTestCase{
            "embedded_in_path",
            "/data/{{ds_nodash}}/file.csv",
            "/data/20260115/file.csv"},
        DateTemplateTestCase{
            "command_with_ds",
            "echo 'Processing {{ds}}' && run --date={{ds_nodash}}",
            "echo 'Processing 2026-01-15' && run --date=20260115"}),
    [](const auto& info) { return info.param.name; });

class DateTemplateEdgeCaseTest : public ::testing::Test {
 protected:
  void SetUp() override {
    persistence_ = std::make_unique<MockPersistence>();
    resolver_ = std::make_unique<TemplateResolver>(*persistence_);
  }

  std::unique_ptr<MockPersistence> persistence_;
  std::unique_ptr<TemplateResolver> resolver_;
};

TEST_F(DateTemplateEdgeCaseTest, EmptyExecutionDate_NoReplacement) {
  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = {},
  };

  auto result = resolver_->resolve_template("date={{ds}}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "date={{ds}}");
}

TEST_F(DateTemplateEdgeCaseTest, EmptyTemplate_ReturnsEmpty) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 5;
  tm.tm_mday = 1;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "");
}

TEST_F(DateTemplateEdgeCaseTest, LeapYear_February29) {
  std::tm tm{};
  tm.tm_year = 2024 - 1900;
  tm.tm_mon = 1;
  tm.tm_mday = 29;
  tm.tm_hour = 12;
  tm.tm_min = 0;
  tm.tm_sec = 0;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("{{ds}}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "2024-02-29");
}

TEST_F(DateTemplateEdgeCaseTest, YearBoundary_December31) {
  std::tm tm{};
  tm.tm_year = 2025 - 1900;
  tm.tm_mon = 11;
  tm.tm_mday = 31;
  tm.tm_hour = 23;
  tm.tm_min = 59;
  tm.tm_sec = 59;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("{{ds}} {{ts}}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "2025-12-31 2025-12-31T23:59:59");
}

TEST_F(DateTemplateEdgeCaseTest, Midnight_ZeroTime) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 1;
  tm.tm_hour = 0;
  tm.tm_min = 0;
  tm.tm_sec = 0;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("{{ts_nodash}}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "20260101T000000");
}

// =============================================================================
// Escape Sequence Tests
// =============================================================================

TEST_F(DateTemplateEdgeCaseTest, EscapedBraces_ProducesLiteral) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  // \{{ should produce literal {{
  auto result = resolver_->resolve_template(R"(\{{ds}})", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "{{ds}}");
}

TEST_F(DateTemplateEdgeCaseTest, MixedEscapedAndNormal) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  // Mix of escaped and actual template variables
  auto result = resolver_->resolve_template(R"(date={{ds}} literal=\{{ds}})", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "date=2026-01-15 literal={{ds}}");
}

TEST_F(DateTemplateEdgeCaseTest, UnclosedBraces_LeavesAsLiteral) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("start={{no_close", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "start={{no_close");
}

TEST_F(DateTemplateEdgeCaseTest, WhitespaceInToken_Trimmed) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("{{  ds  }}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "2026-01-15");
}

TEST_F(DateTemplateEdgeCaseTest, DoubleBraceRun_UnknownTokenStaysLiteral) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  auto result = resolver_->resolve_template("{{{{ds}}", ctx, {});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, "{{{{ds}}");
}

TEST_F(DateTemplateEdgeCaseTest, ManyUnclosedBraces_LinearTime) {
  std::tm tm{};
  tm.tm_year = 2026 - 1900;
  tm.tm_mon = 0;
  tm.tm_mday = 15;
  auto time_point = system_clock::from_time_t(timegm(&tm));

  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = time_point,
  };

  std::string many_unclosed;
  for (int i = 0; i < 50; ++i) {
    many_unclosed += "{{";
  }

  auto result = resolver_->resolve_template(many_unclosed, ctx, {});
  ASSERT_TRUE(result.has_value());

  std::string expected;
  for (int i = 0; i < 50; ++i) {
    expected += "{{";
  }
  EXPECT_EQ(*result, expected);
}

TEST_F(DateTemplateEdgeCaseTest, MalformedXComToken_StaysLiteral) {
  TemplateContext ctx{
      .dag_run_id = DAGRunId("test_run"),
      .execution_date = {},
  };

  auto result1 = resolver_->resolve_template("{{xcom.}}", ctx, {});
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, "{{xcom.}}");

  auto result2 = resolver_->resolve_template("{{xcom.task}}", ctx, {});
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, "{{xcom.task}}");

  auto result3 = resolver_->resolve_template("{{xcom..key}}", ctx, {});
  ASSERT_TRUE(result3.has_value());
  EXPECT_EQ(*result3, "{{xcom..key}}");
}
