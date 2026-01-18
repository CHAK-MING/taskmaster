#include <gtest/gtest.h>

#include "taskmaster/config/task_config.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/xcom/xcom_extractor.hpp"
#include "taskmaster/xcom/xcom_types.hpp"

namespace taskmaster {
namespace {

class XComExtractorTest : public ::testing::Test {
 protected:
  XComExtractor extractor_;
};

TEST_F(XComExtractorTest, ExtractStdoutAsString) {
  ExecutorResult result{.exit_code = 0, .stdout_output = "hello world"};
  std::vector<XComPushConfig> configs{{.key = "output", .source = XComSource::Stdout}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  EXPECT_EQ((*extracted)[0].key, "output");
  EXPECT_EQ((*extracted)[0].value, "hello world");
}

TEST_F(XComExtractorTest, ExtractStderrAsString) {
  ExecutorResult result{.exit_code = 1, .stderr_output = "error message"};
  std::vector<XComPushConfig> configs{{.key = "error", .source = XComSource::Stderr}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  EXPECT_EQ((*extracted)[0].value, "error message");
}

TEST_F(XComExtractorTest, ExtractExitCode) {
  ExecutorResult result{.exit_code = 42};
  std::vector<XComPushConfig> configs{{.key = "code", .source = XComSource::ExitCode}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value, 42);
}

TEST_F(XComExtractorTest, ExtractJsonFromStdout) {
  ExecutorResult result{.exit_code = 0, .stdout_output = R"({"name": "test", "value": 123})"};
  std::vector<XComPushConfig> configs{{.key = "data", .source = XComSource::Json}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value["name"], "test");
  EXPECT_EQ((*extracted)[0].value["value"], 123);
}

TEST_F(XComExtractorTest, ExtractWithJsonPath) {
  ExecutorResult result{.exit_code = 0, .stdout_output = R"({"result": {"status": "ok"}})"};
  std::vector<XComPushConfig> configs{{
      .key = "status",
      .source = XComSource::Json,
      .json_path = "result.status"
  }};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value, "ok");
}

TEST_F(XComExtractorTest, ExtractWithRegex) {
  ExecutorResult result{.exit_code = 0, .stdout_output = "Result: 42 items processed"};
  std::vector<XComPushConfig> configs{{
      .key = "count",
      .source = XComSource::Stdout,
      .regex_pattern = R"(Result: (\d+))",
      .regex_group = 1
  }};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value, "42");
}

TEST_F(XComExtractorTest, ExtractMultipleXComs) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output = "success",
      .stderr_output = "warnings"
  };
  std::vector<XComPushConfig> configs{
      {.key = "out", .source = XComSource::Stdout},
      {.key = "err", .source = XComSource::Stderr},
      {.key = "code", .source = XComSource::ExitCode}
  };

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 3);
  EXPECT_EQ((*extracted)[0].value, "success");
  EXPECT_EQ((*extracted)[1].value, "warnings");
  EXPECT_EQ((*extracted)[2].value, 0);
}

TEST_F(XComExtractorTest, InvalidJsonReturnsError) {
  ExecutorResult result{.exit_code = 0, .stdout_output = "not json"};
  std::vector<XComPushConfig> configs{{.key = "data", .source = XComSource::Json}};

  auto extracted = extractor_.extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, RegexNoMatchReturnsError) {
  ExecutorResult result{.exit_code = 0, .stdout_output = "no numbers here"};
  std::vector<XComPushConfig> configs{{
      .key = "num",
      .source = XComSource::Stdout,
      .regex_pattern = R"(\d+)"
  }};

  auto extracted = extractor_.extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPathNotFoundReturnsError) {
  ExecutorResult result{.exit_code = 0, .stdout_output = R"({"a": 1})"};
  std::vector<XComPushConfig> configs{{
      .key = "missing",
      .source = XComSource::Json,
      .json_path = "b.c.d"
  }};

  auto extracted = extractor_.extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPathWithArrayIndex) {
  ExecutorResult result{.exit_code = 0, .stdout_output = R"({"items": [1, 2, 3]})"};
  std::vector<XComPushConfig> configs{{
      .key = "second",
      .source = XComSource::Json,
      .json_path = "items[1]"
  }};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value, 2);
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_LastLine) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output = "Data size: 42\n[\"large_path\"]"
  };
  std::vector<XComPushConfig> configs{{.key = "branch", .source = XComSource::Json}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value.size(), 1);
  EXPECT_EQ((*extracted)[0].value[0], "large_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_FirstLine) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output = "[\"small_path\"]\nProcessing complete"
  };
  std::vector<XComPushConfig> configs{{.key = "branch", .source = XComSource::Json}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value[0], "small_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_MultipleLogLines) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output = "Starting process\nSize: 100\nStatus: OK\n{\"result\": \"success\"}"
  };
  std::vector<XComPushConfig> configs{{.key = "data", .source = XComSource::Json}};

  auto extracted = extractor_.extract(result, configs);
  ASSERT_TRUE(extracted);
  EXPECT_EQ((*extracted)[0].value["result"], "success");
}

}  // namespace
}  // namespace taskmaster
