#include <gtest/gtest.h>

#include "dagforge/config/task_config.hpp"
#include "dagforge/executor/executor.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/xcom/xcom_codec.hpp"
#include "dagforge/xcom/xcom_extractor.hpp"
#include "dagforge/xcom/xcom_types.hpp"
#include "dagforge/xcom/xcom_util.hpp"

namespace dagforge {
namespace {

auto expect_json_string(std::string_view json_text, std::string_view expected)
    -> void {
  auto parsed = parse_json(json_text);
  ASSERT_TRUE(parsed);
  const auto &value = *parsed;
  ASSERT_TRUE(value.is_string());
  EXPECT_EQ(value.as<std::string>(), expected);
}

auto expect_json_int(std::string_view json_text, int64_t expected) -> void {
  auto parsed = parse_json(json_text);
  ASSERT_TRUE(parsed);
  const auto &value = *parsed;
  ASSERT_TRUE(value.is_number());
  EXPECT_EQ(value.as<int64_t>(), expected);
}

auto expect_json_bool(std::string_view json_text, bool expected) -> void {
  auto parsed = parse_json(json_text);
  ASSERT_TRUE(parsed);
  const auto &value = *parsed;
  ASSERT_TRUE(value.is_boolean());
  EXPECT_EQ(value.get<bool>(), expected);
}

class XComExtractorTest : public ::testing::Test {};

TEST(XComCodecTest, PushConfigRoundTripPreservesFields) {
  XComPushConfig push;
  push.key = "result";
  push.source = XComSource::Json;
  push.json_pointer = "/items/1";
  push.regex_pattern = R"((\d+))";
  push.regex_group = 1;
  ASSERT_TRUE(push.prepare().has_value());

  const auto encoded = xcom::serialize_push_configs({push});
  auto decoded = xcom::parse_push_configs(encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  ASSERT_EQ(decoded->size(), 1U);
  EXPECT_EQ((*decoded)[0].key, "result");
  EXPECT_EQ((*decoded)[0].source, XComSource::Json);
  EXPECT_EQ((*decoded)[0].json_pointer, "/items/1");
  EXPECT_EQ((*decoded)[0].regex_pattern, R"((\d+))");
  EXPECT_EQ((*decoded)[0].regex_group, 1);
  ASSERT_TRUE((*decoded)[0].compiled_regex);
}

TEST(XComCodecTest, ParsePushConfigsRejectsMalformedJson) {
  auto decoded = xcom::parse_push_configs("{not-json");
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error(), make_error_code(Error::ParseError));
}

TEST(XComCodecTest, ParsePushConfigsRejectsInvalidRegex) {
  auto decoded = xcom::parse_push_configs(
      R"([{"key":"result","source":"stdout","json_pointer":"","regex":"(","regex_group":0}])");
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error(), make_error_code(Error::InvalidArgument));
}

TEST(XComCodecTest, RejectsRemovedJsonPathField) {
  auto decoded = xcom::parse_push_configs(
      R"([{"key":"result","source":"json","json_path":"items[0]","regex":"","regex_group":0}])");
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error(), make_error_code(Error::ParseError));
}

TEST(XComCodecTest, RejectsInvalidJsonPointerDuringPreparation) {
  XComPushConfig push{.key = "result",
                      .source = XComSource::Json,
                      .json_pointer = "/items/~2bad"};
  auto prepared = push.prepare();
  ASSERT_FALSE(prepared.has_value());
  EXPECT_EQ(prepared.error(), make_error_code(Error::InvalidArgument));
}

TEST(XComCodecTest, PullConfigRoundTripPreservesDefaultValue) {
  XComPullConfig pull;
  pull.ref.task_id = TaskId{"producer"};
  pull.ref.key = "payload";
  pull.env_var = "PAYLOAD";
  pull.required = true;
  pull.default_value_json = R"("fallback")";
  pull.default_value_rendered = "fallback";
  pull.has_default_value = true;

  const auto encoded = xcom::serialize_pull_configs({pull});
  auto decoded = xcom::parse_pull_configs(encoded);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  ASSERT_EQ(decoded->size(), 1U);
  EXPECT_EQ((*decoded)[0].ref.task_id, TaskId{"producer"});
  EXPECT_EQ((*decoded)[0].ref.key, "payload");
  EXPECT_EQ((*decoded)[0].env_var, "PAYLOAD");
  EXPECT_TRUE((*decoded)[0].required);
  EXPECT_TRUE((*decoded)[0].has_default_value);
  EXPECT_EQ((*decoded)[0].default_value_json, R"("fallback")");
  EXPECT_EQ((*decoded)[0].default_value_rendered, "fallback");
}

TEST(XComCodecTest, ParsePullConfigsRejectsMalformedJson) {
  auto decoded = xcom::parse_pull_configs("[");
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error(), make_error_code(Error::ParseError));
}

TEST(XComUtilTest, RenderSerializedJsonUnwrapsStringValues) {
  auto rendered = xcom::render_serialized_json(R"("hello")");
  ASSERT_TRUE(rendered.has_value()) << rendered.error().message();
  EXPECT_EQ(*rendered, "hello");
}

TEST(XComUtilTest, RenderSerializedJsonPreservesStructuredJson) {
  auto rendered = xcom::render_serialized_json(R"({"ok":true})");
  ASSERT_TRUE(rendered.has_value()) << rendered.error().message();
  EXPECT_EQ(*rendered, R"({"ok":true})");
}

TEST(XComUtilTest, RenderSerializedJsonRejectsInvalidJson) {
  auto rendered = xcom::render_serialized_json("not-json");
  ASSERT_FALSE(rendered.has_value());
  EXPECT_EQ(rendered.error(), make_error_code(Error::ParseError));
}

TEST(XComCacheTest, SetAndGetByCompositeKey) {
  XComCache cache;
  cache.set(DAGRunId("run_1"), TaskId("task_a"), "result",
            R"({"ok":true})");

  auto got = cache.get(DAGRunId("run_1"), TaskId("task_a"), "result");
  ASSERT_TRUE(got);
  auto parsed = parse_json(got->get());
  ASSERT_TRUE(parsed);
  expect_json_bool(dump_json((*parsed)["ok"]), true);
}

TEST(XComCacheTest, ClearRunRemovesOnlyTargetRunEntries) {
  XComCache cache;
  cache.set(DAGRunId("run_1"), TaskId("task_a"), "k1", "v1");
  cache.set(DAGRunId("run_1"), TaskId("task_b"), "k2", "v2");
  cache.set(DAGRunId("run_2"), TaskId("task_a"), "k1", "v3");

  cache.clear_run(DAGRunId("run_1"));

  EXPECT_FALSE(cache.get(DAGRunId("run_1"), TaskId("task_a"), "k1"));
  EXPECT_FALSE(cache.get(DAGRunId("run_1"), TaskId("task_b"), "k2"));

  auto retained = cache.get(DAGRunId("run_2"), TaskId("task_a"), "k1");
  ASSERT_TRUE(retained);
  EXPECT_EQ(retained->get(), "v3");
}

TEST_F(XComExtractorTest, ExtractStdoutAsString) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "hello world",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "output",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  EXPECT_EQ((*extracted)[0].key, "output");
  expect_json_string((*extracted)[0].value, "hello world");
}

TEST_F(XComExtractorTest, ExtractStderrAsString) {
  ExecutorResult result{.exit_code = 1,
                        .stdout_output = "",
                        .stderr_output = "error message",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "error",
                                       .source = XComSource::Stderr,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1);
  expect_json_string((*extracted)[0].value, "error message");
}

TEST_F(XComExtractorTest, ExtractExitCode) {
  ExecutorResult result{.exit_code = 42,
                        .stdout_output = "",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "code",
                                       .source = XComSource::ExitCode,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_int((*extracted)[0].value, 42);
}

TEST_F(XComExtractorTest, ExtractJsonFromStdout) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"name": "test", "value": 123})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  auto parsed = parse_json((*extracted)[0].value);
  ASSERT_TRUE(parsed);
  expect_json_string(dump_json((*parsed)["name"]), "test");
  expect_json_int(dump_json((*parsed)["value"]), 123);
}

TEST_F(XComExtractorTest, ExtractWithJsonPointer) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"result": {"status": "ok"}})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "status",
                                       .source = XComSource::Json,
                                       .json_pointer = "/result/status",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value, "ok");
}

TEST_F(XComExtractorTest, JsonPointerSupportsRfc6901Escapes) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output = R"({"a/b":{"m~n":"escaped"}})",
      .stderr_output = "",
      .error = "",
      .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "value",
                                       .source = XComSource::Json,
                                       .json_pointer = "/a~1b/m~0n",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value, "escaped");
}

TEST_F(XComExtractorTest, ExtractWithRegex) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Result: 42 items processed",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "count",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = R"(Result: (\d+))",
                                       .regex_group = 1}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_string((*extracted)[0].value, "42");
}

TEST_F(XComExtractorTest, ExtractMultipleXComs) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "success",
                        .stderr_output = "warnings",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "out",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = ""},
                                      {.key = "err",
                                       .source = XComSource::Stderr,
                                       .json_pointer = "",
                                       .regex_pattern = ""},
                                      {.key = "code",
                                       .source = XComSource::ExitCode,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 3);
  expect_json_string((*extracted)[0].value, "success");
  expect_json_string((*extracted)[1].value, "warnings");
  expect_json_int((*extracted)[2].value, 0);
}

TEST_F(XComExtractorTest, InvalidJsonReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "not json",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, RegexNoMatchReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "no numbers here",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "num",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = R"(\d+)"}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPointerNotFoundReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"a": 1})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "missing",
                                       .source = XComSource::Json,
                                       .json_pointer = "/b/c/d",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  EXPECT_FALSE(extracted);
}

TEST_F(XComExtractorTest, JsonPointerWithArrayIndex) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"items": [1, 2, 3]})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "second",
                                       .source = XComSource::Json,
                                       .json_pointer = "/items/1",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  expect_json_int((*extracted)[0].value, 2);
}

TEST_F(XComExtractorTest, JsonPointerArrayIndexOutOfBoundsReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"items": [1, 2, 3]})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "missing",
                                       .source = XComSource::Json,
                                       .json_pointer = "/items/999",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_FALSE(extracted);
  EXPECT_EQ(extracted.error(), make_error_code(Error::NotFound));
}

TEST_F(XComExtractorTest, RegexGroupOutOfRangeReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Result: 42 items processed",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "count",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = R"(Result: (\d+))",
                                       .regex_group = 2}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_FALSE(extracted);
  EXPECT_EQ(extracted.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(XComExtractorTest, UncompiledInvalidRegexReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Result: 42 items processed",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "count",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = "(",
                                       .regex_group = 0}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_FALSE(extracted);
  EXPECT_EQ(extracted.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(XComExtractorTest, EmptyStdoutWithoutRegexProducesEmptyString) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "\n\n",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "output",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  ASSERT_EQ(extracted->size(), 1U);
  expect_json_string((*extracted)[0].value, "");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_LastLine) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "Data size: 42\n[\"large_path\"]",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "branch",
                                       .source = XComSource::Json,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  auto parsed = parse_json((*extracted)[0].value);
  ASSERT_TRUE(parsed);
  EXPECT_EQ(parsed->size(), 1);
  expect_json_string(dump_json((*parsed)[0]), "large_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_PrecedingLogs) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output =
                            "Processing started...\n[\"small_path\"]",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "branch",
                                       .source = XComSource::Json,
                                       .json_pointer = "",
                                       .regex_pattern = "",
                                       .regex_group = 0}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  auto parsed = parse_json((*extracted)[0].value);
  ASSERT_TRUE(parsed);
  expect_json_string(dump_json((*parsed)[0]), "small_path");
}

TEST_F(XComExtractorTest, ExtractJsonFromMixedOutput_MultipleLogLines) {
  ExecutorResult result{
      .exit_code = 0,
      .stdout_output =
          "Starting process\nSize: 100\nStatus: OK\n{\"result\": \"success\"}",
      .stderr_output = "",
      .error = "",
      .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "data",
                                       .source = XComSource::Json,
                                       .json_pointer = "",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_TRUE(extracted);
  auto parsed = parse_json((*extracted)[0].value);
  ASSERT_TRUE(parsed);
  expect_json_string(dump_json((*parsed)["result"]), "success");
}

TEST_F(XComExtractorTest, InvalidJsonPointerSyntaxReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = R"({"items": [1, 2, 3]})",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "broken",
                                       .source = XComSource::Json,
                                       .json_pointer = "items/1",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_FALSE(extracted);
  EXPECT_EQ(extracted.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(XComExtractorTest, JsonPointerOnInvalidJsonTextReturnsError) {
  ExecutorResult result{.exit_code = 0,
                        .stdout_output = "plain text",
                        .stderr_output = "",
                        .error = "",
                        .timed_out = false};
  std::vector<XComPushConfig> configs{{.key = "broken",
                                       .source = XComSource::Stdout,
                                       .json_pointer = "/items/0",
                                       .regex_pattern = ""}};

  auto extracted = xcom::extract(result, configs);
  ASSERT_FALSE(extracted);
  EXPECT_EQ(extracted.error(), make_error_code(Error::InvalidArgument));
}

} // namespace
} // namespace dagforge
