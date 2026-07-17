#include "dagforge/core/error.hpp"
#include "dagforge/util/conv.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/parse.hpp"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <string_view>

namespace dagforge::test {

namespace {

struct ParseFixture {
  int value{};
};

} // namespace

TEST(ParseTest, ParsesIntegersWithoutLosingFailureClass) {
  auto parsed = util::parse_integer<int>("2a", 16);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(*parsed, 42);

  const auto trailing = util::parse_integer<int>("12x");
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().kind, util::ParseErrorKind::TrailingCharacters);
  EXPECT_EQ(trailing.error().offset, 2);
  EXPECT_EQ(trailing.error().line, 1);
  EXPECT_EQ(trailing.error().column, 3);

  const auto out_of_range = util::parse_integer<int>(std::to_string(
      static_cast<long long>(std::numeric_limits<int>::max()) + 1));
  ASSERT_FALSE(out_of_range.has_value());
  EXPECT_EQ(out_of_range.error().kind, util::ParseErrorKind::OutOfRange);

  const auto invalid_base = util::parse_integer<int>("10", 1);
  ASSERT_FALSE(invalid_base.has_value());
  EXPECT_EQ(invalid_base.error().kind, util::ParseErrorKind::InvalidBase);
}

TEST(ParseTest, LegacyIntegerWrapperProjectsToStableErrorCode) {
  const auto parsed = util::parse_int<int>("12x");
  ASSERT_FALSE(parsed.has_value());
  EXPECT_EQ(parsed.error(), make_error_code(Error::ParseError));
}

TEST(ParseTest, ReportsJsonSyntaxLocationWithoutLeakingGlazeTypes) {
  constexpr std::string_view kIncomplete = "{\n  \"value\": 1";
  const auto incomplete = parse_json_detailed(kIncomplete);
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().kind, util::ParseErrorKind::IncompleteInput);
  EXPECT_EQ(incomplete.error().offset, kIncomplete.size());
  EXPECT_EQ(incomplete.error().line, 2);

  const auto invalid = parse_json_detailed("{\n  ]");
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().kind, util::ParseErrorKind::InvalidSyntax);
  EXPECT_EQ(invalid.error().line, 2);
}

TEST(ParseTest, SeparatesJsonSyntaxFromSchemaMismatch) {
  const auto mismatch =
      parse_json_as_detailed<ParseFixture>(R"({"value":"not-an-int"})");
  ASSERT_FALSE(mismatch.has_value());
  EXPECT_EQ(mismatch.error().kind, util::ParseErrorKind::SchemaMismatch);
  EXPECT_GE(mismatch.error().column, 1);

  const auto legacy = parse_json_as<ParseFixture>(R"({"value":"not-an-int"})");
  ASSERT_FALSE(legacy.has_value());
  EXPECT_EQ(legacy.error(), make_error_code(Error::ParseError));
}

TEST(ParseTest, JsonPayloadCachesValidatedRootShape) {
  auto object = JsonPayload::from_serialized_detailed("  {\"value\":1}  ");
  ASSERT_TRUE(object.has_value());
  EXPECT_TRUE(object->valid());
  EXPECT_TRUE(object->is_object());
  EXPECT_FALSE(object->is_null());

  auto null = JsonPayload::from_serialized_detailed("\nnull\t");
  ASSERT_TRUE(null.has_value());
  EXPECT_TRUE(null->is_null());
  EXPECT_FALSE(null->is_object());

  const auto invalid = JsonPayload::from_serialized_detailed("{");
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().kind, util::ParseErrorKind::IncompleteInput);
}

} // namespace dagforge::test
