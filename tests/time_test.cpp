#include "dagforge/util/time.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <string_view>

namespace dagforge::test {

using namespace std::chrono_literals;

TEST(TimeTest, FormatsAndParsesCanonicalUtc) {
  const auto value = std::chrono::sys_days{std::chrono::year{2026} / 7 / 17} +
                     3h + 4min + 5s + 987ms;
  EXPECT_EQ(util::format_rfc3339_utc(value), "2026-07-17T03:04:05Z");

  const auto parsed = util::parse_rfc3339_utc("2026-07-17T03:04:05Z");
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(*parsed, std::chrono::floor<std::chrono::seconds>(value));
}

TEST(TimeTest, ReportsCanonicalUtcParseFailures) {
  const auto incomplete = util::parse_rfc3339_utc("2026-07-17T03:04");
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().kind, util::ParseErrorKind::IncompleteInput);

  const auto invalid = util::parse_rfc3339_utc("2026/07/17T03:04:05Z");
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().kind, util::ParseErrorKind::InvalidSyntax);
  EXPECT_EQ(invalid.error().offset, 4U);

  const auto trailing = util::parse_rfc3339_utc("2026-07-17T03:04:05Zextra");
  ASSERT_FALSE(trailing.has_value());
  EXPECT_EQ(trailing.error().kind, util::ParseErrorKind::TrailingCharacters);
  EXPECT_EQ(trailing.error().offset, 20U);
}

TEST(TimeTest, FormatsNamedZonesDeterministically) {
  const auto value =
      std::chrono::sys_days{std::chrono::year{2026} / 7 / 17} + 3h + 4min + 5s;
  const auto *tokyo = std::chrono::locate_zone("Asia/Tokyo");
  EXPECT_EQ(util::format_in_zone(value, *tokyo), "2026-07-17 12:04:05");
  EXPECT_EQ(util::format_in_zone(value, *tokyo, "{:%F %R}"),
            "2026-07-17 12:04");
}

TEST(TimeTest, PreservesCompatibilitySentinelsAndConversions) {
  const auto zero = std::chrono::system_clock::time_point{};
  EXPECT_TRUE(util::format_iso8601(zero).empty());
  EXPECT_EQ(util::format_local_timestamp(zero), "-");
  EXPECT_EQ(util::format_local_timestamp_short(zero), "-");
  EXPECT_TRUE(util::format_iso8601(0).empty());
  EXPECT_TRUE(util::format_iso8601(-1).empty());

  const auto value = util::from_unix_millis(1'721'357'445'987LL);
  EXPECT_EQ(util::to_unix_millis(value), 1'721'357'445'987LL);
  const auto utc = util::to_utc(value);
  EXPECT_EQ(utc.tm_year + 1900, 2024);
  EXPECT_EQ(utc.tm_mon + 1, 7);
}

} // namespace dagforge::test
