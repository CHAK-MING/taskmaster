#include "taskmaster/scheduler/cron.hpp"

#include <chrono>
#include <ctime>

#include "gtest/gtest.h"

using namespace taskmaster;

class CronTest : public ::testing::Test {
protected:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(CronTest, ParseEveryMinute) {
  auto result = CronExpr::parse("* * * * *");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "* * * * *");
}

TEST_F(CronTest, ParseHourly) {
  auto result = CronExpr::parse("@hourly");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@hourly");
}

TEST_F(CronTest, ParseDaily) {
  auto result = CronExpr::parse("@daily");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@daily");
}

TEST_F(CronTest, ParseWeekly) {
  auto result = CronExpr::parse("@weekly");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@weekly");
}

TEST_F(CronTest, ParseMonthly) {
  auto result = CronExpr::parse("@monthly");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@monthly");
}

TEST_F(CronTest, ParseYearly) {
  auto result = CronExpr::parse("@yearly");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@yearly");
}

TEST_F(CronTest, ParseAnnually) {
  auto result = CronExpr::parse("@annually");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value().raw(), "@annually");
}

TEST_F(CronTest, ParseSpecificMinute) {
  auto result = CronExpr::parse("5 * * * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseSpecificHour) {
  auto result = CronExpr::parse("0 2 * * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseSpecificDayOfMonth) {
  auto result = CronExpr::parse("0 0 15 * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseSpecificMonth) {
  auto result = CronExpr::parse("0 0 1 1 *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseSpecificDayOfWeek) {
  auto result = CronExpr::parse("0 0 * * 1");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseRange) {
  auto result = CronExpr::parse("0 9-17 * * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseList) {
  auto result = CronExpr::parse("0,15,30,45 * * * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseStep) {
  auto result = CronExpr::parse("*/5 * * * *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseMonthName) {
  auto result = CronExpr::parse("0 0 1 Jan *");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseDayOfWeekName) {
  auto result = CronExpr::parse("0 0 * * Mon");
  ASSERT_TRUE(result.has_value());
}

TEST_F(CronTest, ParseInvalidEmpty) {
  auto result = CronExpr::parse("");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::InvalidArgument));
}

TEST_F(CronTest, ParseInvalidTooFewParts) {
  auto result = CronExpr::parse("* * * *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidTooManyParts) {
  auto result = CronExpr::parse("* * * * * *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidMinute) {
  auto result = CronExpr::parse("60 * * * *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidHour) {
  auto result = CronExpr::parse("0 24 * * *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidDayOfMonth) {
  auto result = CronExpr::parse("0 0 32 * *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidMonth) {
  auto result = CronExpr::parse("0 0 1 13 *");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidDayOfWeek) {
  auto result = CronExpr::parse("0 0 * * 8");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, ParseInvalidMacro) {
  auto result = CronExpr::parse("@invalid");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().value(), static_cast<int>(Error::ParseError));
}

TEST_F(CronTest, NextAfterEveryMinute) {
  auto cron = CronExpr::parse("* * * * *");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);
  EXPECT_LT(next - now, std::chrono::minutes(2));
}

TEST_F(CronTest, NextAfterHourly) {
  auto cron = CronExpr::parse("@hourly");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);
  EXPECT_LT(next - now, std::chrono::hours(2));
}

TEST_F(CronTest, NextAfterDaily) {
  auto cron = CronExpr::parse("@daily");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);
  EXPECT_LT(next - now, std::chrono::hours(25));
}

TEST_F(CronTest, NextAfterSpecificMinute) {
  auto cron = CronExpr::parse("5 * * * *");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);

  auto tm = std::tm{};
  auto t = std::chrono::system_clock::to_time_t(next);
  gmtime_r(&t, &tm);

  EXPECT_EQ(tm.tm_min, 5);
}

TEST_F(CronTest, NextAfterSpecificHour) {
  auto cron = CronExpr::parse("0 2 * * *");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);
  EXPECT_LT(next - now, std::chrono::hours(25));

  auto tm = std::tm{};
  auto t = std::chrono::system_clock::to_time_t(next);
  gmtime_r(&t, &tm);

  EXPECT_EQ(tm.tm_hour, 2);
  EXPECT_EQ(tm.tm_min, 0);
}

TEST_F(CronTest, NextAfterStep) {
  auto cron = CronExpr::parse("*/5 * * * *");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);
  EXPECT_LT(next - now, std::chrono::minutes(6));

  auto tm = std::tm{};
  auto t = std::chrono::system_clock::to_time_t(next);
  gmtime_r(&t, &tm);

  EXPECT_EQ(tm.tm_min % 5, 0);
}

TEST_F(CronTest, NextAfterMonday) {
  auto cron = CronExpr::parse("0 0 * * Mon");
  ASSERT_TRUE(cron.has_value());

  auto now = std::chrono::system_clock::now();
  auto next = cron.value().next_after(now);

  EXPECT_GT(next, now);

  auto tm = std::tm{};
  auto t = std::chrono::system_clock::to_time_t(next);
  gmtime_r(&t, &tm);

  EXPECT_EQ(tm.tm_wday, 1);
  EXPECT_EQ(tm.tm_hour, 0);
  EXPECT_EQ(tm.tm_min, 0);
}

TEST_F(CronTest, AllBetween_HourlyForOneDay) {
  auto cron = CronExpr::parse("@hourly");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704067200);
  auto end = std::chrono::system_clock::from_time_t(1704153600);

  auto times = cron->all_between(start, end);

  EXPECT_EQ(times.size(), 24);
  for (size_t i = 1; i < times.size(); ++i) {
    EXPECT_EQ(times[i] - times[i-1], std::chrono::hours(1));
  }
}

TEST_F(CronTest, AllBetween_EveryFiveMinutesForOneHour) {
  auto cron = CronExpr::parse("*/5 * * * *");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704067200);
  auto end = std::chrono::system_clock::from_time_t(1704070800);

  auto times = cron->all_between(start, end);

  EXPECT_EQ(times.size(), 12);
  for (size_t i = 1; i < times.size(); ++i) {
    EXPECT_EQ(times[i] - times[i-1], std::chrono::minutes(5));
  }
}

TEST_F(CronTest, AllBetween_DailyForOneWeek) {
  auto cron = CronExpr::parse("@daily");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704067200);
  auto end = std::chrono::system_clock::from_time_t(1704672000);

  auto times = cron->all_between(start, end);

  EXPECT_EQ(times.size(), 7);
  for (size_t i = 1; i < times.size(); ++i) {
    EXPECT_EQ(times[i] - times[i-1], std::chrono::hours(24));
  }
}

TEST_F(CronTest, AllBetween_EmptyWhenNoMatchesInRange) {
  auto cron = CronExpr::parse("0 0 15 * *");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704067200);
  auto end = std::chrono::system_clock::from_time_t(1704153600);

  auto times = cron->all_between(start, end);

  EXPECT_TRUE(times.empty());
}

TEST_F(CronTest, AllBetween_RespectsMaxCount) {
  auto cron = CronExpr::parse("* * * * *");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704067200);
  auto end = std::chrono::system_clock::from_time_t(1704153600);

  auto times = cron->all_between(start, end, 10);

  EXPECT_EQ(times.size(), 10);
}

TEST_F(CronTest, AllBetween_ExcludesEndBoundary) {
  auto cron = CronExpr::parse("@hourly");
  ASSERT_TRUE(cron.has_value());

  auto start = std::chrono::system_clock::from_time_t(1704063600);
  auto end = std::chrono::system_clock::from_time_t(1704067200);

  auto times = cron->all_between(start, end);

  EXPECT_FALSE(times.empty());
  for (const auto& t : times) {
    EXPECT_LT(t, end);
  }
}