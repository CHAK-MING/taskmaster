#include "dagforge/core/error.hpp"

#include <gtest/gtest.h>

namespace dagforge {

TEST(ErrorHelpersTest, OkProducesValue) {
  auto result = ok(42);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
}

TEST(ErrorHelpersTest, FailProducesExpectedErrorCode) {
  auto result = Result<void>{fail(Error::InvalidArgument)};
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::InvalidArgument));
}

TEST(ErrorHelpersTest, ErrorCategoryMessagesStayDense) {
  EXPECT_EQ(make_error_code(Error::Unauthorized).message(), "unauthorized");
  EXPECT_EQ(make_error_code(Error::RateLimited).message(), "rate limited");
  EXPECT_EQ(make_error_code(Error::Unknown).message(), "unknown error");
}

} // namespace dagforge
