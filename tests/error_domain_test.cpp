#include "dagforge/core/error.hpp"
#include "dagforge/http/http_client.hpp"
#include "dagforge/io/result.hpp"

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <system_error>
#include <utility>

using namespace dagforge;

TEST(ErrorDomainTest, ProvidesStableCoreCodesAndMessages) {
  EXPECT_STREQ(error_category().name(), "dagforge");

  for (std::size_t index = 0; index < kErrorDomain.size(); ++index) {
    SCOPED_TRACE(index);
    const auto value = static_cast<Error>(index);
    EXPECT_EQ(make_error_code(value).message(), kErrorDomain[index].message);
    EXPECT_EQ(to_string_view(value), kErrorDomain[index].code);
    EXPECT_EQ(kErrorNames[index], kErrorDomain[index].code);
  }

  EXPECT_EQ(to_string_view(static_cast<Error>(255)), "unknown");
}

TEST(ErrorDomainTest, SafelyHandlesUnknownCoreValues) {
  const std::error_code negative{std::numeric_limits<int>::min(),
                                 error_category()};
  const std::error_code out_of_range{std::numeric_limits<int>::max(),
                                     error_category()};

  EXPECT_EQ(negative.message(), "unknown error");
  EXPECT_EQ(out_of_range.message(), "unknown error");
}

TEST(ErrorDomainTest, SafelyHandlesUnknownIoValues) {
  using namespace dagforge::io;

  EXPECT_STREQ(io_error_category().name(), "dagforge.io");
  for (std::size_t index = 0; index < kIoErrorDomain.size(); ++index) {
    SCOPED_TRACE(index);
    const auto value = static_cast<IoError>(index);
    EXPECT_EQ(make_error_code(value).message(), kIoErrorDomain[index].message);
  }

  constexpr std::array condition_mappings{
      std::pair{IoError::Cancelled, std::errc::operation_canceled},
      std::pair{IoError::TimedOut, std::errc::timed_out},
      std::pair{IoError::WouldBlock, std::errc::operation_would_block},
      std::pair{IoError::InvalidArgument, std::errc::invalid_argument},
      std::pair{IoError::BadDescriptor, std::errc::bad_file_descriptor},
  };
  for (const auto &[error, condition] : condition_mappings) {
    EXPECT_EQ(make_error_code(error), condition);
  }

  const std::error_code negative{std::numeric_limits<int>::min(),
                                 io_error_category()};
  const std::error_code out_of_range{std::numeric_limits<int>::max(),
                                     io_error_category()};
  EXPECT_EQ(negative.message(), "unknown error");
  EXPECT_EQ(out_of_range.message(), "unknown error");
}

TEST(ErrorDomainTest, PreservesHttpTimeoutConditionsAndFallback) {
  using namespace dagforge::http;

  EXPECT_STREQ(http_client_error_category().name(), "dagforge.http.client");
  for (std::size_t index = 0; index < kHttpClientErrorDomain.size(); ++index) {
    SCOPED_TRACE(index);
    const auto value = static_cast<HttpClientError>(index);
    EXPECT_EQ(make_error_code(value).message(),
              kHttpClientErrorDomain[index].message);
  }

  constexpr std::array timeout_errors{
      HttpClientError::DnsTimeout,          HttpClientError::ConnectTimeout,
      HttpClientError::TlsHandshakeTimeout, HttpClientError::WriteTimeout,
      HttpClientError::FirstByteTimeout,    HttpClientError::ReadTimeout,
  };
  for (const auto error : timeout_errors) {
    EXPECT_EQ(make_error_code(error), std::errc::timed_out);
  }
  EXPECT_NE(make_error_code(HttpClientError::ReadFailure),
            std::errc::timed_out);

  const std::error_code negative{std::numeric_limits<int>::min(),
                                 http_client_error_category()};
  const std::error_code out_of_range{std::numeric_limits<int>::max(),
                                     http_client_error_category()};
  EXPECT_EQ(negative.message(), "unknown HTTP client error");
  EXPECT_EQ(out_of_range.message(), "unknown HTTP client error");
}
