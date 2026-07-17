#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error_domain.hpp"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#endif

namespace dagforge::io {

enum class IoError : std::uint8_t {
  Success = 0,
  Cancelled,
  TimedOut,
  EndOfFile,
  ConnectionReset,
  ConnectionRefused,
  BrokenPipe,
  WouldBlock,
  InvalidArgument,
  BadDescriptor,
  NoBufferSpace,
  OperationInProgress,
  NotConnected,
  AlreadyConnected,
  Unknown,
};

inline constexpr ErrorDomainEntry kUnknownIoErrorEntry{"unknown",
                                                       "unknown error"};

inline constexpr std::array<ErrorDomainEntry, 15> kIoErrorDomain = {{
    {"success", "success"},
    {"cancelled", "operation cancelled"},
    {"timed_out", "operation timed out"},
    {"end_of_file", "end of file"},
    {"connection_reset", "connection reset"},
    {"connection_refused", "connection refused"},
    {"broken_pipe", "broken pipe"},
    {"would_block", "operation would block"},
    {"invalid_argument", "invalid argument"},
    {"bad_descriptor", "bad file descriptor"},
    {"no_buffer_space", "no buffer space"},
    {"operation_in_progress", "operation in progress"},
    {"not_connected", "not connected"},
    {"already_connected", "already connected"},
    {"unknown", "unknown error"},
}};

static_assert(std::to_underlying(IoError::Success) == 0,
              "dagforge::io::IoError domain requires a zero-based enum.");
static_assert(kIoErrorDomain.size() == std::to_underlying(IoError::Unknown) + 1,
              "Update kIoErrorDomain when adding IoError values.");

class IoErrorCategory final
    : public StaticErrorCategory<IoError, kIoErrorDomain.size()> {
  using Base = StaticErrorCategory<IoError, kIoErrorDomain.size()>;

public:
  IoErrorCategory() noexcept
      : Base("dagforge.io", kIoErrorDomain, kUnknownIoErrorEntry) {}

  using Base::equivalent;

  [[nodiscard]] auto equivalent(int code,
                                const std::error_condition &cond) const noexcept
      -> bool override {
    if (cond.category() == std::generic_category()) {
      switch (static_cast<IoError>(code)) {
      case IoError::Cancelled:
        return cond.value() == static_cast<int>(std::errc::operation_canceled);
      case IoError::TimedOut:
        return cond.value() == static_cast<int>(std::errc::timed_out);
      case IoError::WouldBlock:
        return cond.value() ==
               static_cast<int>(std::errc::operation_would_block);
      case IoError::InvalidArgument:
        return cond.value() == static_cast<int>(std::errc::invalid_argument);
      case IoError::BadDescriptor:
        return cond.value() == static_cast<int>(std::errc::bad_file_descriptor);
      default:
        break;
      }
    }
    return false;
  }
};

[[nodiscard]] inline auto io_error_category() noexcept
    -> const IoErrorCategory & {
  static const IoErrorCategory instance;
  return instance;
}

[[nodiscard]] inline auto make_error_code(IoError e) noexcept
    -> std::error_code {
  return {std::to_underlying(e), io_error_category()};
}

[[nodiscard]] inline auto is_cancelled(const std::error_code &error) noexcept
    -> bool {
  return error == make_error_code(IoError::Cancelled) ||
         error == std::errc::operation_canceled;
}

} // namespace dagforge::io

template <>
struct std::is_error_code_enum<dagforge::io::IoError> : std::true_type {};
