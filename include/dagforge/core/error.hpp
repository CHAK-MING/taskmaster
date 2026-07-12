#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <array>
#include <cerrno>
#include <concepts>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#endif

namespace dagforge {

enum class Error : std::uint8_t {
  Success,
  FileNotFound,
  FileOpenFailed,
  ParseError,
  DatabaseError,
  DatabaseOpenFailed,
  DatabaseQueryFailed,
  InvalidArgument,
  NotFound,
  AlreadyExists,
  Timeout,
  Cancelled,
  CycleDetected,
  ReadOnly,
  HasDependents,
  HasActiveRuns,
  SystemNotRunning,
  QueueFull,
  InvalidUrl,
  ProcessForkFailed,
  ResourceExhausted,
  InvalidState,
  Incomplete,
  ProtocolError,
  Unauthorized,
  RateLimited,
  Unsupported,
  Unknown,
};

class ErrorCategory : public std::error_category {
  static constexpr std::array<std::string_view, 28> messages = {
      "success",
      "file not found",
      "failed to open file",
      "parse error",
      "database error",
      "failed to open database",
      "database query failed",
      "invalid argument",
      "not found",
      "already exists",
      "timeout",
      "cancelled",
      "cycle detected in DAG",
      "resource is read-only",
      "resource has dependents",
      "DAG has active runs",
      "system not running",
      "queue full",
      "invalid URL",
      "failed to fork process",
      "resource exhausted",
      "invalid state transition",
      "incomplete data",
      "protocol error",
      "unauthorized",
      "rate limited",
      "unsupported operation",
      "unknown error",
  };

  static_assert(std::to_underlying(Error::Success) == 0,
                "dagforge::Error must stay zero-based for table lookup.");
  static_assert(messages.size() == std::to_underlying(Error::Unknown) + 1,
                "Update ErrorCategory::messages when adding dagforge::Error values.");

public:
  ~ErrorCategory() override = default;

  [[nodiscard]] auto name() const noexcept -> const char * override {
    return "dagforge";
  }

  [[nodiscard]] auto message(int ev) const -> std::string override {
    auto idx = static_cast<std::size_t>(ev);
    if (idx >= std::size(messages)) {
      std::unreachable();
    }
    return std::string{messages.at(idx)};
  }
};

inline auto error_category() -> const ErrorCategory & {
  static const ErrorCategory instance;
  return instance;
}

inline auto make_error_code(Error e) -> std::error_code {
  return {std::to_underlying(e), error_category()};
}

template <typename T>
concept ResultValue = std::destructible<T> || std::is_void_v<T>;

template <typename T> using Result = std::expected<T, std::error_code>;

template <typename T>
  requires ResultValue<std::decay_t<T>>
[[nodiscard]] constexpr auto ok(T &&value) -> Result<std::decay_t<T>> {
  return std::forward<T>(value);
}

[[nodiscard]] constexpr auto ok() -> Result<void> { return {}; }

[[nodiscard]] inline auto fail(Error e) -> std::unexpected<std::error_code> {
  return std::unexpected{make_error_code(e)};
}

[[nodiscard]] inline auto fail(Error e, std::string_view /*message*/)
    -> std::unexpected<std::error_code> {
  return fail(e);
}

[[nodiscard]] inline auto fail(std::error_code ec)
    -> std::unexpected<std::error_code> {
  return std::unexpected{ec};
}

[[nodiscard]] inline auto fail(std::error_code ec, std::string_view /*message*/)
    -> std::unexpected<std::error_code> {
  return fail(ec);
}

template <typename T> [[nodiscard]] auto sys_check(T val) -> Result<T> {
  if (val < 0) {
    return fail(std::error_code(errno, std::system_category()));
  }
  return ok(val);
}

} // namespace dagforge

template <> struct std::is_error_code_enum<dagforge::Error> : std::true_type {};
