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
  PersistenceError,
  Unknown,
};

class ErrorCategory : public std::error_category {
  static constexpr std::array<std::string_view, 29> messages = {
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
      "persistence error",
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

[[nodiscard]] constexpr auto to_string_view(Error error) noexcept
    -> std::string_view {
  switch (error) {
  case Error::Success:
    return "success";
  case Error::FileNotFound:
    return "file_not_found";
  case Error::FileOpenFailed:
    return "file_open_failed";
  case Error::ParseError:
    return "parse_error";
  case Error::DatabaseError:
    return "database_error";
  case Error::DatabaseOpenFailed:
    return "database_open_failed";
  case Error::DatabaseQueryFailed:
    return "database_query_failed";
  case Error::InvalidArgument:
    return "invalid_argument";
  case Error::NotFound:
    return "not_found";
  case Error::AlreadyExists:
    return "already_exists";
  case Error::Timeout:
    return "timeout";
  case Error::Cancelled:
    return "cancelled";
  case Error::CycleDetected:
    return "cycle_detected";
  case Error::ReadOnly:
    return "read_only";
  case Error::HasDependents:
    return "has_dependents";
  case Error::HasActiveRuns:
    return "has_active_runs";
  case Error::SystemNotRunning:
    return "system_not_running";
  case Error::QueueFull:
    return "queue_full";
  case Error::InvalidUrl:
    return "invalid_url";
  case Error::ProcessForkFailed:
    return "process_fork_failed";
  case Error::ResourceExhausted:
    return "resource_exhausted";
  case Error::InvalidState:
    return "invalid_state";
  case Error::Incomplete:
    return "incomplete";
  case Error::ProtocolError:
    return "protocol_error";
  case Error::Unauthorized:
    return "unauthorized";
  case Error::RateLimited:
    return "rate_limited";
  case Error::Unsupported:
    return "unsupported";
  case Error::PersistenceError:
    return "persistence_error";
  case Error::Unknown:
    return "unknown";
  }
  return "unknown";
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
