#pragma once

#include "dagforge/core/error_domain.hpp"

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

inline constexpr ErrorDomainEntry kUnknownErrorEntry{"unknown",
                                                     "unknown error"};

inline constexpr std::array<ErrorDomainEntry, 29> kErrorDomain = {{
    {"success", "success"},
    {"file_not_found", "file not found"},
    {"file_open_failed", "failed to open file"},
    {"parse_error", "parse error"},
    {"database_error", "database error"},
    {"database_open_failed", "failed to open database"},
    {"database_query_failed", "database query failed"},
    {"invalid_argument", "invalid argument"},
    {"not_found", "not found"},
    {"already_exists", "already exists"},
    {"timeout", "timeout"},
    {"cancelled", "cancelled"},
    {"cycle_detected", "cycle detected in DAG"},
    {"read_only", "resource is read-only"},
    {"has_dependents", "resource has dependents"},
    {"has_active_runs", "DAG has active runs"},
    {"system_not_running", "system not running"},
    {"queue_full", "queue full"},
    {"invalid_url", "invalid URL"},
    {"process_fork_failed", "failed to fork process"},
    {"resource_exhausted", "resource exhausted"},
    {"invalid_state", "invalid state transition"},
    {"incomplete", "incomplete data"},
    {"protocol_error", "protocol error"},
    {"unauthorized", "unauthorized"},
    {"rate_limited", "rate limited"},
    {"unsupported", "unsupported operation"},
    {"persistence_error", "persistence error"},
    {"unknown", "unknown error"},
}};

// Compatibility view for enum metadata. kErrorDomain remains the single
// source of truth for both symbolic codes and human-readable messages.
inline constexpr auto kErrorNames = detail::error_domain_codes(kErrorDomain);

static_assert(std::to_underlying(Error::Success) == 0,
              "dagforge::Error domain requires a zero-based enum.");
static_assert(kErrorDomain.size() == std::to_underlying(Error::Unknown) + 1,
              "Update kErrorDomain when adding dagforge::Error values.");

class ErrorCategory final
    : public StaticErrorCategory<Error, kErrorDomain.size()> {
public:
  ErrorCategory() noexcept
      : StaticErrorCategory("dagforge", kErrorDomain, kUnknownErrorEntry) {}
};

[[nodiscard]] inline auto error_category() noexcept -> const ErrorCategory & {
  static const ErrorCategory instance;
  return instance;
}

[[nodiscard]] inline auto make_error_code(Error e) noexcept -> std::error_code {
  return {std::to_underlying(e), error_category()};
}

[[nodiscard]] constexpr auto to_string_view(Error error) noexcept
    -> std::string_view {
  return detail::lookup_error_domain_entry(error, kErrorDomain,
                                           kUnknownErrorEntry)
      .code;
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

[[nodiscard]] inline auto fail(std::error_code ec)
    -> std::unexpected<std::error_code> {
  return std::unexpected{ec};
}

template <typename T> [[nodiscard]] auto sys_check(T val) -> Result<T> {
  if (val < 0) {
    return fail(std::error_code(errno, std::system_category()));
  }
  return ok(val);
}

} // namespace dagforge

template <> struct std::is_error_code_enum<dagforge::Error> : std::true_type {};
