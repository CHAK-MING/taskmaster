#pragma once

#include <concepts>
#include <expected>
#include <functional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

namespace taskmaster {

enum class Error : int {
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
  Unknown,
};

class ErrorCategory : public std::error_category {
  static constexpr std::string_view messages[] = {
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
      "unknown error",
  };

public:
  [[nodiscard]] auto name() const noexcept -> const char* override {
    return "taskmaster";
  }

  [[nodiscard]] auto message(int ev) const -> std::string override {
    auto idx = static_cast<std::size_t>(ev);
    if (idx >= std::size(messages)) {
      return "unknown error";
    }
    return std::string{messages[idx]};
  }
};

inline auto error_category() -> const ErrorCategory& {
  static const ErrorCategory instance;
  return instance;
}

inline auto make_error_code(Error e) -> std::error_code {
  return {std::to_underlying(e), error_category()};
}

// Concept for types that can be used with Result<T>
// Note: Result<T> alias itself is unconstrained to allow use with incomplete types
// (e.g., Result<CronExpr> inside CronExpr class definition)
template <typename T>
concept ResultValue = std::destructible<T> || std::is_void_v<T>;

template <typename T>
using Result = std::expected<T, std::error_code>;

template <typename T>
  requires ResultValue<std::decay_t<T>>
[[nodiscard]] constexpr auto ok(T&& value) -> Result<std::decay_t<T>> {
  return std::forward<T>(value);
}

[[nodiscard]] constexpr auto ok() -> Result<void> {
  return {};
}

[[nodiscard]] inline auto fail(Error e) -> std::unexpected<std::error_code> {
  return std::unexpected{make_error_code(e)};
}

[[nodiscard]] inline auto fail(std::error_code ec)
    -> std::unexpected<std::error_code> {
  return std::unexpected{ec};
}

}  // namespace taskmaster

template <>
struct std::is_error_code_enum<taskmaster::Error> : std::true_type {};

namespace taskmaster {

struct StringHash {
  using is_transparent = void;

  [[nodiscard]] std::size_t operator()(std::string_view sv) const noexcept {
    return std::hash<std::string_view>{}(sv);
  }

  [[nodiscard]] std::size_t operator()(const std::string& s) const noexcept {
    return std::hash<std::string_view>{}(s);
  }

  [[nodiscard]] std::size_t operator()(const char* s) const noexcept {
    return std::hash<std::string_view>{}(s);
  }
};

using StringEqual = std::equal_to<>;

}  // namespace taskmaster
