#pragma once

#include <expected>
#include <string>
#include <system_error>

namespace taskmaster {

enum class Error {
  Success = 0,
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
public:
  [[nodiscard]] auto name() const noexcept -> const char * override {
    return "taskmaster";
  }

  [[nodiscard]] auto message(int ev) const -> std::string override {
    switch (static_cast<Error>(ev)) {
    case Error::Success:
      return "success";
    case Error::FileNotFound:
      return "file not found";
    case Error::FileOpenFailed:
      return "failed to open file";
    case Error::ParseError:
      return "parse error";
    case Error::DatabaseError:
      return "database error";
    case Error::DatabaseOpenFailed:
      return "failed to open database";
    case Error::DatabaseQueryFailed:
      return "database query failed";
    case Error::InvalidArgument:
      return "invalid argument";
    case Error::NotFound:
      return "not found";
    case Error::AlreadyExists:
      return "already exists";
    case Error::Timeout:
      return "timeout";
    case Error::Cancelled:
      return "cancelled";
    case Error::CycleDetected:
      return "cycle detected in DAG";
    case Error::ReadOnly:
      return "resource is read-only";
    case Error::HasDependents:
      return "resource has dependents";
    case Error::HasActiveRuns:
      return "DAG has active runs";
    case Error::Unknown:
    default:
      return "unknown error";
    }
  }
};

inline auto error_category() -> const ErrorCategory & {
  static ErrorCategory instance;
  return instance;
}

inline auto make_error_code(Error e) -> std::error_code {
  return {static_cast<int>(e), error_category()};
}

template <typename T = void> using Result = std::expected<T, std::error_code>;

inline constexpr auto ok() -> Result<void> { return {}; }

template <typename T> constexpr auto ok(T &&value) -> Result<std::decay_t<T>> {
  return std::forward<T>(value);
}

inline auto fail(Error e) -> std::unexpected<std::error_code> {
  return std::unexpected{make_error_code(e)};
}

inline auto fail(std::error_code ec) -> std::unexpected<std::error_code> {
  return std::unexpected{ec};
}

} // namespace taskmaster

template <>
struct std::is_error_code_enum<taskmaster::Error> : std::true_type {};

namespace taskmaster {

// Transparent hash for heterogeneous lookup with string_view
struct StringHash {
  using is_transparent = void;
  [[nodiscard]] auto operator()(std::string_view sv) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(sv);
  }
};

} // namespace taskmaster
