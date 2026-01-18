#pragma once

#include <concepts>
#include <format>
#include <functional>
#include <ostream>
#include <string>
#include <string_view>
#include <random>

namespace taskmaster {

// Phantom type tags for type-safe ID disambiguation
struct DAGTag {};
struct TaskTag {};
struct DAGTaskTag {};
struct DAGRunTag {};
struct InstanceTag {};

// Type-safe ID wrapper using phantom type pattern
// Prevents accidental mixing of different ID types at compile time
template <typename Tag>
class TypedId {
public:
  explicit TypedId(std::string value) : value_(std::move(value)) {}

  TypedId() = default;

  [[nodiscard]] auto value() const -> std::string_view { return value_; }
  [[nodiscard]] auto str() const -> const std::string& { return value_; }
  [[nodiscard]] auto c_str() const -> const char* { return value_.c_str(); }

  // Explicit conversion prevents accidental ID type mixing
  [[nodiscard]] explicit operator std::string() const { return value_; }
  [[nodiscard]] explicit operator std::string_view() const { return value_; }

  [[nodiscard]] auto empty() const -> bool { return value_.empty(); }

  [[nodiscard]] friend auto operator<=>(const TypedId& lhs, const TypedId& rhs) = default;
  [[nodiscard]] friend auto operator==(const TypedId& lhs, const TypedId& rhs) -> bool = default;

  [[nodiscard]] auto size() const -> size_t { return value_.size(); }

  [[nodiscard]] auto clone() const -> TypedId {
    return TypedId{value_};
  }

private:
  std::string value_;
};

using DAGId = TypedId<DAGTag>;
using TaskId = TypedId<TaskTag>;
using DAGTaskId = TypedId<DAGTaskTag>;
using DAGRunId = TypedId<DAGRunTag>;
using InstanceId = TypedId<InstanceTag>;


namespace detail {
inline auto generate_short_uuid() -> std::string {
  thread_local std::random_device rd;
  thread_local std::mt19937_64 gen(rd());
  thread_local std::uniform_int_distribution<std::uint32_t> dis;
  return std::format("{:08x}", dis(gen));
}
}  // namespace detail

inline auto generate_dag_task_id(const DAGId& dag_id, const TaskId& task_id) ->DAGTaskId {
  return DAGTaskId{std::format("{}_{}", dag_id.value(), task_id.value())};
}

inline auto generate_dag_run_id(const DAGId& dag_id) -> DAGRunId {
  return DAGRunId{std::format("{}_{}", dag_id.value(), detail::generate_short_uuid())};
}

inline auto generate_instance_id(const DAGRunId& dag_run_id,
                                  const TaskId& task_id) -> InstanceId {
  return InstanceId{std::format("{}_{}", dag_run_id.value(), task_id.value())};
}

// Extract DAGId from DAGRunId (format: "dag_id_uuid")
inline auto extract_dag_id(const DAGRunId& dag_run_id) -> DAGId {
  auto sv = dag_run_id.value();
  auto pos = sv.rfind('_');
  if (pos != std::string_view::npos) {
    return DAGId{std::string{sv.substr(0, pos)}};
  }
  return DAGId{std::string{sv}};
}

template <typename T>
concept IsTypedId = requires(T id) {
  { id.value() } -> std::convertible_to<std::string_view>;
  { id.empty() } -> std::convertible_to<bool>;
};

template <typename Tag>
inline auto operator<<(std::ostream& os, const TypedId<Tag>& id) -> std::ostream& {
  return os << id.value();
}

}

// Enable std::unordered_map<TypedId<Tag>, V> usage
template <typename Tag>
struct std::hash<taskmaster::TypedId<Tag>> {
  auto operator()(const taskmaster::TypedId<Tag>& id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

// Enable std::print/std::format support (C++23)
template <typename Tag>
struct std::formatter<taskmaster::TypedId<Tag>> : std::formatter<std::string> {
  auto format(const taskmaster::TypedId<Tag>& id, auto& ctx) const {
    return std::formatter<std::string>::format(std::string(id.value()), ctx);
  }
};
