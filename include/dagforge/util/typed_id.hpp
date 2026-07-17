#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#endif

namespace dagforge {

enum class IdTextPolicy : std::uint8_t {
  NonEmptyNoControl,
  AllowEmptyNoControl,
};

[[nodiscard]] constexpr auto has_control_chars(std::string_view value) noexcept
    -> bool {
  for (const unsigned char character : value) {
    if (character < 0x20U || character == 0x7FU) {
      return true;
    }
  }
  return false;
}

[[nodiscard]] constexpr auto
is_valid_id_text(std::string_view value,
                 IdTextPolicy policy = IdTextPolicy::NonEmptyNoControl) noexcept
    -> bool {
  return (policy == IdTextPolicy::AllowEmptyNoControl || !value.empty()) &&
         !has_control_chars(value);
}

template <typename Tag> class TypedId {
public:
  explicit TypedId(std::string value) : value_(std::move(value)) {}
  explicit TypedId(std::string_view value) : value_(value) {}
  explicit TypedId(const char *value) : value_(value ? value : "") {}

  TypedId() = default;

  [[nodiscard]] static auto
  from_validated(std::string value,
                 IdTextPolicy policy = IdTextPolicy::NonEmptyNoControl)
      -> std::optional<TypedId> {
    if (!is_valid_id_text(value, policy)) {
      return std::nullopt;
    }
    return TypedId{std::move(value)};
  }

  [[nodiscard]] auto value() const noexcept -> std::string_view {
    return value_;
  }
  [[nodiscard]] auto str() const noexcept -> const std::string & {
    return value_;
  }
  [[nodiscard]] auto c_str() const noexcept -> const char * {
    return value_.c_str();
  }

  [[nodiscard]] friend auto operator<=>(const TypedId &lhs,
                                        const TypedId &rhs) = default;
  [[nodiscard]] friend auto operator==(const TypedId &lhs, const TypedId &rhs)
      -> bool = default;

  [[nodiscard]] friend auto operator==(const TypedId &lhs,
                                       std::string_view rhs) noexcept -> bool {
    return lhs.value_ == rhs;
  }
  [[nodiscard]] friend auto operator==(std::string_view lhs,
                                       const TypedId &rhs) noexcept -> bool {
    return lhs == rhs.value_;
  }

  [[nodiscard]] friend auto operator<(const TypedId &lhs,
                                      std::string_view rhs) noexcept -> bool {
    return std::string_view{lhs.value_} < rhs;
  }
  [[nodiscard]] friend auto operator<(std::string_view lhs,
                                      const TypedId &rhs) noexcept -> bool {
    return lhs < std::string_view{rhs.value_};
  }

  [[nodiscard]] auto clone() const -> TypedId { return TypedId{value_}; }
  [[nodiscard]] auto empty() const noexcept -> bool { return value_.empty(); }
  [[nodiscard]] auto size() const noexcept -> std::size_t {
    return value_.size();
  }
  [[nodiscard]] auto
  valid(IdTextPolicy policy = IdTextPolicy::NonEmptyNoControl) const noexcept
      -> bool {
    return is_valid_id_text(value_, policy);
  }

private:
  std::string value_;
};

template <typename T>
concept IsTypedId = requires(T id) {
  { id.value() } -> std::convertible_to<std::string_view>;
  { id.empty() } -> std::convertible_to<bool>;
};

template <typename Tag>
inline auto operator<<(std::ostream &os, const TypedId<Tag> &id)
    -> std::ostream & {
  return os << id.value();
}

} // namespace dagforge

template <typename Tag> struct std::hash<dagforge::TypedId<Tag>> {
  using is_avalanching = void;

  auto operator()(const dagforge::TypedId<Tag> &id) const noexcept
      -> std::size_t {
    return std::hash<std::string_view>{}(id.value());
  }
};

template <typename Tag>
struct std::formatter<dagforge::TypedId<Tag>>
    : std::formatter<std::string_view> {
  auto format(const dagforge::TypedId<Tag> &id, auto &ctx) const {
    return std::formatter<std::string_view>::format(id.value(), ctx);
  }
};
