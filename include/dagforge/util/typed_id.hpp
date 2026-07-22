#pragma once

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

namespace dagforge {

enum class IdTextPolicy : std::uint8_t {
  NonEmptyNoControl,
  AllowEmptyNoControl,
};

inline constexpr std::size_t kDefaultIdTextMaxBytes = 256;

struct IdTextRules {
  IdTextPolicy policy{IdTextPolicy::NonEmptyNoControl};
  std::size_t max_bytes{kDefaultIdTextMaxBytes};
};

template <typename Tag> struct TypedIdTraits {
  static constexpr IdTextRules rules{};
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

[[nodiscard]] constexpr auto is_valid_id_text(std::string_view value,
                                              IdTextRules rules = {}) noexcept
    -> bool {
  return rules.max_bytes > 0 && value.size() <= rules.max_bytes &&
         (rules.policy == IdTextPolicy::AllowEmptyNoControl ||
          !value.empty()) &&
         !has_control_chars(value);
}

[[nodiscard]] constexpr auto is_valid_id_text(std::string_view value,
                                              IdTextPolicy policy) noexcept
    -> bool {
  return is_valid_id_text(value, IdTextRules{.policy = policy});
}

template <typename Tag> class TypedId {
public:
  // Direct construction is the compatibility path for trusted internal text.
  // External or serialized text must enter through parse().
  explicit TypedId(std::string value) : value_(std::move(value)) {}
  explicit TypedId(std::string_view value) : value_(value) {}
  explicit TypedId(const char *value) : value_(value ? value : "") {}

  TypedId() = default;

  [[nodiscard]] static constexpr auto rules() noexcept -> IdTextRules {
    return TypedIdTraits<Tag>::rules;
  }

  [[nodiscard]] static auto parse(std::string value) -> std::optional<TypedId> {
    if (!is_valid_id_text(value, rules())) {
      return std::nullopt;
    }
    return from_trusted(std::move(value));
  }

  [[nodiscard]] static auto parse(std::string_view value)
      -> std::optional<TypedId> {
    return parse(std::string{value});
  }

  [[nodiscard]] static auto parse(const char *value) -> std::optional<TypedId> {
    return parse(std::string_view{value ? value : ""});
  }

  [[nodiscard]] static auto from_trusted(std::string value) -> TypedId {
    return TypedId{std::move(value)};
  }

  [[nodiscard]] static auto from_trusted(std::string_view value) -> TypedId {
    return TypedId{value};
  }

  [[nodiscard]] static auto from_trusted(const char *value) -> TypedId {
    return TypedId{value};
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
  [[nodiscard]] auto valid() const noexcept -> bool {
    return is_valid_id_text(value_, rules());
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
