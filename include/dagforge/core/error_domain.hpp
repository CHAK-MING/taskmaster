#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#endif

namespace dagforge {

struct ErrorDomainEntry {
  std::string_view code;
  std::string_view message;
};

namespace detail {

template <std::integral Integer, std::size_t Size>
  requires(!std::same_as<std::remove_cv_t<Integer>, bool>)
[[nodiscard]] constexpr auto
lookup_error_domain_entry(Integer value,
                          const std::array<ErrorDomainEntry, Size> &entries,
                          ErrorDomainEntry fallback) noexcept
    -> ErrorDomainEntry {
  if constexpr (std::is_signed_v<Integer>) {
    if (value < 0) {
      return fallback;
    }
  }

  using Unsigned = std::make_unsigned_t<Integer>;
  const auto index = static_cast<std::uintmax_t>(static_cast<Unsigned>(value));
  if (index >= Size) {
    return fallback;
  }
  return entries[static_cast<std::size_t>(index)];
}

template <typename Enum, std::size_t Size>
  requires std::is_enum_v<Enum>
[[nodiscard]] constexpr auto
lookup_error_domain_entry(Enum value,
                          const std::array<ErrorDomainEntry, Size> &entries,
                          ErrorDomainEntry fallback) noexcept
    -> ErrorDomainEntry {
  return lookup_error_domain_entry(std::to_underlying(value), entries,
                                   fallback);
}

template <std::size_t Size>
[[nodiscard]] consteval auto
error_domain_codes(const std::array<ErrorDomainEntry, Size> &entries)
    -> std::array<std::string_view, Size> {
  std::array<std::string_view, Size> codes{};
  for (std::size_t index = 0; index < Size; ++index) {
    codes[index] = entries[index].code;
  }
  return codes;
}

} // namespace detail

template <typename Enum, std::size_t Size>
  requires std::is_enum_v<Enum>
class StaticErrorCategory : public std::error_category {
public:
  StaticErrorCategory(const char *domain_name,
                      std::array<ErrorDomainEntry, Size> entries,
                      ErrorDomainEntry fallback) noexcept
      : domain_name_(domain_name), entries_(entries), fallback_(fallback) {}

  [[nodiscard]] auto name() const noexcept -> const char * final {
    return domain_name_;
  }

  [[nodiscard]] auto message(int value) const -> std::string final {
    return std::string{
        detail::lookup_error_domain_entry(value, entries_, fallback_).message};
  }

private:
  const char *domain_name_;
  std::array<ErrorDomainEntry, Size> entries_;
  ErrorDomainEntry fallback_;
};

} // namespace dagforge
