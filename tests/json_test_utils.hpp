#pragma once

#include "dagforge/util/json.hpp"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>

namespace dagforge::test {

template <typename T>
[[nodiscard]] inline auto make_payload(const T &value) -> JsonPayload {
  auto encoded = JsonPayload::from(value);
  EXPECT_TRUE(encoded.has_value()) << encoded.error().message();
  return encoded ? std::move(*encoded) : JsonPayload{};
}

[[nodiscard]] inline auto parse_payload(std::string_view text) -> JsonPayload {
  auto encoded = JsonPayload::from_serialized(std::string{text});
  EXPECT_TRUE(encoded.has_value()) << encoded.error().message();
  return encoded ? std::move(*encoded) : JsonPayload{};
}

[[nodiscard]] inline auto materialize(const JsonPayload &payload) -> JsonValue {
  auto parsed = payload.materialize();
  EXPECT_TRUE(parsed.has_value()) << parsed.error().message();
  return parsed ? std::move(*parsed) : JsonValue::object_t{};
}

} // namespace dagforge::test
