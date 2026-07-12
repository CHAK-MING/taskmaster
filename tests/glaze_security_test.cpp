#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/core/error.hpp"
#include "dagforge/util/json.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace dagforge::test {

struct JsonPayload {
  std::string name;
  std::vector<int> values;
};

namespace {

class ExactBuffer {
public:
  explicit ExactBuffer(std::string_view input)
      : data_(std::make_unique_for_overwrite<char[]>(input.size())),
        size_(input.size()) {
    std::memcpy(data_.get(), input.data(), input.size());
  }

  [[nodiscard]] auto view() const -> std::string_view {
    return {data_.get(), size_};
  }

private:
  std::unique_ptr<char[]> data_;
  std::size_t size_{};
};

[[nodiscard]] auto make_deep_json(std::size_t depth) -> std::string {
  std::string input(depth, '[');
  input.push_back('0');
  input.append(depth, ']');
  return input;
}

[[nodiscard]] auto make_deep_toml(std::size_t depth) -> std::string {
  std::string input{"ignored = "};
  input.append(depth, '[');
  input.push_back('0');
  input.append(depth, ']');
  input.push_back('\n');
  return input;
}

} // namespace

TEST(GlazeSecurityTest, ParsesExactJsonBufferWithoutNullTerminator) {
  ExactBuffer input{R"({"name":"safe","values":[1,2,3]})"};

  auto parsed = parse_json_as<JsonPayload>(input.view());

  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->name, "safe");
  EXPECT_EQ(parsed->values, (std::vector<int>{1, 2, 3}));
  EXPECT_TRUE(is_valid_json(input.view()));
}

TEST(GlazeSecurityTest, SerializesTypedJsonThroughProjectWrapper) {
  JsonPayload payload{.name = "safe", .values = {1, 2, 3}};

  auto serialized = serialize_json(payload);

  ASSERT_TRUE(serialized.has_value()) << serialized.error().message();
  auto parsed = parse_json_as<JsonPayload>(*serialized);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->name, payload.name);
  EXPECT_EQ(parsed->values, payload.values);
}

TEST(GlazeSecurityTest, RelaxedTypedParserAllowsUnknownKeys) {
  constexpr std::string_view kInput =
      R"({"name":"safe","values":[1],"future_field":true})";

  EXPECT_FALSE(parse_json_as<JsonPayload>(kInput).has_value());
  auto parsed = parse_json_as_allow_unknown<JsonPayload>(kInput);
  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->name, "safe");
  EXPECT_EQ(parsed->values, (std::vector<int>{1}));
}

TEST(GlazeSecurityTest, JsonViewDoesNotConsumePoisonSuffix) {
  constexpr std::string_view kJson =
      R"({"name":"bounded","values":[7]})";
  std::string storage{kJson};
  storage.push_back('X');
  const std::string_view bounded{storage.data(), kJson.size()};

  auto parsed = parse_json_as<JsonPayload>(bounded);

  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->name, "bounded");
  EXPECT_TRUE(is_valid_json(bounded));
}

TEST(GlazeSecurityTest, RejectsMaliciousJsonWithoutReadingPastInput) {
  std::vector<std::string> corpus = {
      "{\"name\":\"unterminated",
      "{\"name\":\"escape\\",
      "{\"values\":[1,2,3",
      std::string{"{\"name\":\""} + std::string(1, '\0') + "\"}",
  };

  for (std::size_t index = 0; index < corpus.size(); ++index) {
    const auto &sample = corpus[index];
    ExactBuffer input{sample};
    Result<JsonValue> parsed;
    EXPECT_NO_THROW(parsed = parse_json(input.view()));
    EXPECT_FALSE(parsed.has_value()) << "sample " << index << " unexpectedly accepted "
                                     << sample.size() << " bytes";
    EXPECT_FALSE(is_valid_json(input.view())) << "sample " << index;
  }
}

TEST(GlazeSecurityTest, AdversarialValidJsonNeverCrashes) {
  std::vector<std::string> corpus = {
      "{\"value\":1e999999999999999999999999}",
      make_deep_json(300),
  };

  for (const auto &sample : corpus) {
    ExactBuffer input{sample};
    EXPECT_NO_THROW((void)is_valid_json(input.view()));
    EXPECT_NO_THROW((void)parse_json(input.view()));
  }
}

TEST(GlazeSecurityTest, ParsesExactTomlBufferWithoutNullTerminator) {
  ExactBuffer input{"[api]\nenabled = true\nport = 9001\n"};

  auto parsed = SystemConfigLoader::load_from_string(input.view());

  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_TRUE(parsed->api.enabled);
  EXPECT_EQ(parsed->api.port, 9001);
}

TEST(GlazeSecurityTest, TomlViewDoesNotConsumePoisonSuffix) {
  constexpr std::string_view kToml = "[api]\nenabled = true\nport = 9002\n";
  std::string storage{kToml};
  storage.push_back('[');
  const std::string_view bounded{storage.data(), kToml.size()};

  auto parsed = SystemConfigLoader::load_from_string(bounded);

  ASSERT_TRUE(parsed.has_value()) << parsed.error().message();
  EXPECT_EQ(parsed->api.port, 9002);
}

TEST(GlazeSecurityTest, RejectsMaliciousTomlWithoutReadingPastInput) {
  std::vector<std::string> corpus = {
      "[api]\nhost = \"unterminated",
      "[api]\nhost = \"escape\\",
      "[api]\nport = [1, 2, 3",
      "[database\nhost = \"localhost\"\n",
      "[api]\nport = 999999999999999999999999999999999999\n",
      std::string{"[api]\nhost = \""} + std::string(1, '\0') + "\"\n",
  };

  for (std::size_t index = 0; index < corpus.size(); ++index) {
    const auto &sample = corpus[index];
    ExactBuffer input{sample};
    Result<SystemConfig> parsed;
    EXPECT_NO_THROW(parsed = SystemConfigLoader::load_from_string(input.view()));
    EXPECT_FALSE(parsed.has_value()) << "sample " << index << " unexpectedly accepted "
                                     << sample.size() << " bytes";
  }
}

TEST(GlazeSecurityTest, AdversarialValidTomlNeverCrashes) {
  ExactBuffer input{make_deep_toml(300)};

  EXPECT_NO_THROW((void)SystemConfigLoader::load_from_string(input.view()));
}

} // namespace dagforge::test
