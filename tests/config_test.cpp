#include "dagforge/config/system_config_loader.hpp"

#include "gtest/gtest.h"

#include <cstdlib>

using namespace dagforge;

TEST(ConfigTest, ComputeDefaults) {
  ComputeConfig cfg;
  EXPECT_EQ(cfg.threads, 0);
  EXPECT_EQ(cfg.queue_capacity, 1024);
  EXPECT_FALSE(cfg.pin_threads_to_cores);
  EXPECT_EQ(cfg.cpu_affinity_offset, 0);
}

TEST(ConfigTest, RuntimeDefaults) {
  RuntimeConfig cfg;
  EXPECT_EQ(cfg.shards, 0);
  EXPECT_FALSE(cfg.pin_shards_to_cores);
  EXPECT_EQ(cfg.cpu_affinity_offset, 0);
}

TEST(ConfigTest, ApiDefaults) {
  ApiConfig cfg;
  EXPECT_FALSE(cfg.enabled);
  EXPECT_EQ(cfg.host, "127.0.0.1");
  EXPECT_EQ(cfg.port, 8888);
}

TEST(ConfigTest, LoadFromTomlString) {
  std::string toml = R"(
[compute]
threads = 3
queue_capacity = 256
pin_threads_to_cores = true
cpu_affinity_offset = 2

[runtime]
shards = 2
pin_shards_to_cores = true
cpu_affinity_offset = 1

[api]
enabled = true
port = 9999
host = "0.0.0.0"
)";

  auto result = SystemConfigLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(result->compute.threads, 3);
  EXPECT_EQ(result->compute.queue_capacity, 256);
  EXPECT_TRUE(result->compute.pin_threads_to_cores);
  EXPECT_EQ(result->compute.cpu_affinity_offset, 2);
  ASSERT_EQ(result->workflow.model_providers.size(), 1U);
  EXPECT_EQ(result->workflow.model_providers.front().name, "openai");
  EXPECT_EQ(result->runtime.shards, 2);
  EXPECT_TRUE(result->runtime.pin_shards_to_cores);
  EXPECT_EQ(result->runtime.cpu_affinity_offset, 1);
  EXPECT_TRUE(result->api.enabled);
  EXPECT_EQ(result->api.port, 9999);
}

TEST(ConfigTest, RejectsInvalidComputeConfiguration) {
  auto negative_threads = SystemConfigLoader::load_from_string(R"(
[compute]
threads = -1
)");
  ASSERT_FALSE(negative_threads.has_value());
  EXPECT_EQ(negative_threads.error(), make_error_code(Error::ParseError));

  auto empty_queue = SystemConfigLoader::load_from_string(R"(
[compute]
queue_capacity = 0
)");
  ASSERT_FALSE(empty_queue.has_value());
  EXPECT_EQ(empty_queue.error(), make_error_code(Error::ParseError));
}

TEST(ConfigTest, EnvironmentOverridesTakePrecedence) {
  constexpr auto *kApiPort = "DAGFORGE_API_PORT";
  constexpr auto *kComputeThreads = "DAGFORGE_COMPUTE_THREADS";
  constexpr auto *kRuntimeShards = "DAGFORGE_RUNTIME_SHARDS";

  ::setenv(kApiPort, "7777", 1);
  ::setenv(kComputeThreads, "6", 1);
  ::setenv(kRuntimeShards, "3", 1);

  std::string toml = R"(
[api]
port = 8080
)";

  auto result = SystemConfigLoader::load_from_string(toml);

  ::unsetenv(kApiPort);
  ::unsetenv(kComputeThreads);
  ::unsetenv(kRuntimeShards);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->api.port, 7777);
  EXPECT_EQ(result->compute.threads, 6);
  EXPECT_EQ(result->runtime.shards, 3);
}
