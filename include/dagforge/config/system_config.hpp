#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#endif


namespace dagforge {

struct DatabaseConfig {
  std::string host{"127.0.0.1"};
  std::uint16_t port{3306};
  std::string username{"dagforge"};
  std::string password{"dagforge"};
  std::string database{"dagforge"};
  std::uint16_t pool_size{4};       // per-shard pool size
  std::uint16_t connect_timeout{5}; // seconds

  auto operator==(const DatabaseConfig &) const -> bool = default;
};

struct ComputeConfig {
  int threads{0}; // 0 = auto
  int queue_capacity{1024};
  bool pin_threads_to_cores{false};
  int cpu_affinity_offset{0};

  auto operator==(const ComputeConfig &) const -> bool = default;
};

struct SchedulerConfig {
  std::string log_level{"info"};
  std::string log_file;
  std::string pid_file;
  int tick_interval_ms{1000};
  int max_concurrency{10};
  int shards{0}; // 0 = auto (hardware_concurrency)
  int scheduler_shards{1};
  bool pin_shards_to_cores{false};
  int cpu_affinity_offset{0};
  int zombie_reaper_interval_sec{0};
  int zombie_heartbeat_timeout_sec{0};

  auto operator==(const SchedulerConfig &) const -> bool = default;
};

struct ModelProviderConfig {
  std::string name{"openai"};
  std::string base_url{"https://api.openai.com"};
  std::string responses_path{"/v1/responses"};
  std::string api_key_env{"OPENAI_API_KEY"};
  int timeout_sec{120};
  std::size_t max_response_bytes{16UL * 1024UL * 1024UL};

  auto operator==(const ModelProviderConfig &) const -> bool = default;
};

struct McpServerConfig {
  std::string name;
  std::string url;
  std::string bearer_token_env;
  std::string protocol_version{"2025-06-18"};
  int timeout_sec{120};
  std::size_t max_response_bytes{16UL * 1024UL * 1024UL};

  auto operator==(const McpServerConfig &) const -> bool = default;
};

struct WorkflowConfig {
  bool enabled{true};
  std::vector<ModelProviderConfig> model_providers{{}};
  std::vector<McpServerConfig> mcp_servers;

  auto operator==(const WorkflowConfig &) const -> bool = default;
};

struct ApiConfig {
  bool enabled{false};
  std::uint16_t port{8888};
  std::string host{"127.0.0.1"};
  bool reuse_port{false};
  bool tls_enabled{false};
  std::string tls_cert_file;
  std::string tls_key_file;

  auto operator==(const ApiConfig &) const -> bool = default;
};

enum class DAGSourceMode : std::uint8_t { File, Api, Hybrid };

} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::DAGSourceMode> {
  static constexpr auto value =
      glz::enumerate("file", dagforge::DAGSourceMode::File, "api",
                dagforge::DAGSourceMode::Api, "hybrid",
                dagforge::DAGSourceMode::Hybrid);
};
} // namespace glz

namespace dagforge {

[[nodiscard]] constexpr auto to_string_view(DAGSourceMode value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_string_view(value);
}

template <>
[[nodiscard]] inline auto parse<DAGSourceMode>(std::string_view s) noexcept
    -> DAGSourceMode {
  return ::dagforge::util::parse_enum(s, DAGSourceMode::File);
}

struct DAGSourceConfig {
  DAGSourceMode mode{DAGSourceMode::File};
  std::string directory{"./dags"};
  int scan_interval_sec{30};

  auto operator==(const DAGSourceConfig &) const -> bool = default;
};

struct SystemConfig {
  DatabaseConfig database;
  ComputeConfig compute;
  WorkflowConfig workflow;
  SchedulerConfig scheduler;
  ApiConfig api;
  DAGSourceConfig dag_source;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge
