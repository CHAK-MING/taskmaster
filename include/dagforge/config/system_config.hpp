#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dagforge {

struct ComputeConfig {
  int threads{0}; // 0 = auto
  int queue_capacity{1024};
  bool pin_threads_to_cores{false};
  int cpu_affinity_offset{0};

  auto operator==(const ComputeConfig &) const -> bool = default;
};

struct RuntimeConfig {
  int shards{0}; // 0 = auto (hardware_concurrency)
  bool pin_shards_to_cores{false};
  int cpu_affinity_offset{0};

  auto operator==(const RuntimeConfig &) const -> bool = default;
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
  std::vector<ModelProviderConfig> model_providers;
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

struct SystemConfig {
  ComputeConfig compute;
  WorkflowConfig workflow;
  RuntimeConfig runtime;
  ApiConfig api;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge
