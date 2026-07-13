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

struct SandboxConfig {
  std::string minijail_path{
      "~/.local/libexec/dagforge/minijail/minijail0"};
  std::string seccomp_bpf_path{
      "~/.local/libexec/dagforge/minijail/dagforge_command.bpf"};
  std::string workspace_root{"./workspaces"};
  std::uint64_t max_memory_bytes{1024ULL * 1024ULL * 1024ULL};
  std::uint64_t max_file_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t tmp_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint32_t max_processes{128};
  std::uint32_t max_open_files{256};

  auto operator==(const SandboxConfig &) const -> bool = default;
};

struct WorkflowConfig {
  bool enabled{true};

  auto operator==(const WorkflowConfig &) const -> bool = default;
};

struct AdmissionConfig {
  bool allow_unlisted_programs{true};
  bool allow_unlisted_environment{true};
  std::vector<std::string> allowed_programs;
  std::vector<std::string> allowed_environment;
  std::size_t max_nodes{256};
  std::size_t max_parallel_nodes{32};
  std::uint64_t max_total_output_bytes{64ULL * 1024ULL * 1024ULL};
  int max_run_duration_sec{3600};

  auto operator==(const AdmissionConfig &) const -> bool = default;
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
  AdmissionConfig admission;
  RuntimeConfig runtime;
  SandboxConfig sandbox;
  ApiConfig api;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge
