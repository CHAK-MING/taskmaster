#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/util/enum.hpp"
#include <boost/describe/enum.hpp>
#include <cstdint>
#include <string>
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
BOOST_DESCRIBE_ENUM(DAGSourceMode, File, Api, Hybrid)

[[nodiscard]] constexpr auto to_string_view(DAGSourceMode value) noexcept
    -> std::string_view {
  return ::dagforge::util::enum_to_snake_case_view(value);
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
  SchedulerConfig scheduler;
  ApiConfig api;
  DAGSourceConfig dag_source;

  auto operator==(const SystemConfig &) const -> bool = default;
};

} // namespace dagforge
