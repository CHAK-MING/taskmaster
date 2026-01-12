#pragma once

#include <cstdint>
#include <string>

namespace taskmaster {

struct StorageConfig {
  std::string db_file{"taskmaster.db"};
};

struct SchedulerConfig {
  std::string log_level{"info"};
  std::string log_file;
  std::string pid_file;
  int tick_interval_ms{1000};
  int max_concurrency{10};
};

struct ApiConfig {
  bool enabled{false};
  uint16_t port{8080};
  std::string host{"127.0.0.1"};
};

enum class DAGSourceMode { File, Api, Hybrid };

[[nodiscard]] constexpr auto dag_source_mode_to_string(DAGSourceMode mode) noexcept
    -> std::string_view {
  switch (mode) {
    case DAGSourceMode::File: return "file";
    case DAGSourceMode::Api: return "api";
    case DAGSourceMode::Hybrid: return "hybrid";
  }
  return "file";
}

[[nodiscard]] inline auto string_to_dag_source_mode(std::string_view str) noexcept
    -> DAGSourceMode {
  if (str == "api") return DAGSourceMode::Api;
  if (str == "hybrid") return DAGSourceMode::Hybrid;
  return DAGSourceMode::File;
}

struct DAGSourceConfig {
  DAGSourceMode mode{DAGSourceMode::File};
  std::string directory{"./dags"};
  int scan_interval_sec{30};
};

struct SystemConfig {
  StorageConfig storage;
  SchedulerConfig scheduler;
  ApiConfig api;
  DAGSourceConfig dag_source;
};

}  // namespace taskmaster
