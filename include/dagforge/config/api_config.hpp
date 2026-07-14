#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstddef>
#include <cstdint>
#include <string>
#endif

namespace dagforge::config {

struct ApiConfig {
  bool enabled{false};
  std::uint16_t port{8888};
  std::string host{"127.0.0.1"};
  bool reuse_port{false};
  bool tls_enabled{false};
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_min_version{"1.2"};
  std::string bearer_token_env;
  std::uint64_t max_request_header_bytes{64ULL * 1024ULL};
  std::uint64_t max_request_body_bytes{1024ULL * 1024ULL};
  std::uint64_t connection_idle_timeout_ms{30'000};
  std::size_t max_connections{1024};
  std::size_t max_requests_per_connection{100};
  std::size_t max_concurrent_requests{128};

  auto operator==(const ApiConfig &) const -> bool = default;
};

} // namespace dagforge::config
