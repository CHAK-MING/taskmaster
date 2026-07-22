#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace dagforge::config {

struct HttpEgressConfig {
  bool allow_plaintext{false};
  bool deny_private_networks{true};
  std::vector<std::string> allowed_origins;
  std::vector<std::string> allowed_ip_cidrs;
  std::size_t max_request_headers{64};
  std::uint64_t max_request_header_bytes{64ULL * 1024ULL};
  std::uint64_t max_request_body_bytes{1024ULL * 1024ULL};
  std::size_t max_response_headers{128};
  std::uint64_t max_response_header_bytes{64ULL * 1024ULL};
  std::uint64_t max_response_body_bytes{10ULL * 1024ULL * 1024ULL};
  std::size_t max_concurrent_requests_per_shard{32};
  std::size_t max_concurrent_requests{256};
  std::uint64_t dns_timeout_ms{5000};
  std::uint64_t connect_timeout_ms{10000};
  std::uint64_t tls_handshake_timeout_ms{10000};
  std::uint64_t write_timeout_ms{30000};
  std::uint64_t first_byte_timeout_ms{30000};
  std::uint64_t read_timeout_ms{30000};
  std::uint64_t idle_connection_timeout_ms{30000};
  std::size_t max_idle_connections_per_origin{4};
  std::size_t max_idle_connections_per_shard{32};
  std::string tls_min_version{"1.2"};
  std::string tls_ca_file;
  std::string tls_client_cert_file;
  std::string tls_client_key_file;

  auto operator==(const HttpEgressConfig &) const -> bool = default;
};

struct HttpExecutorConfig {
  bool enabled{false};
  HttpEgressConfig egress;

  auto operator==(const HttpExecutorConfig &) const -> bool = default;
};

} // namespace dagforge::config
