#pragma once

#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/config/http_executor_config.hpp"

namespace dagforge::config::detail {

[[nodiscard]] inline auto minijail_resource_limits_valid(
    const MinijailConfig &config) noexcept -> bool {
  return config.max_memory_bytes > 0 && config.max_file_bytes > 0 &&
         config.tmp_bytes > 0 && config.max_stdout_bytes > 0 &&
         config.max_stderr_bytes > 0 && config.max_stream_line_bytes > 0 &&
         config.max_processes > 0 && config.max_open_files > 0;
}

[[nodiscard]] inline auto http_request_response_limits_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.max_request_headers > 0 &&
         config.max_request_header_bytes > 0 &&
         config.max_request_body_bytes > 0 &&
         config.max_response_headers > 0 &&
         config.max_response_header_bytes > 0 &&
         config.max_response_body_bytes > 0;
}

[[nodiscard]] inline auto http_concurrency_limits_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.max_concurrent_requests_per_shard > 0 &&
         config.max_concurrent_requests > 0;
}

[[nodiscard]] inline auto http_timeout_limits_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.dns_timeout_ms > 0 && config.connect_timeout_ms > 0 &&
         config.tls_handshake_timeout_ms > 0 && config.write_timeout_ms > 0 &&
         config.first_byte_timeout_ms > 0 && config.read_timeout_ms > 0 &&
         config.idle_connection_timeout_ms > 0;
}

[[nodiscard]] inline auto http_idle_connection_limits_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.max_idle_connections_per_origin > 0 &&
         config.max_idle_connections_per_shard > 0 &&
         config.max_idle_connections_per_origin <=
             config.max_idle_connections_per_shard;
}

[[nodiscard]] inline auto http_tls_version_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.tls_min_version == "1.2" || config.tls_min_version == "1.3";
}

[[nodiscard]] inline auto http_tls_client_identity_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return config.tls_client_cert_file.empty() ==
         config.tls_client_key_file.empty();
}

[[nodiscard]] inline auto http_egress_config_valid(
    const HttpEgressConfig &config) noexcept -> bool {
  return http_request_response_limits_valid(config) &&
         http_concurrency_limits_valid(config) &&
         http_timeout_limits_valid(config) &&
         http_idle_connection_limits_valid(config) &&
         http_tls_version_valid(config) &&
         http_tls_client_identity_valid(config);
}

} // namespace dagforge::config::detail
