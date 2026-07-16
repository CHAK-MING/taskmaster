#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/util/json.hpp"
#include "dagforge/util/log.hpp"

#include "detail/executor_config_validation.hpp"

#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>

namespace dagforge::config {
namespace {

[[nodiscard]] auto read_config_file(std::string_view path)
    -> Result<std::string> {
  std::ifstream input(std::string(path), std::ios::binary);
  if (!input) {
    return fail(Error::FileNotFound);
  }
  std::string text(std::istreambuf_iterator<char>(input), {});
  if (!input.good() && !input.eof()) {
    return fail(Error::FileOpenFailed);
  }
  return ok(std::move(text));
}

template <typename T>
auto apply_env_override(const char *name, T &target) -> void {
  if (const char *value = std::getenv(name); value != nullptr) {
    target = boost::lexical_cast<T>(value);
  }
}

auto apply_env_override(const char *name, std::string &target) -> void {
  if (const char *value = std::getenv(name); value != nullptr) {
    target = value;
  }
}

auto apply_env_override_bool(const char *name, bool &target) -> void {
  if (const char *value = std::getenv(name); value != nullptr) {
    const std::string_view token{value};
    target = token == "1" || token == "true";
  }
}

[[nodiscard]] auto validate_runtime(const RuntimeConfig &runtime)
    -> std::optional<std::string_view> {
  if (runtime.shards < 0) {
    return "runtime.shards must be non-negative";
  }
  if (runtime.cpu_affinity_offset < 0) {
    return "runtime.cpu_affinity_offset must be non-negative";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_sandbox(const MinijailConfig &sandbox)
    -> std::optional<std::string_view> {
  if (sandbox.executable.empty()) {
    return "executors.command.minijail.executable must not be empty";
  }
  if (sandbox.seccomp_bpf_path.empty()) {
    return "executors.command.minijail.seccomp_bpf_path must not be empty";
  }
  if (sandbox.execution_root.empty()) {
    return "executors.command.minijail.execution_root must not be empty";
  }
  if (!detail::minijail_resource_limits_valid(sandbox)) {
    return "executors.command.minijail resource limits must be greater than zero";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_http_egress(const HttpEgressConfig &egress)
    -> std::optional<std::string_view> {
  if (!detail::http_request_response_limits_valid(egress)) {
    return "executors.http.egress request and response limits must be greater than zero";
  }
  if (!detail::http_concurrency_limits_valid(egress)) {
    return "executors.http.egress concurrency limits must be greater than zero";
  }
  if (!detail::http_timeout_limits_valid(egress)) {
    return "executors.http.egress timeouts must be greater than zero";
  }
  if (!detail::http_idle_connection_limits_valid(egress)) {
    return "executors.http.egress idle connection limits are inconsistent";
  }
  if (!detail::http_tls_version_valid(egress)) {
    return "executors.http.egress.tls_min_version must be 1.2 or 1.3";
  }
  if (!detail::http_tls_client_identity_valid(egress)) {
    return "executors.http.egress TLS client certificate and key must be configured together";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_admission(const AdmissionConfig &admission)
    -> std::optional<std::string_view> {
  if (admission.max_nodes == 0) {
    return "admission.max_nodes must be greater than zero";
  }
  if (admission.max_parallel_nodes == 0 ||
      admission.max_parallel_nodes > admission.max_nodes) {
    return "admission.max_parallel_nodes must be between 1 and max_nodes";
  }
  if (admission.max_total_output_bytes == 0) {
    return "admission.max_total_output_bytes must be greater than zero";
  }
  if (admission.max_run_duration_sec <= 0) {
    return "admission.max_run_duration_sec must be greater than zero";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_storage(const StorageConfig &storage)
    -> std::optional<std::string_view> {
  if (storage.enabled && storage.directory.empty()) {
    return "storage.directory is required when storage is enabled";
  }
  if (storage.max_completed_runs == 0) {
    return "storage.max_completed_runs must be greater than zero";
  }
  if (storage.max_evidence_records == 0) {
    return "storage.max_evidence_records must be greater than zero";
  }
  if (storage.max_plan_bytes == 0 || storage.max_checkpoint_bytes == 0 ||
      storage.max_evidence_file_bytes == 0 ||
      storage.max_evidence_record_bytes == 0 ||
      storage.max_artifact_metadata_bytes == 0 ||
      storage.max_artifact_bytes == 0) {
    return "storage byte limits must be greater than zero";
  }
  if (storage.max_evidence_record_bytes > storage.max_evidence_file_bytes) {
    return "storage.max_evidence_record_bytes must not exceed max_evidence_file_bytes";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_api(const ApiConfig &api)
    -> std::optional<std::string_view> {
  if (api.max_request_body_bytes == 0 || api.max_request_header_bytes == 0) {
    return "api request limits must be greater than zero";
  }
  if (api.connection_idle_timeout_ms == 0 || api.max_connections == 0 ||
      api.max_requests_per_connection == 0 ||
      api.max_concurrent_requests == 0) {
    return "api connection limits and timeouts must be greater than zero";
  }
  if (api.tls_min_version != "1.2" && api.tls_min_version != "1.3") {
    return "api.tls_min_version must be 1.2 or 1.3";
  }
  if (api.tls_enabled &&
      (api.tls_cert_file.empty() || api.tls_key_file.empty())) {
    return "api TLS certificate and key are required when TLS is enabled";
  }
  return std::nullopt;
}

[[nodiscard]] auto validate_system_config(const SystemConfig &config)
    -> std::optional<std::string_view> {
  if (auto violation = validate_runtime(config.runtime)) {
    return violation;
  }
  if (auto violation = validate_sandbox(config.executors.command.minijail)) {
    return violation;
  }
  if (auto violation = validate_http_egress(config.executors.http.egress)) {
    return violation;
  }
  if (auto violation = validate_admission(config.admission)) {
    return violation;
  }
  if (auto violation = validate_storage(config.storage)) {
    return violation;
  }
  return validate_api(config.api);
}

auto apply_environment_overrides(SystemConfig &cfg) -> void {
  auto &http = cfg.executors.http;
  auto &egress = http.egress;
  auto &command = cfg.executors.command;
  auto &minijail = command.minijail;

  apply_env_override("DAGFORGE_API_PORT", cfg.api.port);
  apply_env_override("DAGFORGE_API_HOST", cfg.api.host);
  apply_env_override_bool("DAGFORGE_API_ENABLED", cfg.api.enabled);
  apply_env_override_bool("DAGFORGE_API_REUSEPORT", cfg.api.reuse_port);
  apply_env_override_bool("DAGFORGE_API_TLS_ENABLED", cfg.api.tls_enabled);
  apply_env_override("DAGFORGE_API_TLS_CERT_FILE", cfg.api.tls_cert_file);
  apply_env_override("DAGFORGE_API_TLS_KEY_FILE", cfg.api.tls_key_file);
  apply_env_override("DAGFORGE_API_TLS_MIN_VERSION",
                     cfg.api.tls_min_version);
  apply_env_override("DAGFORGE_API_BEARER_TOKEN_ENV",
                     cfg.api.bearer_token_env);
  apply_env_override("DAGFORGE_API_MAX_REQUEST_BODY_BYTES",
                     cfg.api.max_request_body_bytes);
  apply_env_override("DAGFORGE_API_MAX_REQUEST_HEADER_BYTES",
                     cfg.api.max_request_header_bytes);
  apply_env_override("DAGFORGE_API_CONNECTION_IDLE_TIMEOUT_MS",
                     cfg.api.connection_idle_timeout_ms);
  apply_env_override("DAGFORGE_API_MAX_CONNECTIONS",
                     cfg.api.max_connections);
  apply_env_override("DAGFORGE_API_MAX_REQUESTS_PER_CONNECTION",
                     cfg.api.max_requests_per_connection);
  apply_env_override("DAGFORGE_API_MAX_CONCURRENT_REQUESTS",
                     cfg.api.max_concurrent_requests);

  apply_env_override_bool("DAGFORGE_WORKFLOW_ENABLED", cfg.workflow.enabled);

  apply_env_override_bool("DAGFORGE_HTTP_EXECUTOR_ENABLED",
                          http.enabled);
  apply_env_override_bool("DAGFORGE_HTTP_EXECUTOR_ALLOW_PLAINTEXT",
                          egress.allow_plaintext);
  apply_env_override_bool("DAGFORGE_HTTP_EXECUTOR_DENY_PRIVATE_NETWORKS",
                          egress.deny_private_networks);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADERS",
                     egress.max_request_headers);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADER_BYTES",
                     egress.max_request_header_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_BODY_BYTES",
                     egress.max_request_body_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADER_BYTES",
                     egress.max_response_header_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADERS",
                     egress.max_response_headers);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_BODY_BYTES",
                     egress.max_response_body_bytes);
  apply_env_override(
      "DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS_PER_SHARD",
      egress.max_concurrent_requests_per_shard);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS",
                     egress.max_concurrent_requests);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_DNS_TIMEOUT_MS",
                     egress.dns_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_CONNECT_TIMEOUT_MS",
                     egress.connect_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_HANDSHAKE_TIMEOUT_MS",
                     egress.tls_handshake_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_WRITE_TIMEOUT_MS",
                     egress.write_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_FIRST_BYTE_TIMEOUT_MS",
                     egress.first_byte_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_READ_TIMEOUT_MS",
                     egress.read_timeout_ms);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_IDLE_CONNECTION_TIMEOUT_MS",
                     egress.idle_connection_timeout_ms);
  apply_env_override(
      "DAGFORGE_HTTP_EXECUTOR_MAX_IDLE_CONNECTIONS_PER_ORIGIN",
      egress.max_idle_connections_per_origin);
  apply_env_override(
      "DAGFORGE_HTTP_EXECUTOR_MAX_IDLE_CONNECTIONS_PER_SHARD",
      egress.max_idle_connections_per_shard);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_MIN_VERSION",
                     egress.tls_min_version);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CA_FILE",
                     egress.tls_ca_file);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_CERT_FILE",
                     egress.tls_client_cert_file);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_KEY_FILE",
                     egress.tls_client_key_file);

  apply_env_override("DAGFORGE_RUNTIME_SHARDS", cfg.runtime.shards);
  apply_env_override_bool("DAGFORGE_RUNTIME_PIN_SHARDS",
                          cfg.runtime.pin_shards_to_cores);
  apply_env_override("DAGFORGE_RUNTIME_CPU_AFFINITY_OFFSET",
                     cfg.runtime.cpu_affinity_offset);

  apply_env_override("DAGFORGE_SANDBOX_MINIJAIL",
                     minijail.executable);
  apply_env_override("DAGFORGE_SANDBOX_SECCOMP_BPF",
                     minijail.seccomp_bpf_path);
  apply_env_override("DAGFORGE_SANDBOX_EXECUTION_ROOT",
                     minijail.execution_root);
  apply_env_override("DAGFORGE_SANDBOX_MAX_MEMORY_BYTES",
                     minijail.max_memory_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_FILE_BYTES",
                     minijail.max_file_bytes);
  apply_env_override("DAGFORGE_SANDBOX_TMP_BYTES", minijail.tmp_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STDOUT_BYTES",
                     minijail.max_stdout_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STDERR_BYTES",
                     minijail.max_stderr_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STREAM_LINE_BYTES",
                     minijail.max_stream_line_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_PROCESSES",
                     minijail.max_processes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_OPEN_FILES",
                     minijail.max_open_files);
}

[[nodiscard]] auto finalize_config(SystemConfig cfg) -> Result<SystemConfig> {
  apply_environment_overrides(cfg);
  if (auto violation = validate_system_config(cfg)) {
    log::error("Invalid system configuration: {}", *violation);
    return fail(Error::ParseError);
  }
  return ok(std::move(cfg));
}

} // namespace

auto SystemConfigLoader::load_from_file(std::string_view path)
    -> Result<SystemConfig> {
  auto text = read_config_file(path);
  if (!text) {
    return fail(text.error());
  }
  return load_from_string(*text);
}

auto SystemConfigLoader::load_from_string(std::string_view json)
    -> Result<SystemConfig> {
  try {
    auto config = parse_json_as<SystemConfig>(json);
    if (!config) {
      log::error("Failed to parse JSON system configuration");
      return fail(config.error());
    }
    return finalize_config(std::move(*config));
  } catch (const std::exception &e) {
    log::error("Failed to load JSON system configuration: {}", e.what());
    return fail(Error::ParseError);
  } catch (...) {
    log::error("Failed to load JSON system configuration: unknown exception");
    return fail(Error::ParseError);
  }
}

} // namespace dagforge::config
