#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/log.hpp"


#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <string>
#include <string_view>


namespace dagforge {
namespace detail {

struct SystemToml {
  WorkflowConfig workflow{};
  HttpExecutorConfig http_executor{};
  AdmissionConfig admission{};
  StorageConfig storage{};
  RuntimeConfig runtime{};
  SandboxConfig sandbox{};
  ApiConfig api{};
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::WorkflowConfig> {
  using T = dagforge::WorkflowConfig;
  static constexpr auto value = object("enabled", &T::enabled);
};

template <> struct meta<dagforge::HttpExecutorConfig> {
  using T = dagforge::HttpExecutorConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "allow_plaintext", &T::allow_plaintext,
      "deny_private_networks", &T::deny_private_networks, "allowed_origins",
      &T::allowed_origins, "allowed_ip_cidrs", &T::allowed_ip_cidrs,
      "max_request_headers",
      &T::max_request_headers, "max_request_header_bytes",
      &T::max_request_header_bytes, "max_request_body_bytes",
      &T::max_request_body_bytes, "max_response_headers",
      &T::max_response_headers, "max_response_header_bytes",
      &T::max_response_header_bytes, "max_response_body_bytes",
      &T::max_response_body_bytes, "max_concurrent_requests_per_shard",
      &T::max_concurrent_requests_per_shard, "max_concurrent_requests",
      &T::max_concurrent_requests, "tls_min_version", &T::tls_min_version,
      "tls_ca_file", &T::tls_ca_file, "tls_client_cert_file",
      &T::tls_client_cert_file, "tls_client_key_file",
      &T::tls_client_key_file);
};

template <> struct meta<dagforge::AdmissionConfig> {
  using T = dagforge::AdmissionConfig;
  static constexpr auto value = object(
      "allow_unlisted_executors", &T::allow_unlisted_executors,
      "allowed_executors", &T::allowed_executors, "max_nodes", &T::max_nodes,
      "max_parallel_nodes", &T::max_parallel_nodes,
      "max_total_output_bytes", &T::max_total_output_bytes,
      "max_run_duration_sec", &T::max_run_duration_sec);
};

template <> struct meta<dagforge::StorageConfig> {
  using T = dagforge::StorageConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "directory", &T::directory,
      "max_completed_runs", &T::max_completed_runs,
      "max_evidence_records", &T::max_evidence_records);
};

template <> struct meta<dagforge::RuntimeConfig> {
  using T = dagforge::RuntimeConfig;
  static constexpr auto value = object(
      "shards", &T::shards, "pin_shards_to_cores", &T::pin_shards_to_cores,
      "cpu_affinity_offset", &T::cpu_affinity_offset);
};

template <> struct meta<dagforge::SandboxConfig> {
  using T = dagforge::SandboxConfig;
  static constexpr auto value = object(
      "minijail_path", &T::minijail_path, "seccomp_bpf_path",
      &T::seccomp_bpf_path, "workspace_root", &T::workspace_root,
      "max_memory_bytes", &T::max_memory_bytes,
      "max_file_bytes", &T::max_file_bytes, "tmp_bytes", &T::tmp_bytes,
      "max_stdout_bytes", &T::max_stdout_bytes, "max_stderr_bytes",
      &T::max_stderr_bytes, "max_stream_line_bytes",
      &T::max_stream_line_bytes,
      "max_processes", &T::max_processes, "max_open_files",
      &T::max_open_files, "allow_unlisted_programs",
      &T::allow_unlisted_programs, "allow_unlisted_environment",
      &T::allow_unlisted_environment, "require_trusted_files",
      &T::require_trusted_files, "retain_workspaces", &T::retain_workspaces,
      "allowed_programs",
      &T::allowed_programs, "allowed_environment", &T::allowed_environment);
};

template <> struct meta<dagforge::ApiConfig> {
  using T = dagforge::ApiConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "port", &T::port, "host", &T::host, "reuse_port",
      &T::reuse_port, "tls_enabled", &T::tls_enabled, "tls_cert_file",
      &T::tls_cert_file, "tls_key_file", &T::tls_key_file,
      "tls_min_version", &T::tls_min_version, "bearer_token_env",
      &T::bearer_token_env, "max_request_header_bytes",
      &T::max_request_header_bytes, "max_request_body_bytes",
      &T::max_request_body_bytes, "max_concurrent_requests",
      &T::max_concurrent_requests, "connection_idle_timeout_ms",
      &T::connection_idle_timeout_ms, "max_connections", &T::max_connections,
      "max_requests_per_connection", &T::max_requests_per_connection);
};

template <> struct meta<dagforge::detail::SystemToml> {
  using T = dagforge::detail::SystemToml;
  static constexpr auto value = object(
      "workflow", &T::workflow, "http_executor", &T::http_executor,
      "admission", &T::admission, "storage", &T::storage, "runtime",
      &T::runtime, "sandbox", &T::sandbox, "api", &T::api);
};
} // namespace glz

namespace dagforge {
namespace {

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

[[nodiscard]] auto convert_toml(std::string_view toml_text)
    -> Result<SystemConfig> {
  auto raw_result = toml_util::parse_toml<detail::SystemToml>(toml_text);
  if (!raw_result)
    return fail(raw_result.error());
  auto raw = std::move(*raw_result);

  SystemConfig cfg{};
  cfg.workflow = std::move(raw.workflow);
  cfg.http_executor = std::move(raw.http_executor);
  cfg.admission = std::move(raw.admission);
  cfg.storage = std::move(raw.storage);
  cfg.runtime = std::move(raw.runtime);
  cfg.sandbox = std::move(raw.sandbox);
  cfg.api = std::move(raw.api);

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
                          cfg.http_executor.enabled);
  apply_env_override_bool("DAGFORGE_HTTP_EXECUTOR_ALLOW_PLAINTEXT",
                          cfg.http_executor.allow_plaintext);
  apply_env_override_bool("DAGFORGE_HTTP_EXECUTOR_DENY_PRIVATE_NETWORKS",
                          cfg.http_executor.deny_private_networks);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADERS",
                     cfg.http_executor.max_request_headers);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_HEADER_BYTES",
                     cfg.http_executor.max_request_header_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_REQUEST_BODY_BYTES",
                     cfg.http_executor.max_request_body_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADER_BYTES",
                     cfg.http_executor.max_response_header_bytes);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_HEADERS",
                     cfg.http_executor.max_response_headers);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_RESPONSE_BODY_BYTES",
                     cfg.http_executor.max_response_body_bytes);
  apply_env_override(
      "DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS_PER_SHARD",
      cfg.http_executor.max_concurrent_requests_per_shard);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_MAX_CONCURRENT_REQUESTS",
                     cfg.http_executor.max_concurrent_requests);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_MIN_VERSION",
                     cfg.http_executor.tls_min_version);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CA_FILE",
                     cfg.http_executor.tls_ca_file);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_CERT_FILE",
                     cfg.http_executor.tls_client_cert_file);
  apply_env_override("DAGFORGE_HTTP_EXECUTOR_TLS_CLIENT_KEY_FILE",
                     cfg.http_executor.tls_client_key_file);

  apply_env_override("DAGFORGE_RUNTIME_SHARDS", cfg.runtime.shards);
  apply_env_override_bool("DAGFORGE_RUNTIME_PIN_SHARDS",
                          cfg.runtime.pin_shards_to_cores);
  apply_env_override("DAGFORGE_RUNTIME_CPU_AFFINITY_OFFSET",
                     cfg.runtime.cpu_affinity_offset);

  apply_env_override("DAGFORGE_SANDBOX_MINIJAIL",
                     cfg.sandbox.minijail_path);
  apply_env_override("DAGFORGE_SANDBOX_SECCOMP_BPF",
                     cfg.sandbox.seccomp_bpf_path);
  apply_env_override("DAGFORGE_SANDBOX_WORKSPACE_ROOT",
                     cfg.sandbox.workspace_root);
  apply_env_override("DAGFORGE_SANDBOX_MAX_MEMORY_BYTES",
                     cfg.sandbox.max_memory_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_FILE_BYTES",
                     cfg.sandbox.max_file_bytes);
  apply_env_override("DAGFORGE_SANDBOX_TMP_BYTES", cfg.sandbox.tmp_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STDOUT_BYTES",
                     cfg.sandbox.max_stdout_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STDERR_BYTES",
                     cfg.sandbox.max_stderr_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_STREAM_LINE_BYTES",
                     cfg.sandbox.max_stream_line_bytes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_PROCESSES",
                     cfg.sandbox.max_processes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_OPEN_FILES",
                     cfg.sandbox.max_open_files);

  if (cfg.runtime.shards < 0 || cfg.runtime.cpu_affinity_offset < 0 ||
      cfg.sandbox.minijail_path.empty() ||
      cfg.sandbox.seccomp_bpf_path.empty() ||
      cfg.sandbox.workspace_root.empty() ||
      cfg.sandbox.max_memory_bytes == 0 || cfg.sandbox.max_file_bytes == 0 ||
      cfg.sandbox.tmp_bytes == 0 || cfg.sandbox.max_stdout_bytes == 0 ||
      cfg.sandbox.max_stderr_bytes == 0 ||
      cfg.sandbox.max_stream_line_bytes == 0 ||
      cfg.sandbox.max_processes == 0 ||
      cfg.sandbox.max_open_files == 0 || cfg.admission.max_nodes == 0 ||
      cfg.http_executor.max_request_headers == 0 ||
      cfg.http_executor.max_request_header_bytes == 0 ||
      cfg.http_executor.max_request_body_bytes == 0 ||
      cfg.http_executor.max_response_headers == 0 ||
      cfg.http_executor.max_response_header_bytes == 0 ||
      cfg.http_executor.max_response_body_bytes == 0 ||
      cfg.http_executor.max_concurrent_requests_per_shard == 0 ||
      cfg.http_executor.max_concurrent_requests == 0 ||
      (cfg.http_executor.tls_min_version != "1.2" &&
       cfg.http_executor.tls_min_version != "1.3") ||
      (cfg.http_executor.tls_client_cert_file.empty() !=
       cfg.http_executor.tls_client_key_file.empty()) ||
      cfg.admission.max_parallel_nodes == 0 ||
      cfg.admission.max_parallel_nodes > cfg.admission.max_nodes ||
      cfg.admission.max_total_output_bytes == 0 ||
      cfg.admission.max_run_duration_sec <= 0 ||
      (cfg.storage.enabled && cfg.storage.directory.empty()) ||
      cfg.storage.max_completed_runs == 0 ||
      cfg.storage.max_evidence_records == 0 ||
      cfg.api.max_request_body_bytes == 0 ||
      cfg.api.max_request_header_bytes == 0 ||
      cfg.api.connection_idle_timeout_ms == 0 ||
      cfg.api.max_connections == 0 ||
      cfg.api.max_requests_per_connection == 0 ||
      cfg.api.max_concurrent_requests == 0 ||
      (cfg.api.tls_min_version != "1.2" &&
       cfg.api.tls_min_version != "1.3") ||
      (cfg.api.tls_enabled &&
       (cfg.api.tls_cert_file.empty() || cfg.api.tls_key_file.empty()))) {
    return fail(Error::ParseError);
  }
  return ok(std::move(cfg));
}

} // namespace

auto SystemConfigLoader::load_from_file(std::string_view path)
    -> Result<SystemConfig> {
  auto text = toml_util::read_file(path);
  if (!text) {
    return fail(text.error());
  }
  return load_from_string(*text);
}

auto SystemConfigLoader::load_from_string(std::string_view toml_str)
    -> Result<SystemConfig> {
  try {
    return convert_toml(toml_str);
  } catch (const std::exception &e) {
    log::error("Failed to parse TOML system configuration: {}", e.what());
    return fail(Error::ParseError);
  } catch (...) {
    log::error("Failed to parse TOML system configuration: unknown exception");
    return fail(Error::ParseError);
  }
}

} // namespace dagforge
