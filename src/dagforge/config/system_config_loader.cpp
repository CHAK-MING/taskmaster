#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/log.hpp"


#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>


namespace dagforge::config {
namespace detail {

struct HttpExecutorToml {
  bool enabled{false};
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
};

struct SandboxToml {
  std::string minijail_path{"~/.local/libexec/dagforge/minijail/minijail0"};
  std::string seccomp_bpf_path{
      "~/.local/libexec/dagforge/minijail/dagforge_command.bpf"};
  std::optional<std::string> execution_root;
  std::optional<std::string> workspace_root;
  std::uint64_t max_memory_bytes{1024ULL * 1024ULL * 1024ULL};
  std::uint64_t max_file_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t tmp_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stdout_bytes{10ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stderr_bytes{10ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stream_line_bytes{64ULL * 1024ULL};
  std::uint32_t max_processes{128};
  std::uint32_t max_open_files{256};
  bool allow_unlisted_programs{false};
  bool allow_unlisted_environment{false};
  bool require_trusted_files{true};
  std::optional<bool> retain_workdirs;
  std::optional<bool> retain_workspaces;
  std::vector<CommandProgramConfig> programs;
  std::vector<std::string> allowed_programs;
  std::vector<std::string> allowed_environment;
  std::optional<std::vector<std::string>> inherited_environment;
};

struct SystemToml {
  WorkflowConfig workflow{};
  HttpExecutorToml http_executor{};
  AdmissionConfig admission{};
  StorageConfig storage{};
  RuntimeConfig runtime{};
  SandboxToml sandbox{};
  ApiConfig api{};
};

} // namespace detail
} // namespace dagforge::config

namespace glz {
template <> struct meta<dagforge::config::CommandProgramConfig> {
  using T = dagforge::config::CommandProgramConfig;
  static constexpr auto value = object("name", &T::name, "path", &T::path);
};

template <> struct meta<dagforge::config::WorkflowConfig> {
  using T = dagforge::config::WorkflowConfig;
  static constexpr auto value = object("enabled", &T::enabled);
};

template <> struct meta<dagforge::config::detail::HttpExecutorToml> {
  using T = dagforge::config::detail::HttpExecutorToml;
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
      &T::max_concurrent_requests, "dns_timeout_ms", &T::dns_timeout_ms,
      "connect_timeout_ms", &T::connect_timeout_ms,
      "tls_handshake_timeout_ms", &T::tls_handshake_timeout_ms,
      "write_timeout_ms", &T::write_timeout_ms, "first_byte_timeout_ms",
      &T::first_byte_timeout_ms, "read_timeout_ms", &T::read_timeout_ms,
      "idle_connection_timeout_ms", &T::idle_connection_timeout_ms,
      "max_idle_connections_per_origin",
      &T::max_idle_connections_per_origin, "max_idle_connections_per_shard",
      &T::max_idle_connections_per_shard, "tls_min_version",
      &T::tls_min_version,
      "tls_ca_file", &T::tls_ca_file, "tls_client_cert_file",
      &T::tls_client_cert_file, "tls_client_key_file",
      &T::tls_client_key_file);
};

template <> struct meta<dagforge::config::AdmissionConfig> {
  using T = dagforge::config::AdmissionConfig;
  static constexpr auto value = object(
      "allow_unlisted_executors", &T::allow_unlisted_executors,
      "allowed_executors", &T::allowed_executors, "max_nodes", &T::max_nodes,
      "max_parallel_nodes", &T::max_parallel_nodes,
      "max_total_output_bytes", &T::max_total_output_bytes,
      "max_run_duration_sec", &T::max_run_duration_sec);
};

template <> struct meta<dagforge::config::StorageConfig> {
  using T = dagforge::config::StorageConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "directory", &T::directory,
      "max_completed_runs", &T::max_completed_runs,
      "max_evidence_records", &T::max_evidence_records);
};

template <> struct meta<dagforge::config::RuntimeConfig> {
  using T = dagforge::config::RuntimeConfig;
  static constexpr auto value = object(
      "shards", &T::shards, "pin_shards_to_cores", &T::pin_shards_to_cores,
      "cpu_affinity_offset", &T::cpu_affinity_offset);
};

template <> struct meta<dagforge::config::detail::SandboxToml> {
  using T = dagforge::config::detail::SandboxToml;
  static constexpr auto value = object(
      "minijail_path", &T::minijail_path, "seccomp_bpf_path",
      &T::seccomp_bpf_path, "execution_root", &T::execution_root,
      "workspace_root", &T::workspace_root,
      "max_memory_bytes", &T::max_memory_bytes,
      "max_file_bytes", &T::max_file_bytes, "tmp_bytes", &T::tmp_bytes,
      "max_stdout_bytes", &T::max_stdout_bytes, "max_stderr_bytes",
      &T::max_stderr_bytes, "max_stream_line_bytes",
      &T::max_stream_line_bytes,
      "max_processes", &T::max_processes, "max_open_files",
      &T::max_open_files, "allow_unlisted_programs",
      &T::allow_unlisted_programs, "allow_unlisted_environment",
      &T::allow_unlisted_environment, "require_trusted_files",
      &T::require_trusted_files, "retain_workdirs", &T::retain_workdirs,
      "retain_workspaces", &T::retain_workspaces, "programs", &T::programs,
      "allowed_programs", &T::allowed_programs, "allowed_environment",
      &T::allowed_environment, "inherited_environment",
      &T::inherited_environment);
};

template <> struct meta<dagforge::config::ApiConfig> {
  using T = dagforge::config::ApiConfig;
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

template <> struct meta<dagforge::config::detail::SystemToml> {
  using T = dagforge::config::detail::SystemToml;
  static constexpr auto value = object(
      "workflow", &T::workflow, "http_executor", &T::http_executor,
      "admission", &T::admission, "storage", &T::storage, "runtime",
      &T::runtime, "sandbox", &T::sandbox, "api", &T::api);
};
} // namespace glz

namespace dagforge::config {
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
  cfg.admission = std::move(raw.admission);
  cfg.storage = std::move(raw.storage);
  cfg.runtime = std::move(raw.runtime);
  cfg.api = std::move(raw.api);

  cfg.executors.http.enabled = raw.http_executor.enabled;
  cfg.executors.http.egress = HttpEgressConfig{
      .allow_plaintext = raw.http_executor.allow_plaintext,
      .deny_private_networks = raw.http_executor.deny_private_networks,
      .allowed_origins = std::move(raw.http_executor.allowed_origins),
      .allowed_ip_cidrs = std::move(raw.http_executor.allowed_ip_cidrs),
      .max_request_headers = raw.http_executor.max_request_headers,
      .max_request_header_bytes = raw.http_executor.max_request_header_bytes,
      .max_request_body_bytes = raw.http_executor.max_request_body_bytes,
      .max_response_headers = raw.http_executor.max_response_headers,
      .max_response_header_bytes = raw.http_executor.max_response_header_bytes,
      .max_response_body_bytes = raw.http_executor.max_response_body_bytes,
      .max_concurrent_requests_per_shard =
          raw.http_executor.max_concurrent_requests_per_shard,
      .max_concurrent_requests = raw.http_executor.max_concurrent_requests,
      .dns_timeout_ms = raw.http_executor.dns_timeout_ms,
      .connect_timeout_ms = raw.http_executor.connect_timeout_ms,
      .tls_handshake_timeout_ms =
          raw.http_executor.tls_handshake_timeout_ms,
      .write_timeout_ms = raw.http_executor.write_timeout_ms,
      .first_byte_timeout_ms = raw.http_executor.first_byte_timeout_ms,
      .read_timeout_ms = raw.http_executor.read_timeout_ms,
      .idle_connection_timeout_ms =
          raw.http_executor.idle_connection_timeout_ms,
      .max_idle_connections_per_origin =
          raw.http_executor.max_idle_connections_per_origin,
      .max_idle_connections_per_shard =
          raw.http_executor.max_idle_connections_per_shard,
      .tls_min_version = std::move(raw.http_executor.tls_min_version),
      .tls_ca_file = std::move(raw.http_executor.tls_ca_file),
      .tls_client_cert_file =
          std::move(raw.http_executor.tls_client_cert_file),
      .tls_client_key_file =
          std::move(raw.http_executor.tls_client_key_file),
  };
  cfg.executors.command.policy = CommandPolicyConfig{
      .allow_unlisted_programs = raw.sandbox.allow_unlisted_programs,
      .allow_unlisted_environment = raw.sandbox.allow_unlisted_environment,
      .require_trusted_programs = raw.sandbox.require_trusted_files,
      .programs = std::move(raw.sandbox.programs),
      .allowed_programs = std::move(raw.sandbox.allowed_programs),
      .allowed_environment = std::move(raw.sandbox.allowed_environment),
      .inherited_environment = raw.sandbox.inherited_environment.value_or(
          std::vector<std::string>{"LANG", "LC_ALL", "LC_CTYPE", "TERM"}),
  };
  cfg.executors.command.minijail = MinijailConfig{
      .executable = std::move(raw.sandbox.minijail_path),
      .seccomp_bpf_path = std::move(raw.sandbox.seccomp_bpf_path),
      .execution_root = raw.sandbox.execution_root
                            .value_or(raw.sandbox.workspace_root
                                          .value_or("./executions")),
      .max_memory_bytes = raw.sandbox.max_memory_bytes,
      .max_file_bytes = raw.sandbox.max_file_bytes,
      .tmp_bytes = raw.sandbox.tmp_bytes,
      .max_stdout_bytes = raw.sandbox.max_stdout_bytes,
      .max_stderr_bytes = raw.sandbox.max_stderr_bytes,
      .max_stream_line_bytes = raw.sandbox.max_stream_line_bytes,
      .max_processes = raw.sandbox.max_processes,
      .max_open_files = raw.sandbox.max_open_files,
      .require_trusted_files = raw.sandbox.require_trusted_files,
      .retain_workdirs = raw.sandbox.retain_workdirs.value_or(
          raw.sandbox.retain_workspaces.value_or(false)),
  };

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
  apply_env_override("DAGFORGE_SANDBOX_WORKSPACE_ROOT",
                     minijail.execution_root);
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

  if (cfg.runtime.shards < 0 || cfg.runtime.cpu_affinity_offset < 0 ||
      minijail.executable.empty() || minijail.seccomp_bpf_path.empty() ||
      minijail.execution_root.empty() || minijail.max_memory_bytes == 0 ||
      minijail.max_file_bytes == 0 || minijail.tmp_bytes == 0 ||
      minijail.max_stdout_bytes == 0 || minijail.max_stderr_bytes == 0 ||
      minijail.max_stream_line_bytes == 0 || minijail.max_processes == 0 ||
      minijail.max_open_files == 0 || cfg.admission.max_nodes == 0 ||
      egress.max_request_headers == 0 ||
      egress.max_request_header_bytes == 0 ||
      egress.max_request_body_bytes == 0 ||
      egress.max_response_headers == 0 ||
      egress.max_response_header_bytes == 0 ||
      egress.max_response_body_bytes == 0 ||
      egress.max_concurrent_requests_per_shard == 0 ||
      egress.max_concurrent_requests == 0 ||
      egress.dns_timeout_ms == 0 || egress.connect_timeout_ms == 0 ||
      egress.tls_handshake_timeout_ms == 0 ||
      egress.write_timeout_ms == 0 || egress.first_byte_timeout_ms == 0 ||
      egress.read_timeout_ms == 0 ||
      egress.idle_connection_timeout_ms == 0 ||
      egress.max_idle_connections_per_origin == 0 ||
      egress.max_idle_connections_per_shard == 0 ||
      egress.max_idle_connections_per_origin >
          egress.max_idle_connections_per_shard ||
      (egress.tls_min_version != "1.2" &&
       egress.tls_min_version != "1.3") ||
      (egress.tls_client_cert_file.empty() !=
       egress.tls_client_key_file.empty()) ||
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

} // namespace dagforge::config
