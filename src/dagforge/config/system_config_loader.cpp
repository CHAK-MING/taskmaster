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
  ComputeConfig compute{};
  WorkflowConfig workflow{};
  RuntimeConfig runtime{};
  SandboxConfig sandbox{};
  ApiConfig api{};
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::ComputeConfig> {
  using T = dagforge::ComputeConfig;
  static constexpr auto value = object(
      "threads", &T::threads, "queue_capacity", &T::queue_capacity,
      "pin_threads_to_cores", &T::pin_threads_to_cores,
      "cpu_affinity_offset", &T::cpu_affinity_offset);
};

template <> struct meta<dagforge::WorkflowConfig> {
  using T = dagforge::WorkflowConfig;
  static constexpr auto value = object("enabled", &T::enabled);
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
      "max_processes", &T::max_processes, "max_open_files",
      &T::max_open_files);
};

template <> struct meta<dagforge::ApiConfig> {
  using T = dagforge::ApiConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "port", &T::port, "host", &T::host, "reuse_port",
      &T::reuse_port, "tls_enabled", &T::tls_enabled, "tls_cert_file",
      &T::tls_cert_file, "tls_key_file", &T::tls_key_file);
};

template <> struct meta<dagforge::detail::SystemToml> {
  using T = dagforge::detail::SystemToml;
  static constexpr auto value = object(
      "compute", &T::compute, "workflow", &T::workflow, "runtime",
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
  cfg.compute = std::move(raw.compute);
  cfg.workflow = std::move(raw.workflow);
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

  apply_env_override("DAGFORGE_COMPUTE_THREADS", cfg.compute.threads);
  apply_env_override("DAGFORGE_COMPUTE_QUEUE_CAPACITY",
                     cfg.compute.queue_capacity);
  apply_env_override_bool("DAGFORGE_COMPUTE_PIN_THREADS",
                          cfg.compute.pin_threads_to_cores);
  apply_env_override("DAGFORGE_COMPUTE_CPU_AFFINITY_OFFSET",
                     cfg.compute.cpu_affinity_offset);
  apply_env_override_bool("DAGFORGE_WORKFLOW_ENABLED", cfg.workflow.enabled);

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
  apply_env_override("DAGFORGE_SANDBOX_MAX_PROCESSES",
                     cfg.sandbox.max_processes);
  apply_env_override("DAGFORGE_SANDBOX_MAX_OPEN_FILES",
                     cfg.sandbox.max_open_files);

  if (cfg.compute.threads < 0 || cfg.compute.queue_capacity <= 0 ||
      cfg.compute.cpu_affinity_offset < 0 ||
      cfg.runtime.shards < 0 || cfg.runtime.cpu_affinity_offset < 0 ||
      cfg.sandbox.minijail_path.empty() ||
      cfg.sandbox.seccomp_bpf_path.empty() ||
      cfg.sandbox.workspace_root.empty() ||
      cfg.sandbox.max_memory_bytes == 0 || cfg.sandbox.max_file_bytes == 0 ||
      cfg.sandbox.tmp_bytes == 0 || cfg.sandbox.max_processes == 0 ||
      cfg.sandbox.max_open_files == 0) {
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
