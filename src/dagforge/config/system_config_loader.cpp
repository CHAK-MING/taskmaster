#include "dagforge/config/system_config_loader.hpp"
#include "dagforge/config/toml_util.hpp"
#include "dagforge/util/log.hpp"


#include <boost/lexical_cast.hpp>
#include <cstdlib>
#include <string>
#include <string_view>


namespace dagforge {
namespace detail {

struct DAGSourceToml {
  std::string mode{"file"};
  std::string directory{"./dags"};
  int scan_interval_sec{30};
};

struct SystemToml {
  DatabaseConfig database{};
  ComputeConfig compute{};
  SchedulerConfig scheduler{};
  ApiConfig api{};
  DAGSourceToml dag_source{};
};

} // namespace detail
} // namespace dagforge

namespace glz {
template <> struct meta<dagforge::DatabaseConfig> {
  using T = dagforge::DatabaseConfig;
  static constexpr auto value =
      object("host", &T::host, "port", &T::port, "username", &T::username,
             "password", &T::password, "database", &T::database, "pool_size",
             &T::pool_size, "connect_timeout", &T::connect_timeout);
};

template <> struct meta<dagforge::ComputeConfig> {
  using T = dagforge::ComputeConfig;
  static constexpr auto value = object(
      "threads", &T::threads, "queue_capacity", &T::queue_capacity,
      "pin_threads_to_cores", &T::pin_threads_to_cores,
      "cpu_affinity_offset", &T::cpu_affinity_offset);
};

template <> struct meta<dagforge::SchedulerConfig> {
  using T = dagforge::SchedulerConfig;
  static constexpr auto value =
      object("log_level", &T::log_level, "log_file", &T::log_file, "pid_file",
             &T::pid_file, "tick_interval_ms", &T::tick_interval_ms,
             "max_concurrency", &T::max_concurrency, "shards", &T::shards,
             "scheduler_shards", &T::scheduler_shards,
             "pin_shards_to_cores", &T::pin_shards_to_cores,
             "cpu_affinity_offset", &T::cpu_affinity_offset,
             "zombie_reaper_interval_sec", &T::zombie_reaper_interval_sec,
             "zombie_heartbeat_timeout_sec", &T::zombie_heartbeat_timeout_sec);
};

template <> struct meta<dagforge::ApiConfig> {
  using T = dagforge::ApiConfig;
  static constexpr auto value = object(
      "enabled", &T::enabled, "port", &T::port, "host", &T::host, "reuse_port",
      &T::reuse_port, "tls_enabled", &T::tls_enabled, "tls_cert_file",
      &T::tls_cert_file, "tls_key_file", &T::tls_key_file);
};

template <> struct meta<dagforge::detail::DAGSourceToml> {
  using T = dagforge::detail::DAGSourceToml;
  static constexpr auto value =
      object("mode", &T::mode, "directory", &T::directory, "scan_interval_sec",
             &T::scan_interval_sec);
};

template <> struct meta<dagforge::detail::SystemToml> {
  using T = dagforge::detail::SystemToml;
  static constexpr auto value = object(
      "database", &T::database, "compute", &T::compute, "scheduler",
      &T::scheduler, "api", &T::api, "dag_source", &T::dag_source);
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
  cfg.database = std::move(raw.database);
  cfg.compute = std::move(raw.compute);
  cfg.scheduler = std::move(raw.scheduler);
  cfg.api = std::move(raw.api);

  cfg.dag_source.mode = parse<DAGSourceMode>(raw.dag_source.mode);
  cfg.dag_source.directory = std::move(raw.dag_source.directory);
  cfg.dag_source.scan_interval_sec = raw.dag_source.scan_interval_sec;

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

  apply_env_override("DAGFORGE_SCHEDULER_SHARDS", cfg.scheduler.shards);
  apply_env_override("DAGFORGE_SCHEDULER_MAX_CONCURRENCY",
                     cfg.scheduler.max_concurrency);
  apply_env_override("DAGFORGE_SCHEDULER_ENGINE_SHARDS",
                     cfg.scheduler.scheduler_shards);
  apply_env_override_bool("DAGFORGE_SCHEDULER_PIN_SHARDS",
                          cfg.scheduler.pin_shards_to_cores);
  apply_env_override("DAGFORGE_SCHEDULER_CPU_AFFINITY_OFFSET",
                     cfg.scheduler.cpu_affinity_offset);
  apply_env_override("DAGFORGE_ZOMBIE_REAPER_INTERVAL_SEC",
                     cfg.scheduler.zombie_reaper_interval_sec);
  apply_env_override("DAGFORGE_ZOMBIE_HEARTBEAT_TIMEOUT_SEC",
                     cfg.scheduler.zombie_heartbeat_timeout_sec);
  apply_env_override("DAGFORGE_LOG_LEVEL", cfg.scheduler.log_level);

  apply_env_override("DAGFORGE_DB_HOST", cfg.database.host);
  apply_env_override("DAGFORGE_DB_PORT", cfg.database.port);
  apply_env_override("DAGFORGE_DB_USERNAME", cfg.database.username);
  apply_env_override("DAGFORGE_DB_PASSWORD", cfg.database.password);
  apply_env_override("DAGFORGE_DB_DATABASE", cfg.database.database);
  apply_env_override("DAGFORGE_DB_POOL_SIZE", cfg.database.pool_size);
  apply_env_override("DAGFORGE_DB_CONNECT_TIMEOUT",
                     cfg.database.connect_timeout);

  apply_env_override("DAGFORGE_DAG_DIRECTORY", cfg.dag_source.directory);

  const bool reaper_disabled = cfg.scheduler.zombie_reaper_interval_sec == 0 &&
                               cfg.scheduler.zombie_heartbeat_timeout_sec == 0;
  const bool reaper_enabled_valid =
      cfg.scheduler.zombie_reaper_interval_sec > 0 &&
      cfg.scheduler.zombie_heartbeat_timeout_sec > 0 &&
      cfg.scheduler.zombie_heartbeat_timeout_sec >
          cfg.scheduler.zombie_reaper_interval_sec;

  if (cfg.compute.threads < 0 || cfg.compute.queue_capacity <= 0 ||
      cfg.compute.cpu_affinity_offset < 0 ||
      cfg.scheduler.tick_interval_ms <= 0 ||
      cfg.scheduler.max_concurrency <= 0 || cfg.scheduler.shards < 0 ||
      cfg.scheduler.scheduler_shards <= 0 ||
      cfg.scheduler.cpu_affinity_offset < 0 ||
      (!reaper_disabled && !reaper_enabled_valid)) {
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
