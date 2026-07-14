#include "dagforge/config/system_config_loader.hpp"

#include "gtest/gtest.h"

#include <cstdlib>

using namespace dagforge;
using namespace dagforge::config;

TEST(ConfigTest, RuntimeDefaults) {
  RuntimeConfig cfg;
  EXPECT_EQ(cfg.shards, 0);
  EXPECT_FALSE(cfg.pin_shards_to_cores);
  EXPECT_EQ(cfg.cpu_affinity_offset, 0);
}

TEST(ConfigTest, CommandExecutorDefaults) {
  CommandExecutorConfig cfg;
  EXPECT_EQ(cfg.minijail.executable,
            "~/.local/libexec/dagforge/minijail/minijail0");
  EXPECT_EQ(cfg.minijail.seccomp_bpf_path,
            "~/.local/libexec/dagforge/minijail/dagforge_command.bpf");
  EXPECT_EQ(cfg.minijail.execution_root, "./executions");
  EXPECT_EQ(cfg.minijail.max_memory_bytes, 1024ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(cfg.minijail.tmp_bytes, 64ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(cfg.minijail.max_stdout_bytes, 10ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(cfg.minijail.max_stream_line_bytes, 64ULL * 1024ULL);
  EXPECT_FALSE(cfg.policy.allow_unlisted_programs);
  EXPECT_FALSE(cfg.policy.allow_unlisted_environment);
  EXPECT_TRUE(cfg.policy.require_trusted_programs);
  EXPECT_TRUE(cfg.minijail.require_trusted_files);
  EXPECT_FALSE(cfg.minijail.retain_workdirs);
  EXPECT_EQ(cfg.policy.inherited_environment,
            (std::vector<std::string>{"LANG", "LC_ALL", "LC_CTYPE", "TERM"}));
}

TEST(ConfigTest, ApiDefaults) {
  ApiConfig cfg;
  EXPECT_FALSE(cfg.enabled);
  EXPECT_EQ(cfg.host, "127.0.0.1");
  EXPECT_EQ(cfg.port, 8888);
}

TEST(ConfigTest, HttpExecutorDefaultsAreDenyByDefault) {
  HttpExecutorConfig cfg;
  EXPECT_FALSE(cfg.enabled);
  EXPECT_FALSE(cfg.egress.allow_plaintext);
  EXPECT_TRUE(cfg.egress.deny_private_networks);
  EXPECT_TRUE(cfg.egress.allowed_origins.empty());
  EXPECT_TRUE(cfg.egress.allowed_ip_cidrs.empty());
  EXPECT_EQ(cfg.egress.max_request_headers, 64U);
  EXPECT_EQ(cfg.egress.max_request_header_bytes, 64ULL * 1024ULL);
  EXPECT_EQ(cfg.egress.max_request_body_bytes, 1024ULL * 1024ULL);
  EXPECT_EQ(cfg.egress.max_response_headers, 128U);
  EXPECT_EQ(cfg.egress.max_response_header_bytes, 64ULL * 1024ULL);
  EXPECT_EQ(cfg.egress.max_response_body_bytes, 10ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(cfg.egress.max_concurrent_requests_per_shard, 32U);
  EXPECT_EQ(cfg.egress.max_concurrent_requests, 256U);
  EXPECT_EQ(cfg.egress.dns_timeout_ms, 5000U);
  EXPECT_EQ(cfg.egress.connect_timeout_ms, 10000U);
  EXPECT_EQ(cfg.egress.tls_handshake_timeout_ms, 10000U);
  EXPECT_EQ(cfg.egress.write_timeout_ms, 30000U);
  EXPECT_EQ(cfg.egress.first_byte_timeout_ms, 30000U);
  EXPECT_EQ(cfg.egress.read_timeout_ms, 30000U);
  EXPECT_EQ(cfg.egress.idle_connection_timeout_ms, 30000U);
  EXPECT_EQ(cfg.egress.max_idle_connections_per_origin, 4U);
  EXPECT_EQ(cfg.egress.max_idle_connections_per_shard, 32U);
  EXPECT_EQ(cfg.egress.tls_min_version, "1.2");
}

TEST(ConfigTest, LoadFromTomlString) {
  std::string toml = R"(
[runtime]
shards = 2
pin_shards_to_cores = true
cpu_affinity_offset = 1

[sandbox]
minijail_path = "/opt/dagforge/minijail0"
seccomp_bpf_path = "/opt/dagforge/dagforge_command.bpf"
execution_root = "/var/lib/dagforge/executions"
max_memory_bytes = 536870912
max_file_bytes = 33554432
tmp_bytes = 16777216
max_stdout_bytes = 1048576
max_stderr_bytes = 2097152
max_stream_line_bytes = 4096
max_processes = 64
max_open_files = 128
allow_unlisted_programs = false
allow_unlisted_environment = false
require_trusted_files = true
retain_workdirs = true
programs = [{ name = "echo", path = "/bin/echo" }]
allowed_programs = ["/bin/echo"]
allowed_environment = ["DAGFORGE_INPUT"]
inherited_environment = ["LANG", "TERM"]

[http_executor]
enabled = true
allow_plaintext = true
deny_private_networks = true
allowed_origins = ["http://127.0.0.1:8081", "https://example.com"]
allowed_ip_cidrs = ["127.0.0.0/8"]
max_request_headers = 12
max_request_header_bytes = 1024
max_request_body_bytes = 2048
max_response_headers = 10
max_response_header_bytes = 4096
max_response_body_bytes = 8192
max_concurrent_requests_per_shard = 3
max_concurrent_requests = 5
dns_timeout_ms = 101
connect_timeout_ms = 102
tls_handshake_timeout_ms = 103
write_timeout_ms = 104
first_byte_timeout_ms = 105
read_timeout_ms = 106
idle_connection_timeout_ms = 107
max_idle_connections_per_origin = 2
max_idle_connections_per_shard = 6
tls_min_version = "1.3"
tls_ca_file = "/opt/dagforge/ca.pem"
tls_client_cert_file = "/opt/dagforge/client.pem"
tls_client_key_file = "/opt/dagforge/client.key"

[admission]
allow_unlisted_executors = false
allowed_executors = ["command"]
max_nodes = 64
max_parallel_nodes = 8
max_total_output_bytes = 1048576
max_run_duration_sec = 60

[storage]
enabled = true
directory = "/tmp/dagforge-test-state"
max_completed_runs = 20
max_evidence_records = 200

[api]
enabled = true
port = 9999
host = "0.0.0.0"
tls_min_version = "1.3"
bearer_token_env = "DAGFORGE_TEST_TOKEN"
max_request_header_bytes = 2048
max_request_body_bytes = 4096
connection_idle_timeout_ms = 1500
max_connections = 9
max_requests_per_connection = 4
max_concurrent_requests = 7
)";

  auto result = SystemConfigLoader::load_from_string(toml);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_TRUE(result->workflow.enabled);
  EXPECT_EQ(result->runtime.shards, 2);
  EXPECT_TRUE(result->runtime.pin_shards_to_cores);
  EXPECT_EQ(result->runtime.cpu_affinity_offset, 1);
  EXPECT_EQ(result->executors.command.minijail.executable,
            "/opt/dagforge/minijail0");
  EXPECT_EQ(result->executors.command.minijail.max_memory_bytes, 536870912U);
  EXPECT_EQ(result->executors.command.minijail.max_processes, 64U);
  EXPECT_EQ(result->executors.command.minijail.max_stdout_bytes, 1048576U);
  EXPECT_EQ(result->executors.command.minijail.execution_root,
            "/var/lib/dagforge/executions");
  EXPECT_TRUE(result->executors.command.minijail.retain_workdirs);
  EXPECT_FALSE(result->executors.command.policy.allow_unlisted_programs);
  ASSERT_EQ(result->executors.command.policy.programs.size(), 1U);
  EXPECT_EQ(result->executors.command.policy.programs.front().name, "echo");
  EXPECT_EQ(result->executors.command.policy.programs.front().path,
            "/bin/echo");
  ASSERT_EQ(result->executors.command.policy.allowed_programs.size(), 1U);
  EXPECT_EQ(result->executors.command.policy.allowed_programs.front(),
            "/bin/echo");
  EXPECT_EQ(result->executors.command.policy.inherited_environment,
            (std::vector<std::string>{"LANG", "TERM"}));
  EXPECT_TRUE(result->executors.http.enabled);
  EXPECT_TRUE(result->executors.http.egress.allow_plaintext);
  EXPECT_TRUE(result->executors.http.egress.deny_private_networks);
  ASSERT_EQ(result->executors.http.egress.allowed_origins.size(), 2U);
  EXPECT_EQ(result->executors.http.egress.allowed_origins.front(),
            "http://127.0.0.1:8081");
  EXPECT_EQ(result->executors.http.egress.max_request_headers, 12U);
  EXPECT_EQ(result->executors.http.egress.max_request_header_bytes, 1024U);
  EXPECT_EQ(result->executors.http.egress.max_request_body_bytes, 2048U);
  EXPECT_EQ(result->executors.http.egress.max_response_headers, 10U);
  EXPECT_EQ(result->executors.http.egress.max_response_header_bytes, 4096U);
  EXPECT_EQ(result->executors.http.egress.max_response_body_bytes, 8192U);
  EXPECT_EQ(result->executors.http.egress.max_concurrent_requests_per_shard,
            3U);
  EXPECT_EQ(result->executors.http.egress.max_concurrent_requests, 5U);
  EXPECT_EQ(result->executors.http.egress.dns_timeout_ms, 101U);
  EXPECT_EQ(result->executors.http.egress.connect_timeout_ms, 102U);
  EXPECT_EQ(result->executors.http.egress.tls_handshake_timeout_ms, 103U);
  EXPECT_EQ(result->executors.http.egress.write_timeout_ms, 104U);
  EXPECT_EQ(result->executors.http.egress.first_byte_timeout_ms, 105U);
  EXPECT_EQ(result->executors.http.egress.read_timeout_ms, 106U);
  EXPECT_EQ(result->executors.http.egress.idle_connection_timeout_ms, 107U);
  EXPECT_EQ(result->executors.http.egress.max_idle_connections_per_origin, 2U);
  EXPECT_EQ(result->executors.http.egress.max_idle_connections_per_shard, 6U);
  EXPECT_EQ(result->executors.http.egress.tls_min_version, "1.3");
  EXPECT_FALSE(result->admission.allow_unlisted_executors);
  ASSERT_EQ(result->admission.allowed_executors.size(), 1U);
  EXPECT_EQ(result->admission.allowed_executors.front(), "command");
  EXPECT_EQ(result->admission.max_parallel_nodes, 8U);
  EXPECT_TRUE(result->storage.enabled);
  EXPECT_EQ(result->storage.directory, "/tmp/dagforge-test-state");
  EXPECT_EQ(result->storage.max_completed_runs, 20U);
  EXPECT_EQ(result->storage.max_evidence_records, 200U);
  EXPECT_TRUE(result->api.enabled);
  EXPECT_EQ(result->api.port, 9999);
  EXPECT_EQ(result->api.bearer_token_env, "DAGFORGE_TEST_TOKEN");
  EXPECT_EQ(result->api.max_request_body_bytes, 4096U);
  EXPECT_EQ(result->api.max_request_header_bytes, 2048U);
  EXPECT_EQ(result->api.connection_idle_timeout_ms, 1500U);
  EXPECT_EQ(result->api.max_connections, 9U);
  EXPECT_EQ(result->api.max_requests_per_connection, 4U);
  EXPECT_EQ(result->api.max_concurrent_requests, 7U);
}

TEST(ConfigTest, RejectsInvalidCommandExecutorConfiguration) {
  auto missing_helper = SystemConfigLoader::load_from_string(R"(
[sandbox]
minijail_path = ""
)");
  ASSERT_FALSE(missing_helper.has_value());
  EXPECT_EQ(missing_helper.error(), make_error_code(Error::ParseError));

  auto empty_limit = SystemConfigLoader::load_from_string(R"(
[sandbox]
max_memory_bytes = 0
)");
  ASSERT_FALSE(empty_limit.has_value());
  EXPECT_EQ(empty_limit.error(), make_error_code(Error::ParseError));
}

TEST(ConfigTest, LegacyWorkspaceKeysRemainSupported) {
  auto result = SystemConfigLoader::load_from_string(R"(
[sandbox]
workspace_root = "/var/lib/dagforge/legacy-workspaces"
retain_workspaces = true
)");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->executors.command.minijail.execution_root,
            "/var/lib/dagforge/legacy-workspaces");
  EXPECT_TRUE(result->executors.command.minijail.retain_workdirs);
}

TEST(ConfigTest, RejectsInvalidHttpExecutorLimits) {
  auto result = SystemConfigLoader::load_from_string(R"(
[http_executor]
max_response_body_bytes = 0
)");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::ParseError));

  auto invalid_timeout = SystemConfigLoader::load_from_string(R"(
[http_executor]
first_byte_timeout_ms = 0
)" );
  ASSERT_FALSE(invalid_timeout.has_value());

  auto invalid_pool = SystemConfigLoader::load_from_string(R"(
[http_executor]
max_idle_connections_per_origin = 8
max_idle_connections_per_shard = 4
)" );
  ASSERT_FALSE(invalid_pool.has_value());
}

TEST(ConfigTest, RejectsIncompleteTlsIdentityAndInvalidTlsVersion) {
  auto incomplete_identity = SystemConfigLoader::load_from_string(R"(
[http_executor]
tls_client_cert_file = "/tmp/client.pem"
)" );
  ASSERT_FALSE(incomplete_identity.has_value());

  auto invalid_version = SystemConfigLoader::load_from_string(R"(
[api]
tls_min_version = "1.1"
)" );
  ASSERT_FALSE(invalid_version.has_value());
}

TEST(ConfigTest, EnvironmentOverridesTakePrecedence) {
  constexpr auto *kApiPort = "DAGFORGE_API_PORT";
  constexpr auto *kRuntimeShards = "DAGFORGE_RUNTIME_SHARDS";
  constexpr auto *kLegacySandboxRoot = "DAGFORGE_SANDBOX_WORKSPACE_ROOT";
  constexpr auto *kSandboxRoot = "DAGFORGE_SANDBOX_EXECUTION_ROOT";

  ::setenv(kApiPort, "7777", 1);
  ::setenv(kRuntimeShards, "3", 1);
  ::setenv(kLegacySandboxRoot, "/tmp/dagforge-legacy-executions", 1);
  ::setenv(kSandboxRoot, "/tmp/dagforge-test-executions", 1);

  std::string toml = R"(
[api]
port = 8080
)";

  auto result = SystemConfigLoader::load_from_string(toml);

  ::unsetenv(kApiPort);
  ::unsetenv(kRuntimeShards);
  ::unsetenv(kLegacySandboxRoot);
  ::unsetenv(kSandboxRoot);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->api.port, 7777);
  EXPECT_EQ(result->runtime.shards, 3);
  EXPECT_EQ(result->executors.command.minijail.execution_root,
            "/tmp/dagforge-test-executions");
}
