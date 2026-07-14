#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include <cstdint>
#include <string>
#include <vector>
#endif

namespace dagforge::config {

struct CommandProgramConfig {
  std::string name;
  std::string path;

  auto operator==(const CommandProgramConfig &) const -> bool = default;
};

struct CommandPolicyConfig {
  bool allow_unlisted_programs{false};
  bool allow_unlisted_environment{false};
  bool require_trusted_programs{true};
  std::vector<CommandProgramConfig> programs;
  std::vector<std::string> allowed_programs;
  std::vector<std::string> allowed_environment;
  std::vector<std::string> inherited_environment{"LANG", "LC_ALL", "LC_CTYPE",
                                                 "TERM"};

  auto operator==(const CommandPolicyConfig &) const -> bool = default;
};

struct MinijailConfig {
  std::string executable{"~/.local/libexec/dagforge/minijail/minijail0"};
  std::string seccomp_bpf_path{
      "~/.local/libexec/dagforge/minijail/dagforge_command.bpf"};
  std::string execution_root{"./executions"};
  std::uint64_t max_memory_bytes{1024ULL * 1024ULL * 1024ULL};
  std::uint64_t max_file_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t tmp_bytes{64ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stdout_bytes{10ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stderr_bytes{10ULL * 1024ULL * 1024ULL};
  std::uint64_t max_stream_line_bytes{64ULL * 1024ULL};
  std::uint32_t max_processes{128};
  std::uint32_t max_open_files{256};
  bool require_trusted_files{true};
  bool retain_workdirs{false};

  auto operator==(const MinijailConfig &) const -> bool = default;
};

struct CommandExecutorConfig {
  CommandPolicyConfig policy;
  MinijailConfig minijail;

  auto operator==(const CommandExecutorConfig &) const -> bool = default;
};

} // namespace dagforge::config
