#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executor/executor.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <format>
#include <memory>
#include <string>
#include <system_error>

#include <unistd.h>

namespace dagforge::test {
namespace {

namespace fs = std::filesystem;

[[nodiscard]] auto configured_path(const char *environment,
                                   std::string_view relative) -> std::string {
  if (const char *value = std::getenv(environment);
      value != nullptr && *value != '\0') {
    return value;
  }
  const char *home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
    return {};
  }
  return (fs::path(home) / relative).string();
}

class CommandExecutorTest : public ::testing::Test {
protected:
  void SetUp() override {
    sandbox_.minijail_path = configured_path(
        "DAGFORGE_TEST_MINIJAIL", ".local/libexec/dagforge/minijail/minijail0");
    sandbox_.seccomp_bpf_path = configured_path(
        "DAGFORGE_TEST_SECCOMP_BPF",
        ".local/libexec/dagforge/minijail/dagforge_command.bpf");
    const char *home = std::getenv("HOME");
    ASSERT_NE(home, nullptr);
    sandbox_.workspace_root =
        (fs::path(home) / ".cache" / "dagforge" / "tests" /
         std::format("command-executor-{}", ::getpid()))
            .string();

    std::error_code error;
    fs::remove_all(sandbox_.workspace_root, error);
    ASSERT_TRUE(runtime_.start().has_value());
    executor_ = create_command_executor(runtime_, sandbox_);
    ASSERT_NE(executor_, nullptr);
  }

  void TearDown() override {
    executor_.reset();
    runtime_.stop();
    std::error_code error;
    fs::remove_all(sandbox_.workspace_root, error);
  }

  [[nodiscard]] auto sandbox_available() const -> bool {
    std::error_code error;
    return fs::is_regular_file(sandbox_.minijail_path, error) &&
           fs::is_regular_file(sandbox_.seccomp_bpf_path, error);
  }

  [[nodiscard]] auto run(CommandExecutorConfig command,
                         std::chrono::seconds timeout =
                             std::chrono::seconds(5))
      -> Result<ExecutorResult> {
    const auto instance =
        InstanceId{std::format("command-test-{}", next_instance_++)};
    return sync_wait_on_runtime(
        runtime_, execute_async(runtime_, *executor_, instance,
                                std::move(command), {}, {}, {}, {}, timeout));
  }

  [[nodiscard]] auto workspace(std::size_t instance) const -> fs::path {
    return fs::path(sandbox_.workspace_root) /
           std::format("command-test-{}", instance);
  }

  Runtime runtime_{1, false, 0,
                   ComputePoolConfig{.thread_count = 1, .queue_capacity = 8}};
  SandboxConfig sandbox_;
  std::unique_ptr<IExecutor> executor_;
  std::size_t next_instance_{0};
};

TEST_F(CommandExecutorTest, RunsInsideWritableWorkspace) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandExecutorConfig{
      .program = "/bin/sh",
      .arguments = {"-c",
                    "grep -Eq '^NoNewPrivs:[[:space:]]+1$' "
                    "/proc/self/status && printf artifact > output.txt && "
                    "printf sandbox-ok"},
  });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->exit_code, 0);
  EXPECT_EQ(result->stdout_output, "sandbox-ok");
  EXPECT_TRUE(result->stderr_output.empty());

  std::ifstream output(workspace(0) / "output.txt");
  ASSERT_TRUE(output.good());
  std::string contents;
  output >> contents;
  EXPECT_EQ(contents, "artifact");
}

TEST_F(CommandExecutorTest, DeniesHostFilesOutsideAllowlist) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandExecutorConfig{
      .program = "/bin/cat",
      .arguments = {"/etc/hostname"},
  });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_NE(result->exit_code, 0);
  EXPECT_TRUE(result->stdout_output.empty());
}

TEST_F(CommandExecutorTest, DeniesExternalNetwork) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandExecutorConfig{
      .program = "/usr/bin/python3",
      .arguments = {
          "-c",
          "import socket; socket.create_connection(('1.1.1.1', 53), 0.2)"},
  });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_NE(result->exit_code, 0);
}

TEST_F(CommandExecutorTest, RejectsRelativeProgramAndReservedEnvironment) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto relative = run(CommandExecutorConfig{.program = "sh"});
  ASSERT_FALSE(relative.has_value());
  EXPECT_EQ(relative.error(), make_error_code(Error::Unauthorized));

  CommandExecutorConfig reserved{.program = "/bin/true"};
  reserved.env.emplace("PATH", "/tmp");
  auto reserved_result = run(std::move(reserved));
  ASSERT_FALSE(reserved_result.has_value());
  EXPECT_EQ(reserved_result.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(CommandExecutorTest, EnforcesWallTimeout) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandExecutorConfig{
                        .program = "/bin/sh",
                        .arguments = {"-c", "sleep 5"},
                    },
                    std::chrono::seconds(1));

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->timed_out);
  EXPECT_EQ(result->exit_code, kExitCodeTimeout);
}

} // namespace
} // namespace dagforge::test
