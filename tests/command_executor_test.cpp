#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/executor/command_executor.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <format>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

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
    sandbox_.retain_workspaces = true;
    sandbox_.allowed_programs = {
        "/bin/cat", "/bin/sh", "/bin/true", "/usr/bin/python3"};
    sandbox_.allowed_environment = {"PATH"};

    std::error_code error;
    fs::remove_all(sandbox_.workspace_root, error);
    ASSERT_TRUE(runtime_.start().has_value());
    (void)recreate_executor();
  }

  void TearDown() override {
    executor_.reset();
    runtime_.stop();
    std::error_code error;
    fs::remove_all(sandbox_.workspace_root, error);
  }

  [[nodiscard]] auto sandbox_available() const -> bool {
    std::error_code error;
    return executor_ != nullptr &&
           fs::is_regular_file(sandbox_.minijail_path, error) &&
           fs::is_regular_file(sandbox_.seccomp_bpf_path, error);
  }

  [[nodiscard]] auto run(CommandSpec command,
                         std::chrono::seconds timeout =
                             std::chrono::seconds(5))
      -> Result<CommandExecutionResult> {
    const auto instance =
        InstanceId{std::format("command-test-{}", next_instance_++)};
    return sync_wait_on_runtime(
        runtime_, execute_command_async(*executor_, instance,
                                        std::move(command), {}, {}, {}, {},
                                        timeout));
  }

  [[nodiscard]] auto recreate_executor() -> Result<void> {
    if (executor_) {
      executor_->shutdown();
      executor_.reset();
    }
    auto created = create_command_executor(runtime_, sandbox_);
    if (!created) {
      return fail(created.error());
    }
    executor_ = std::move(*created);
    return ok();
  }

  [[nodiscard]] auto workspace(std::size_t instance) const -> fs::path {
    return fs::path(sandbox_.workspace_root) /
           std::format("command-test-{}", instance);
  }

  Runtime runtime_{1, false, 0};
  SandboxConfig sandbox_;
  std::unique_ptr<ICommandExecutor> executor_;
  std::size_t next_instance_{0};
};

TEST_F(CommandExecutorTest, RunsInsideWritableWorkspace) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandSpec{
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

TEST_F(CommandExecutorTest, ReportsRunningAfterSandboxLaunch) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  std::vector<std::string> states;
  const auto instance =
      InstanceId{std::format("command-test-{}", next_instance_++)};
  auto result = sync_wait_on_runtime(
      runtime_, execute_command_async(
                    *executor_, instance,
                    CommandSpec{.program = "/bin/true"}, {}, {}, {},
                    {}, std::chrono::seconds(5),
                    [&states](std::string_view state) {
                      states.emplace_back(state);
                    }));

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(states.size(), 1U);
  EXPECT_EQ(states.front(), "running");
}

TEST_F(CommandExecutorTest, DeniesHostFilesOutsideAllowlist) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandSpec{
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

  auto result = run(CommandSpec{
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

  auto relative = run(CommandSpec{.program = "sh"});
  ASSERT_FALSE(relative.has_value());
  EXPECT_EQ(relative.error(), make_error_code(Error::InvalidArgument));

  CommandSpec reserved{.program = "/bin/true"};
  reserved.environment.emplace("PATH", "/tmp");
  auto reserved_result = run(std::move(reserved));
  ASSERT_FALSE(reserved_result.has_value());
  EXPECT_EQ(reserved_result.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(CommandExecutorTest, LowLevelExecutorEnforcesProgramAllowlist) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  auto result = run(CommandSpec{.program = "/bin/echo"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::Unauthorized));
}

TEST_F(CommandExecutorTest, EnforcesWallTimeout) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto result = run(CommandSpec{
                        .program = "/bin/sh",
                        .arguments = {"-c", "sleep 5"},
                    },
                    std::chrono::seconds(1));

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->timed_out);
  EXPECT_EQ(result->exit_code, kExitCodeTimeout);
}

TEST_F(CommandExecutorTest, TerminatesProcessWhenOutputLimitIsExceeded) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  sandbox_.max_stdout_bytes = 1024;
  sandbox_.max_stream_line_bytes = 512;
  ASSERT_TRUE(recreate_executor().has_value());
  auto result = run(CommandSpec{
      .program = "/usr/bin/python3",
      .arguments = {"-c", "import sys; sys.stdout.write('x' * 4096)"},
  });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->resource_exhausted);
  EXPECT_NE(result->exit_code, 0);
}

TEST_F(CommandExecutorTest, ShutdownRejectsNewCommands) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  executor_->shutdown();
  auto result = run(CommandSpec{.program = "/bin/true"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::InvalidState));
}

TEST_F(CommandExecutorTest, ShutdownKillsAndReapsActiveProcessGroup) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  std::mutex result_mutex;
  std::optional<Result<CommandExecutionResult>> completion;
  std::jthread worker([&] {
    auto result = run(CommandSpec{
                          .program = "/bin/sh",
                          .arguments = {"-c", "sleep 30"},
                      },
                      std::chrono::seconds(60));
    std::lock_guard lock(result_mutex);
    completion.emplace(std::move(result));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const auto started = std::chrono::steady_clock::now();
  executor_->shutdown();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  worker.join();

  EXPECT_LT(elapsed, std::chrono::seconds(5));
  std::lock_guard lock(result_mutex);
  ASSERT_TRUE(completion.has_value());
  ASSERT_TRUE(completion->has_value()) << completion->error().message();
  EXPECT_NE((*completion)->exit_code, 0);
}

TEST_F(CommandExecutorTest, RejectsGroupWritableSeccompProgram) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  const auto unsafe_policy =
      fs::path(sandbox_.workspace_root).parent_path() / "unsafe-policy.bpf";
  std::error_code error;
  fs::copy_file(sandbox_.seccomp_bpf_path, unsafe_policy,
                fs::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error) << error.message();
  fs::permissions(unsafe_policy,
                  fs::perms::owner_read | fs::perms::owner_write |
                      fs::perms::group_write,
                  fs::perm_options::replace, error);
  ASSERT_FALSE(error) << error.message();

  auto config = sandbox_;
  config.seccomp_bpf_path = unsafe_policy.string();
  config.workspace_root =
      (fs::path(sandbox_.workspace_root).parent_path() / "unsafe-policy-work")
          .string();
  auto created = create_command_executor(runtime_, std::move(config));
  ASSERT_FALSE(created.has_value());
  EXPECT_EQ(created.error(), make_error_code(Error::Unauthorized));
  fs::remove(unsafe_policy, error);
}

} // namespace
} // namespace dagforge::test
