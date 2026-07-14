#include "dagforge/config/command_executor_config.hpp"
#include "dagforge/core/runtime.hpp"
#include "dagforge/core/sync_wait.hpp"
#include "dagforge/sandbox/command_runner.hpp"

#include "../src/dagforge/sandbox/detail/command_policy.hpp"
#include "../src/dagforge/sandbox/detail/minijail_command_runner.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <format>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <unistd.h>

namespace dagforge::test {
namespace {

using namespace dagforge::sandbox;
using namespace dagforge::config;

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

class CommandRunnerTest : public ::testing::Test {
protected:
  void SetUp() override {
    config_.minijail.executable = configured_path(
        "DAGFORGE_TEST_MINIJAIL", ".local/libexec/dagforge/minijail/minijail0");
    config_.minijail.seccomp_bpf_path = configured_path(
        "DAGFORGE_TEST_SECCOMP_BPF",
        ".local/libexec/dagforge/minijail/dagforge_command.bpf");
    const char *home = std::getenv("HOME");
    ASSERT_NE(home, nullptr);
    config_.minijail.execution_root =
        (fs::path(home) / ".cache" / "dagforge" / "tests" /
         std::format("command-executor-{}", ::getpid()))
            .string();
    config_.minijail.retain_workdirs = true;
    config_.policy.programs = {
        {.name = "cat", .path = "/bin/cat"},
        {.name = "sh", .path = "/bin/sh"},
        {.name = "true", .path = "/bin/true"},
        {.name = "python3", .path = "/usr/bin/python3"},
    };
    config_.policy.allowed_programs = {
        "/bin/cat", "/bin/sh", "/bin/true", "/usr/bin/python3"};
    config_.policy.inherited_environment = {};

    std::error_code error;
    fs::remove_all(config_.minijail.execution_root, error);
    ASSERT_TRUE(runtime_.start().has_value());
    (void)recreate_executor();
  }

  void TearDown() override {
    runner_.reset();
    runtime_.stop();
    std::error_code error;
    fs::remove_all(config_.minijail.execution_root, error);
  }

  [[nodiscard]] auto sandbox_available() const -> bool {
    std::error_code error;
    return runner_ != nullptr &&
           fs::is_regular_file(config_.minijail.executable, error) &&
           fs::is_regular_file(config_.minijail.seccomp_bpf_path, error);
  }

  [[nodiscard]] auto run(CommandSpec command,
                         std::chrono::seconds timeout =
                             std::chrono::seconds(5))
      -> Result<CommandRunResult> {
    const auto instance =
        InstanceId{std::format("command-test-{}", next_instance_++)};
    return sync_wait_on_runtime(
        runtime_, run_command_async(*runner_, instance, std::move(command), {},
                                    {}, {}, {}, timeout));
  }

  [[nodiscard]] auto recreate_executor() -> Result<void> {
    if (runner_) {
      (void)runner_->quiesce(std::chrono::seconds(5));
      runner_.reset();
    }
    auto policy = sandbox::detail::CommandPolicy::create(config_.policy);
    if (!policy) {
      return fail(policy.error());
    }
    auto created = sandbox::detail::create_minijail_command_runner(
        runtime_, config_.minijail, std::move(*policy));
    if (!created) {
      return fail(created.error());
    }
    runner_ = std::move(*created);
    return ok();
  }

  [[nodiscard]] auto workdir(std::size_t instance) const -> fs::path {
    return fs::path(config_.minijail.execution_root) /
           std::format("command-test-{}", instance);
  }

  Runtime runtime_{1, false, 0};
  CommandExecutorConfig config_;
  std::unique_ptr<ICommandRunner> runner_;
  std::size_t next_instance_{0};
};

TEST_F(CommandRunnerTest, RunsInsideWritableWorkdir) {
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

  std::ifstream output(workdir(0) / "output.txt");
  ASSERT_TRUE(output.good());
  std::string contents;
  output >> contents;
  EXPECT_EQ(contents, "artifact");
}

TEST_F(CommandRunnerTest, ReportsRunningAfterSandboxLaunch) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  std::vector<std::string> states;
  const auto instance =
      InstanceId{std::format("command-test-{}", next_instance_++)};
  auto result = sync_wait_on_runtime(
      runtime_, run_command_async(
                    *runner_, instance,
                    CommandSpec{.program = "/bin/true"}, {}, {}, {},
                    {}, std::chrono::seconds(5),
                    [&states](std::string_view state) {
                      states.emplace_back(state);
                    }));

  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(states.size(), 1U);
  EXPECT_EQ(states.front(), "running");
}

TEST_F(CommandRunnerTest, StreamsBothOutputsAndEmitsHeartbeats) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  std::vector<std::string> stdout_lines;
  std::vector<std::string> stderr_lines;
  std::atomic<unsigned> heartbeats{0};
  const auto instance =
      InstanceId{std::format("command-test-{}", next_instance_++)};
  auto result = sync_wait_on_runtime(
      runtime_, run_command_async(
                    *runner_, instance,
                    CommandSpec{
                        .program = "/bin/sh",
                        .arguments = {
                            "-c",
                            "printf 'out-one\\nout-tail'; "
                            "printf 'err-one\\nerr-tail' >&2; sleep 2"},
                    },
                    {},
                    [&stdout_lines](std::string_view line) {
                      stdout_lines.emplace_back(line);
                    },
                    [&stderr_lines](std::string_view line) {
                      stderr_lines.emplace_back(line);
                    },
                    [&heartbeats](const InstanceId &) {
                      heartbeats.fetch_add(1, std::memory_order_relaxed);
                    },
                    std::chrono::seconds(5)));

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->exit_code, 0);
  EXPECT_EQ(result->stdout_output, "out-one\nout-tail");
  EXPECT_EQ(result->stderr_output, "err-one\nerr-tail");
  EXPECT_TRUE(result->stdout_streamed);
  EXPECT_TRUE(result->stderr_streamed);
  EXPECT_EQ(stdout_lines,
            (std::vector<std::string>{"out-one", "out-tail"}));
  EXPECT_EQ(stderr_lines,
            (std::vector<std::string>{"err-one", "err-tail"}));
  EXPECT_GE(heartbeats.load(std::memory_order_relaxed), 2U);
}

TEST_F(CommandRunnerTest, RemovesWorkdirWhenRetentionIsDisabled) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  config_.minijail.retain_workdirs = false;
  ASSERT_TRUE(recreate_executor().has_value());
  auto result = run(CommandSpec{.program = "/bin/true"});
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->exit_code, 0);
  EXPECT_FALSE(fs::exists(workdir(0)));
}

TEST_F(CommandRunnerTest, RejectsUnsafeInstanceNamesAndExistingWorkdirs) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto unsafe = sync_wait_on_runtime(
      runtime_, run_command_async(*runner_, InstanceId{"../escape"},
                                  CommandSpec{.program = "/bin/true"}));
  ASSERT_FALSE(unsafe.has_value());
  EXPECT_EQ(unsafe.error(), make_error_code(Error::InvalidArgument));

  const InstanceId retained{"retained-instance"};
  auto first = sync_wait_on_runtime(
      runtime_, run_command_async(*runner_, retained.clone(),
                                  CommandSpec{.program = "/bin/true"}));
  ASSERT_TRUE(first.has_value()) << first.error().message();
  auto duplicate = sync_wait_on_runtime(
      runtime_, run_command_async(*runner_, retained.clone(),
                                  CommandSpec{.program = "/bin/true"}));
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error(), make_error_code(Error::AlreadyExists));
  runner_->cancel(InstanceId{"missing-instance"});
}

TEST_F(CommandRunnerTest, DeniesHostFilesOutsideAllowlist) {
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

TEST_F(CommandRunnerTest, DeniesHostPathsBesideExecutionRoot) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  const auto host_secret =
      fs::path(config_.minijail.execution_root).parent_path() /
      "dagforge-host-secret";
  std::error_code error;
  fs::create_directories(host_secret, error);
  ASSERT_FALSE(error) << error.message();
  {
    std::ofstream token(host_secret / "token");
    ASSERT_TRUE(token.good());
    token << "secret";
  }

  auto result = run(CommandSpec{
      .program = "sh",
      .arguments = {
          "-c",
          "if cat \"$1/token\" >/dev/null 2>&1; then exit 1; fi; "
          "printf host-unreadable",
          "sh", host_secret.string()},
  });

  fs::remove_all(host_secret, error);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->exit_code, 0);
  EXPECT_EQ(result->stdout_output, "host-unreadable");
}

TEST_F(CommandRunnerTest, DeniesExternalNetwork) {
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

TEST_F(CommandRunnerTest, ResolvesRegisteredProgramWithoutPathSearch) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  auto registered = run(CommandSpec{
      .program = "sh",
      .arguments = {"-c", "printf registered-program"},
  });
  ASSERT_TRUE(registered.has_value()) << registered.error().message();
  EXPECT_EQ(registered->exit_code, 0);
  EXPECT_EQ(registered->stdout_output, "registered-program");

  auto unregistered = run(CommandSpec{.program = "bash"});
  ASSERT_FALSE(unregistered.has_value());
  EXPECT_EQ(unregistered.error(), make_error_code(Error::Unauthorized));

  auto relative_path = run(CommandSpec{.program = "./sh"});
  ASSERT_FALSE(relative_path.has_value());
  EXPECT_EQ(relative_path.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(CommandRunnerTest, RejectsReservedEnvironment) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  CommandSpec reserved{.program = "/bin/true"};
  reserved.environment.emplace("PATH", "/tmp");
  auto reserved_result = run(std::move(reserved));
  ASSERT_FALSE(reserved_result.has_value());
  EXPECT_EQ(reserved_result.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(CommandRunnerTest, InheritsOnlyConfiguredNonSensitiveEnvironment) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed";
  }

  constexpr auto *kVisible = "DAGFORGE_TEST_VISIBLE";
  constexpr auto *kSecret = "DAGFORGE_TEST_TOKEN";
  ::setenv(kVisible, "visible", 1);
  ::setenv(kSecret, "secret", 1);
  config_.policy.inherited_environment = {kVisible};
  ASSERT_TRUE(recreate_executor().has_value());

  auto result = run(CommandSpec{
      .program = "sh",
      .arguments = {
          "-c",
          "test \"$DAGFORGE_TEST_VISIBLE\" = visible && "
          "test -z \"${DAGFORGE_TEST_TOKEN+x}\" && "
          "test \"$HOME\" = \"$PWD\" && printf environment-ok"},
  });

  ::unsetenv(kVisible);
  ::unsetenv(kSecret);

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->exit_code, 0);
  EXPECT_EQ(result->stdout_output, "environment-ok");
}

TEST_F(CommandRunnerTest, RejectsSensitiveHostEnvironmentInheritance) {
  config_.policy.inherited_environment = {"DAGFORGE_TEST_TOKEN"};
  auto recreated = recreate_executor();
  ASSERT_FALSE(recreated.has_value());
  EXPECT_EQ(recreated.error(), make_error_code(Error::InvalidArgument));
}

TEST_F(CommandRunnerTest, LowLevelRunnerEnforcesProgramAllowlist) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  auto result = run(CommandSpec{.program = "/bin/echo"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::Unauthorized));
}

TEST_F(CommandRunnerTest, EnforcesWallTimeout) {
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

TEST_F(CommandRunnerTest, TerminatesProcessWhenOutputLimitIsExceeded) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  config_.minijail.max_stdout_bytes = 1024;
  config_.minijail.max_stream_line_bytes = 512;
  ASSERT_TRUE(recreate_executor().has_value());
  auto result = run(CommandSpec{
      .program = "/usr/bin/python3",
      .arguments = {"-c", "import sys; sys.stdout.write('x' * 4096)"},
  });

  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_TRUE(result->resource_exhausted);
  EXPECT_NE(result->exit_code, 0);
}

TEST_F(CommandRunnerTest, QuiesceRejectsNewCommands) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  ASSERT_TRUE(runner_->quiesce(std::chrono::seconds(5)).has_value());
  auto result = run(CommandSpec{.program = "/bin/true"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error(), make_error_code(Error::InvalidState));
}

TEST_F(CommandRunnerTest, QuiesceKillsAndReapsActiveProcessGroup) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  std::mutex result_mutex;
  std::optional<Result<CommandRunResult>> completion;
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
  EXPECT_EQ(runner_->quiesce(std::chrono::milliseconds(0)).error(),
            make_error_code(Error::Timeout));
  EXPECT_TRUE(runner_->quiesce(std::chrono::seconds(5)).has_value());
  const auto elapsed = std::chrono::steady_clock::now() - started;
  worker.join();

  EXPECT_LT(elapsed, std::chrono::seconds(5));
  std::lock_guard lock(result_mutex);
  ASSERT_TRUE(completion.has_value());
  ASSERT_TRUE(completion->has_value()) << completion->error().message();
  EXPECT_NE((*completion)->exit_code, 0);
}

TEST_F(CommandRunnerTest, RejectsInvalidRunnerConfigurationAndTempRoot) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  EXPECT_EQ(sandbox::detail::create_minijail_command_runner(
                runtime_, config_.minijail, {})
                .error(),
            make_error_code(Error::InvalidArgument));

  auto policy = sandbox::detail::CommandPolicy::create(config_.policy);
  ASSERT_TRUE(policy.has_value()) << policy.error().message();
  auto invalid_limits = config_.minijail;
  invalid_limits.max_memory_bytes = 0;
  EXPECT_EQ(sandbox::detail::create_minijail_command_runner(
                runtime_, invalid_limits, *policy)
                .error(),
            make_error_code(Error::InvalidArgument));

  auto missing_helper = config_.minijail;
  missing_helper.executable = "/definitely/missing/minijail";
  EXPECT_FALSE(sandbox::detail::create_minijail_command_runner(
                   runtime_, missing_helper, *policy)
                   .has_value());

  auto missing_policy = config_.minijail;
  missing_policy.seccomp_bpf_path = "/definitely/missing/seccomp.bpf";
  EXPECT_FALSE(sandbox::detail::create_minijail_command_runner(
                   runtime_, missing_policy, *policy)
                   .has_value());

  auto temp_root = config_.minijail;
  temp_root.execution_root =
      (fs::temp_directory_path() /
       std::format("dagforge-invalid-root-{}", ::getpid()))
          .string();
  EXPECT_EQ(sandbox::detail::create_minijail_command_runner(
                runtime_, temp_root, *policy)
                .error(),
            make_error_code(Error::InvalidArgument));

  const auto real_root = fs::path(config_.minijail.execution_root).parent_path() /
                         "symlink-target";
  const auto symlink_root =
      fs::path(config_.minijail.execution_root).parent_path() / "symlink-root";
  std::error_code error;
  fs::remove_all(real_root, error);
  fs::remove(symlink_root, error);
  fs::create_directories(real_root, error);
  ASSERT_FALSE(error) << error.message();
  fs::create_directory_symlink(real_root, symlink_root, error);
  ASSERT_FALSE(error) << error.message();
  auto symlink_config = config_.minijail;
  symlink_config.execution_root = symlink_root.string();
  EXPECT_EQ(sandbox::detail::create_minijail_command_runner(
                runtime_, symlink_config, *policy)
                .error(),
            make_error_code(Error::Unauthorized));
  fs::remove(symlink_root, error);
  fs::remove_all(real_root, error);
}

TEST_F(CommandRunnerTest, RejectsGroupWritableSeccompProgram) {
  if (!sandbox_available()) {
    GTEST_SKIP() << "Minijail helper is not installed or unsupported";
  }

  const auto unsafe_policy =
      fs::path(config_.minijail.execution_root).parent_path() /
      "unsafe-policy.bpf";
  std::error_code error;
  fs::copy_file(config_.minijail.seccomp_bpf_path, unsafe_policy,
                fs::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error) << error.message();
  fs::permissions(unsafe_policy,
                  fs::perms::owner_read | fs::perms::owner_write |
                      fs::perms::group_write,
                  fs::perm_options::replace, error);
  ASSERT_FALSE(error) << error.message();

  auto config = config_;
  config.minijail.seccomp_bpf_path = unsafe_policy.string();
  config.minijail.execution_root =
      (fs::path(config_.minijail.execution_root).parent_path() /
       "unsafe-policy-work")
          .string();
  auto policy = sandbox::detail::CommandPolicy::create(config.policy);
  ASSERT_TRUE(policy.has_value()) << policy.error().message();
  auto created = sandbox::detail::create_minijail_command_runner(
      runtime_, config.minijail, std::move(*policy));
  ASSERT_FALSE(created.has_value());
  EXPECT_EQ(created.error(), make_error_code(Error::Unauthorized));
  fs::remove(unsafe_policy, error);
}

} // namespace
} // namespace dagforge::test
