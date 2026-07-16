#include "dagforge/util/daemon.hpp"
#include "test_utils.hpp"

#include "gtest/gtest.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace dagforge;

namespace {

auto make_pid_file_path() -> std::string {
  std::string path = dagforge::test::make_temp_path("dagforge_pid_test_");
  if (!path.empty()) {
    ::unlink(path.c_str());
  }
  return path;
}

} // namespace

TEST(DaemonTest, PidFileGuardAcquireWritesPidAndRemovesOnRelease) {
  const auto path = make_pid_file_path();
  ASSERT_FALSE(path.empty());

  {
    auto guard = PidFileGuard::acquire(path);
    ASSERT_TRUE(guard.has_value()) << guard.error().message();

    auto pid = read_pid_file(path);
    ASSERT_TRUE(pid.has_value()) << pid.error().message();
    EXPECT_EQ(*pid, static_cast<std::int64_t>(::getpid()));
  }

  auto pid = read_pid_file(path);
  ASSERT_FALSE(pid.has_value());
  EXPECT_EQ(pid.error(), make_error_code(Error::FileNotFound));
}

TEST(DaemonTest, PidFileGuardCreatesParentAndExcludesSecondOwner) {
  const auto root = std::filesystem::path(
      dagforge::test::make_temp_path("dagforge_pid_parent_"));
  ASSERT_FALSE(root.empty());
  ::unlink(root.c_str());
  const auto path = root / "nested" / "service.pid";

  {
    auto first = PidFileGuard::acquire(path.string());
    ASSERT_TRUE(first.has_value()) << first.error().message();
    EXPECT_TRUE(std::filesystem::exists(path));

    auto second = PidFileGuard::acquire(path.string());
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), make_error_code(Error::AlreadyExists));

    auto moved = std::move(*first);
    PidFileGuard assigned;
    assigned = std::move(moved);
    EXPECT_TRUE(std::filesystem::exists(path));

    auto *self = &assigned;
    assigned = std::move(*self);
  }
  EXPECT_FALSE(std::filesystem::exists(path));
  std::filesystem::remove_all(root);
}

TEST(DaemonTest, PidFileGuardRejectsEmptyPath) {
  auto guard = PidFileGuard::acquire("");
  ASSERT_FALSE(guard.has_value());
  EXPECT_EQ(guard.error(), make_error_code(Error::InvalidArgument));
}

TEST(DaemonTest, PidFileGuardRejectsParentBelowRegularFile) {
  const auto root = std::filesystem::path(
      dagforge::test::make_temp_path("dagforge_pid_parent_file_"));
  ASSERT_FALSE(root.empty());
  {
    std::ofstream output(root, std::ios::trunc);
    ASSERT_TRUE(output.is_open());
    output << "not a directory";
  }

  auto guard = PidFileGuard::acquire((root / "service.pid").string());
  EXPECT_FALSE(guard.has_value());
  std::filesystem::remove(root);
}

TEST(DaemonTest, ReadPidFileRejectsInvalidContent) {
  const auto path = make_pid_file_path();
  ASSERT_FALSE(path.empty());

  {
    std::ofstream out(path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << "not-a-pid\n";
  }

  auto pid = read_pid_file(path);
  ASSERT_FALSE(pid.has_value());
  EXPECT_EQ(pid.error(), make_error_code(Error::ParseError));

  ::unlink(path.c_str());
}

TEST(DaemonTest, ReadPidFileRejectsEmptyNonPositiveAndTrailingContent) {
  const auto path = make_pid_file_path();
  ASSERT_FALSE(path.empty());

  for (std::string_view content : {"", "0\n", "-1\n", "12 trailing\n"}) {
    std::ofstream out(path, std::ios::trunc);
    ASSERT_TRUE(out.is_open());
    out << content;
    out.close();
    auto pid = read_pid_file(path);
    ASSERT_FALSE(pid.has_value()) << content;
    EXPECT_EQ(pid.error(), make_error_code(Error::ParseError)) << content;
  }

  EXPECT_TRUE(remove_pid_file(path).has_value());
  EXPECT_TRUE(remove_pid_file(path).has_value());
}

TEST(DaemonTest, ProcessHelpersRejectInvalidOrMissingProcesses) {
  EXPECT_FALSE(is_process_alive(0));
  EXPECT_FALSE(is_process_alive(-1));
  EXPECT_TRUE(is_process_alive(::getpid()));

  auto invalid = send_signal(0, SIGTERM);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error(), make_error_code(Error::InvalidArgument));

  const auto missing = static_cast<std::int64_t>(1'000'000'000);
  auto sent = send_signal(missing, SIGTERM);
  EXPECT_FALSE(sent.has_value());
  EXPECT_TRUE(wait_for_process_exit(missing, std::chrono::milliseconds(1)));
}

TEST(DaemonTest, ZombieChildIsNotReportedAlive) {
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    _Exit(0);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(is_process_alive(child));
  int status = 0;
  EXPECT_EQ(::waitpid(child, &status, 0), child);
}

TEST(DaemonTest, SendSignalAndWaitForProcessExit) {
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);

  if (child == 0) {
    for (;;) {
      ::pause();
    }
  }

  ASSERT_TRUE(is_process_alive(child));
  auto sent = send_signal(child, SIGTERM);
  ASSERT_TRUE(sent.has_value()) << sent.error().message();
  EXPECT_TRUE(wait_for_process_exit(child, std::chrono::seconds(2)));
}

TEST(DaemonTest, WaitForProcessExitReportsLiveProcessAtDeadline) {
  const pid_t child = ::fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    for (;;) {
      ::pause();
    }
  }

  EXPECT_FALSE(wait_for_process_exit(child, std::chrono::milliseconds(20)));
  ASSERT_TRUE(send_signal(child, SIGTERM).has_value());
  int status = 0;
  EXPECT_EQ(::waitpid(child, &status, 0), child);
}

TEST(DaemonTest, WaitForShutdownReturnsAfterSignalHandlerRuns) {
  g_shutdown_requested.store(false, std::memory_order_release);
  setup_signal_handlers();

  std::promise<void> done;
  auto future = done.get_future();
  std::thread waiter([&done]() {
    wait_for_shutdown();
    done.set_value();
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  ASSERT_EQ(::kill(::getpid(), SIGTERM), 0);
  EXPECT_EQ(future.wait_for(std::chrono::seconds(2)),
            std::future_status::ready);

  waiter.join();
  g_shutdown_requested.store(false, std::memory_order_release);
}
