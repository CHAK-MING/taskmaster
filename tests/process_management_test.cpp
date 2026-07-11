#include "dagforge/executor/process_management.hpp"
#include "test_utils.hpp"

#include <gtest/gtest.h>

#include <boost/process/v2/process.hpp>

#include <chrono>
#include <csignal>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

using namespace dagforge;
using namespace std::chrono_literals;

TEST(ProcessManagementTest, KillProcessGroupOrProcessKillsEntireProcessGroup) {
  const auto child = ::fork();
  ASSERT_GE(child, 0);

  if (child == 0) {
    (void)::setpgid(0, 0);
    const auto grandchild = ::fork();
    if (grandchild == 0) {
      ::pause();
      _exit(0);
    }
    ::pause();
    _exit(0);
  }

  ASSERT_EQ(::setpgid(child, child), 0);
  std::this_thread::sleep_for(100ms);

  kill_process_group_or_process(child);

  int status = 0;
  ASSERT_EQ(::waitpid(child, &status, 0), child);
  EXPECT_TRUE(WIFSIGNALED(status));
  EXPECT_EQ(WTERMSIG(status), SIGKILL);
}

TEST(ProcessManagementTest, WaitResultDefaultsToNoError) {
  ProcessWaitResult result;

  EXPECT_EQ(result.exit_code, -1);
  EXPECT_FALSE(result.timed_out);
  EXPECT_FALSE(result.error);
}

TEST(ProcessManagementTest, TerminateAndReapProcessMarksTimeout) {
  io::IoContext io;
  boost::process::v2::process proc(io, "/bin/sh", {"-c", "sleep 30"});

  Result<ProcessWaitResult> result = fail(Error::Unknown);
  std::exception_ptr eptr;
  boost::asio::co_spawn(
      io,
      [&]() -> task<void> {
        result = co_await terminate_and_reap_process(proc, true);
        co_return;
      },
      [&](std::exception_ptr e) { eptr = e; });
  (void)io.run_for(std::chrono::seconds(5));

  ASSERT_FALSE(eptr);
  ASSERT_TRUE(result);
  EXPECT_TRUE(result->timed_out);
}
