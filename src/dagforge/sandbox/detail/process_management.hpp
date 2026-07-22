#pragma once

#include "dagforge/core/asio_awaitable.hpp"
#include "dagforge/core/coroutine.hpp"

#include <boost/process/v2/process.hpp>

#include <cerrno>
#include <csignal>
#include <system_error>

#include <sys/types.h>
#include <unistd.h>

namespace dagforge::sandbox {

namespace bp = boost::process::v2;

struct ProcessWaitResult {
  int exit_code{-1};
  bool timed_out{false};
  std::error_code error{};
};

inline auto kill_process_group_or_process(pid_t pid) noexcept -> void {
  if (pid <= 0) {
    return;
  }
  if (::kill(-pid, SIGKILL) == 0) {
    return;
  }
  if (errno != ESRCH) {
    (void)::kill(pid, SIGKILL);
    return;
  }
  (void)::kill(pid, SIGKILL);
}

inline auto terminate_process_group_or_process(bp::process &proc) noexcept
    -> void {
  // Kill the process group while its leader identity is still authoritative.
  // Terminating only the leader first creates a race in which descendants can
  // outlive the process object and escape the subsequent reap.
  kill_process_group_or_process(proc.id());
  boost::system::error_code ignored;
  if (proc.running(ignored)) {
    proc.terminate(ignored);
  }
}

[[nodiscard]] inline auto reap_process(bp::process &proc)
    -> task<Result<ProcessWaitResult>> {
  auto wait_res = co_await co_as_result(proc.async_wait(use_nothrow));
  if (!wait_res) {
    co_return fail(wait_res.error());
  }
  co_return ok(ProcessWaitResult{.exit_code = *wait_res});
}

[[nodiscard]] inline auto terminate_and_reap_process(bp::process &proc,
                                                     bool timed_out = false)
    -> task<Result<ProcessWaitResult>> {
  terminate_process_group_or_process(proc);
  auto result_res = co_await reap_process(proc);
  if (!result_res) {
    co_return fail(result_res.error());
  }
  auto result = std::move(*result_res);
  result.timed_out = timed_out;
  co_return ok(std::move(result));
}

} // namespace dagforge::sandbox
