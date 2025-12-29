#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/util/log.hpp"

#include <sys/syscall.h>
#include <sys/wait.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <unordered_map>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>

namespace taskmaster {

namespace {

// Configuration constants
inline constexpr std::size_t MAX_OUTPUT_SIZE = 10 * 1024 * 1024; 
inline constexpr std::size_t READ_BUFFER_SIZE = 4096;
inline constexpr std::size_t INITIAL_OUTPUT_RESERVE = 8192;

auto pidfd_open(pid_t pid, unsigned int flags) -> int {
  return static_cast<int>(syscall(SYS_pidfd_open, pid, flags));
}

auto create_pipe() -> std::pair<int, int> {
  int fds[2];
  if (pipe2(fds, O_CLOEXEC | O_NONBLOCK) < 0) {
    return {-1, -1};
  }
  return {fds[0], fds[1]};
}

auto fork_and_exec(const std::string& cmd, const std::string& working_dir,
                   int stdout_write_fd) -> pid_t {
  pid_t pid = vfork();
  if (pid < 0) {
    return -1;
  }

  if (pid == 0) {
    // Child process - must only use async-signal-safe functions
    setpgid(0, 0);

    dup2(stdout_write_fd, STDOUT_FILENO);
    dup2(stdout_write_fd, STDERR_FILENO);
    close(stdout_write_fd);

    if (!working_dir.empty()) {
      if (chdir(working_dir.c_str()) < 0) {
        _exit(127);
      }
    }

    execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr);
    _exit(127);
  }

  close(stdout_write_fd);
  setpgid(pid, pid);
  return pid;
}

auto get_exit_code(int status) -> int {
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    return 128 + WTERMSIG(status);
  }
  return -1;
}

auto read_output(int fd, std::chrono::seconds timeout,
                 std::chrono::steady_clock::time_point start)
    -> task<std::pair<std::string, bool>> {
  std::string output;
  output.reserve(INITIAL_OUTPUT_RESERVE);
  std::array<char, READ_BUFFER_SIZE> buffer;

  while (true) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > timeout) {
      co_return std::make_pair(std::move(output), true);
    }

    // Calculate remaining timeout
    auto remaining =
        timeout - std::chrono::duration_cast<std::chrono::seconds>(elapsed);
    auto remaining_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    if (remaining_ms.count() <= 0) {
      co_return std::make_pair(std::move(output), true);
    }

    // Poll with timeout to avoid blocking indefinitely
    auto poll_result = co_await async_poll_timeout(fd, POLLIN, remaining_ms);
    if (poll_result.timed_out) {
      co_return std::make_pair(std::move(output), true);
    }
    if (poll_result.error != std::errc{}) {
      break;
    }

    // Now read - data should be available
    ssize_t bytes_read = read(fd, buffer.data(), buffer.size());
    if (bytes_read < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        continue;
      }
      break;
    }
    if (bytes_read == 0) {
      break;
    }

    output.append(buffer.data(), static_cast<std::size_t>(bytes_read));
    if (output.size() >= MAX_OUTPUT_SIZE) {
      output.resize(MAX_OUTPUT_SIZE);
      break;
    }
  }

  co_return std::make_pair(std::move(output), false);
}

auto wait_process(int pidfd, pid_t pid, bool timed_out) -> task<int> {
  if (timed_out) {
    kill(-pid, SIGKILL);
  }

  if (pidfd >= 0) {
    (void)co_await async_poll(pidfd, POLLIN);
  }

  int status = 0;
  int wait_result = waitpid(pid, &status, WNOHANG);

  if (wait_result == 0) {
    kill(-pid, SIGKILL);
    (void)co_await async_sleep(std::chrono::milliseconds(10));
    wait_result = waitpid(pid, &status, WNOHANG);

    if (wait_result == 0) {
      waitpid(pid, &status, 0);
    }
  } else if (wait_result < 0) {
    log::warn("waitpid failed for pid {}: {}", pid, strerror(errno));
    co_return -1;
  }

  co_return get_exit_code(status);
}

struct ExecutionContext {
  std::mutex* mutex;
  std::unordered_map<std::string, pid_t, StringHash, std::equal_to<>>*
      active_processes;

  auto register_process(const std::string& id, pid_t pid) -> void {
    std::lock_guard lock(*mutex);
    (*active_processes)[id] = pid;
  }

  auto unregister_process(std::string_view id) -> void {
    std::lock_guard lock(*mutex);
    active_processes->erase(active_processes->find(id));
  }
};

auto execute_command(std::string cmd, std::string working_dir,
                     std::chrono::seconds timeout, std::string instance_id,
                     ExecutorCallback callback, ExecutionContext* ctx)
    -> spawn_task {
  ExecutorResult result;

  auto [read_fd, write_fd] = create_pipe();
  if (read_fd < 0) {
    result.error = "Failed to create pipe";
    result.exit_code = -1;
    callback(instance_id, std::move(result));
    co_return;
  }

  auto start = std::chrono::steady_clock::now();

  pid_t pid = fork_and_exec(cmd, working_dir, write_fd);
  if (pid < 0) {
    close(read_fd);
    result.error = "Failed to fork process";
    result.exit_code = -1;
    callback(instance_id, std::move(result));
    co_return;
  }

  ctx->register_process(instance_id, pid);

  int pidfd = pidfd_open(pid, 0);
  if (pidfd < 0) {
    log::warn("pidfd_open failed for pid {}", pid);
  }

  auto read_result = co_await read_output(read_fd, timeout, start);
  std::string output = std::move(read_result.first);
  bool timed_out = read_result.second;
  close(read_fd);

  int exit_code = -1;

  if (timed_out) {
    kill(-pid, SIGKILL);
  }

  if (pidfd >= 0) {
    exit_code = co_await wait_process(pidfd, pid, timed_out);
    close(pidfd);
  } else {
    int status = 0;
    if (waitpid(pid, &status, 0) > 0) {
      exit_code = get_exit_code(status);
    }
  }

  result.exit_code = exit_code;
  result.stdout_output = std::move(output);
  result.timed_out = timed_out;

  ctx->unregister_process(instance_id);

  callback(instance_id, std::move(result));
}

}  // namespace

class Executor : public IExecutor {
public:
  explicit Executor(Runtime& rt)
      : runtime_{&rt}, ctx_{&mutex_, &active_processes_} {
  }

  ~Executor() override = default;

  auto execute(const std::string& instance_id, const ExecutorConfig& config,
               ExecutorCallback callback) -> void override {
    const auto* shell = std::get_if<ShellExecutorConfig>(&config);
    if (!shell) {
      ExecutorResult result;
      result.exit_code = -1;
      result.error = "Invalid executor config";
      callback(instance_id, std::move(result));
      return;
    }

    auto t = execute_command(shell->command, shell->working_dir, shell->timeout,
                             instance_id, std::move(callback), &ctx_);
    runtime_->schedule_external(t.take());
  }

  auto cancel(std::string_view instance_id) -> void override {
    std::lock_guard lock(mutex_);
    auto it = active_processes_.find(instance_id);
    if (it != active_processes_.end()) {
      pid_t pid = it->second;
      if (pid > 0) {
        kill(-pid, SIGKILL);
      }
      log::info("Cancelled process for instance {}", instance_id);
    }
  }

private:
  Runtime* runtime_;
  std::mutex mutex_;
  std::unordered_map<std::string, pid_t, StringHash, std::equal_to<>>
      active_processes_;
  ExecutionContext ctx_;
};

auto create_shell_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<Executor>(rt);
}

}  // namespace taskmaster
