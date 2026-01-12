#include "taskmaster/core/coroutine.hpp"
#include "taskmaster/core/error.hpp"
#include "taskmaster/core/runtime.hpp"
#include "taskmaster/executor/executor.hpp"
#include "taskmaster/io/async_fd.hpp"
#include "taskmaster/io/context.hpp"
#include "taskmaster/util/log.hpp"

#include <sys/syscall.h>
#include <sys/wait.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <experimental/scope>
#include <unordered_map>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <unistd.h>

namespace taskmaster {

namespace {

inline constexpr std::size_t MAX_OUTPUT_SIZE = 10 * 1024 * 1024;
inline constexpr std::size_t READ_BUFFER_SIZE = 4096;
inline constexpr std::size_t INITIAL_OUTPUT_RESERVE = 8192;

[[nodiscard]] auto pidfd_open(pid_t pid, unsigned int flags) -> int {
  return static_cast<int>(syscall(SYS_pidfd_open, pid, flags));
}

struct ReadOutputResult {
  std::string output;
  bool timed_out = false;
};

auto finish(ExecutionSink& sink, const InstanceId& instance_id,
            ExecutorResult result) -> void {
  if (sink.on_complete) {
    sink.on_complete(instance_id, std::move(result));
  }
}

[[nodiscard]] auto fork_and_exec(const std::string& cmd, const std::string& working_dir,
                   int stdout_write_fd) -> pid_t {
  pid_t pid = vfork();
  if (pid < 0) {
    return -1;
  }

  if (pid == 0) {
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

[[nodiscard]] auto get_exit_code(int status) -> int {
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    return 128 + WTERMSIG(status);
  }
  return -1;
}

struct ReadChunkResult {
  ssize_t bytes = 0;
  bool timed_out = false;
  bool should_retry = false;
  bool error = false;
};

auto perform_read_uring(io::AsyncFd& fd, std::span<char> buffer, std::chrono::milliseconds timeout)
    -> task<ReadChunkResult> {
  auto poll_result = co_await fd.async_poll_timeout(POLLIN, timeout);
  if (poll_result.timed_out) {
    co_return ReadChunkResult{.timed_out = true};
  }
  if (poll_result.error) {
    co_return ReadChunkResult{.error = true};
  }

  auto read_result = co_await fd.async_read(io::buffer(buffer));
  if (!read_result) {
    if (read_result.error == std::errc::resource_unavailable_try_again ||
        read_result.error == std::errc::operation_would_block) {
      co_return ReadChunkResult{.should_retry = true};
    }
    co_return ReadChunkResult{.error = true};
  }
  co_return ReadChunkResult{.bytes = static_cast<ssize_t>(read_result.bytes_transferred)};
}

auto perform_read_posix(int fd, std::span<char> buffer, std::chrono::milliseconds timeout)
    -> ReadChunkResult {
  pollfd pfd{.fd = fd, .events = POLLIN};
  int pr = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
  
  if (pr == 0) {
    return ReadChunkResult{.timed_out = true};
  }
  if (pr < 0) {
    if (errno == EINTR) {
      return ReadChunkResult{.should_retry = true};
    }
    return ReadChunkResult{.error = true};
  }

  ssize_t n = ::read(fd, buffer.data(), buffer.size());
  if (n < 0) {
    if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
      return ReadChunkResult{.should_retry = true};
    }
    return ReadChunkResult{.error = true};
  }
  return ReadChunkResult{.bytes = n};
}

auto read_output(io::AsyncFd& fd, std::chrono::seconds timeout,
                 std::chrono::steady_clock::time_point start)
    -> task<ReadOutputResult> {
  ReadOutputResult out;
  out.output.reserve(INITIAL_OUTPUT_RESERVE);
  std::array<char, READ_BUFFER_SIZE> buffer;

  const bool io_uring_ok = fd.has_context() && fd.context().valid();
  if (!io_uring_ok) {
    log::warn("ShellExecutor: IoContext invalid; falling back to blocking poll/read");
  }

  while (true) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > timeout) {
      out.timed_out = true;
      co_return out;
    }

    auto remaining =
        timeout - std::chrono::duration_cast<std::chrono::seconds>(elapsed);
    auto remaining_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(remaining);
    if (remaining_ms.count() <= 0) {
      out.timed_out = true;
      co_return out;
    }

    ReadChunkResult res;
    if (io_uring_ok) {
      res = co_await perform_read_uring(fd, buffer, remaining_ms);
    } else {
      res = perform_read_posix(fd.fd(), buffer, remaining_ms);
    }

    if (res.timed_out) {
      out.timed_out = true;
      co_return out;
    }
    if (res.should_retry) {
      continue;
    }
    if (res.error) {
      break;
    }
    if (res.bytes == 0) {
      break;
    }

    out.output.append(buffer.data(), static_cast<std::size_t>(res.bytes));

    if (out.output.size() >= MAX_OUTPUT_SIZE) {
      out.output.resize(MAX_OUTPUT_SIZE);
      break;
    }
  }

  co_return out;
}



auto wait_process(io::AsyncFd& pidfd, pid_t pid, bool timed_out) -> task<int> {
  if (timed_out) {
    kill(-pid, SIGKILL);
  }

  if (pidfd.is_open()) {
    (void)co_await pidfd.async_poll(POLLIN);
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

[[nodiscard]] auto open_pidfd(io::IoContext& ctx, pid_t pid) -> io::AsyncFd {
  int pidfd = pidfd_open(pid, 0);
  if (pidfd < 0) {
    log::warn("pidfd_open failed for pid {}", pid);
    return io::AsyncFd{};
  }
  return io::AsyncFd::from_raw(ctx, pidfd, io::Ownership::Owned);
}

auto wait_for_exit(io::AsyncFd& pidfd, pid_t pid, bool timed_out)
    -> task<int> {
  if (pidfd.is_open() && pidfd.has_context() && pidfd.context().valid()) {
    co_return co_await wait_process(pidfd, pid, timed_out);
  }

  if (timed_out) {
    kill(-pid, SIGKILL);
  }

  int status = 0;
  if (waitpid(pid, &status, 0) > 0) {
    co_return get_exit_code(status);
  }
  co_return -1;
}

struct ExecutionContext {
  std::mutex* mutex;
  std::unordered_map<InstanceId, pid_t>* active_processes;

  auto register_process(const InstanceId& id, pid_t pid) -> void {
    std::lock_guard lock(*mutex);
    (*active_processes)[id] = pid;
  }

  auto unregister_process(const InstanceId& id) -> void {
    std::lock_guard lock(*mutex);
    auto it = active_processes->find(id);
    if (it != active_processes->end()) {
      active_processes->erase(it);
    }
  }
};

auto execute_command(std::string cmd, std::string working_dir,
                     std::chrono::seconds timeout, InstanceId instance_id,
                     ExecutionSink sink, ExecutionContext* ctx)
    -> spawn_task {
  ExecutorResult result;

  auto& io_ctx = current_io_context();
  auto pipe_result = io::AsyncPipe::create(io_ctx, O_CLOEXEC | O_NONBLOCK);
  if (!pipe_result) {
    result.error = "Failed to create pipe";
    result.exit_code = -1;
    finish(sink, instance_id, std::move(result));
    co_return;
  }
  auto pipe = std::move(*pipe_result);

  auto start = std::chrono::steady_clock::now();

  int write_fd = pipe.write_end.release();
  pid_t pid = fork_and_exec(cmd, working_dir, write_fd);
  if (pid < 0) {
    result.error = "Failed to fork process";
    result.exit_code = -1;
    finish(sink, instance_id, std::move(result));
    co_return;
  }

  ctx->register_process(instance_id, pid);
  std::experimental::scope_exit unregister{[&] { ctx->unregister_process(instance_id); }};

  auto pidfd = open_pidfd(io_ctx, pid);

  auto read_result = co_await read_output(pipe.read_end, timeout, start);
  std::string output = std::move(read_result.output);
  bool timed_out = read_result.timed_out;

  int exit_code = co_await wait_for_exit(pidfd, pid, timed_out);

  result.exit_code = exit_code;
  result.stdout_output = std::move(output);
  result.timed_out = timed_out;

  finish(sink, instance_id, std::move(result));
}

}  // namespace

class ShellExecutor : public IExecutor {
public:
  explicit ShellExecutor(Runtime& rt)
      : runtime_{&rt}, ctx_{&mutex_, &active_processes_} {
  }

  ~ShellExecutor() override = default;

  auto start(ExecutorContext exec_ctx, ExecutorRequest req, ExecutionSink sink)
      -> void override {
    (void)exec_ctx;
    const auto* shell = std::get_if<ShellExecutorConfig>(&req.config);
    if (!shell) {
      ExecutorResult result;
      result.exit_code = -1;
      result.error = "Invalid executor config";
      if (sink.on_complete) {
        sink.on_complete(req.instance_id, std::move(result));
      }
      return;
    }

    auto cmd_preview = shell->command.size() > 80 
        ? shell->command.substr(0, 80) + "..." 
        : shell->command;
    log::info("ShellExecutor start: instance_id={} timeout={}s cmd='{}'",
              req.instance_id, shell->timeout.count(), cmd_preview);
    auto t = execute_command(shell->command, shell->working_dir, shell->timeout,
                             req.instance_id, std::move(sink), &ctx_);
    runtime_->schedule_external(t.take());
  }

  auto cancel(const InstanceId& instance_id) -> void override {
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
  std::unordered_map<InstanceId, pid_t> active_processes_;
  ExecutionContext ctx_;
};

auto create_shell_executor(Runtime& rt) -> std::unique_ptr<IExecutor> {
  return std::make_unique<ShellExecutor>(rt);
}

}  // namespace taskmaster
