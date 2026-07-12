#include "dagforge/util/daemon.hpp"


#include <boost/interprocess/sync/file_lock.hpp>

#include <charconv>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>


namespace dagforge {

std::atomic<bool> g_shutdown_requested{false};

namespace {
[[nodiscard]] auto is_zombie_process(std::int64_t pid) -> bool {
#ifdef __linux__
  std::ifstream in(std::format("/proc/{}/stat", pid));
  if (!in.is_open()) {
    return false;
  }

  std::string line;
  std::getline(in, line);
  const auto close_paren = line.rfind(')');
  if (close_paren == std::string::npos || close_paren + 2 >= line.size()) {
    return false;
  }
  return line[close_paren + 2] == 'Z';
#else
  (void)pid;
  return false;
#endif
}

auto ensure_parent_directory(std::string_view path) -> Result<void> {
  std::error_code ec;
  const std::filesystem::path file_path{std::string(path)};
  const auto parent = file_path.parent_path();
  if (parent.empty()) {
    return ok();
  }
  if (!std::filesystem::exists(parent, ec)) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return fail(ec);
    }
  }
  return ok();
}

auto write_pid_to_file(std::string_view path, std::int64_t pid)
    -> Result<void> {
  std::ofstream out(std::string(path), std::ios::trunc);
  if (!out.is_open()) {
    return fail(Error::FileOpenFailed);
  }
  out << pid << '\n';
  out.flush();
  if (!out.good()) {
    return fail(Error::FileOpenFailed);
  }
  return ok();
}

void signal_handler(int) {
  g_shutdown_requested.store(true, std::memory_order_release);
  g_shutdown_requested.notify_one();
}
} // namespace

PidFileGuard::PidFileGuard(std::string path,
                           std::unique_ptr<boost::interprocess::file_lock> lock) noexcept
    : path_(std::move(path)), lock_(std::move(lock)), owns_(true) {}

PidFileGuard::~PidFileGuard() { release(); }

PidFileGuard::PidFileGuard(PidFileGuard &&other) noexcept
    : path_(std::move(other.path_)), lock_(std::move(other.lock_)),
      owns_(other.owns_) {
  other.owns_ = false;
}

auto PidFileGuard::operator=(PidFileGuard &&other) noexcept -> PidFileGuard & {
  if (this == &other) {
    return *this;
  }
  release();
  path_ = std::move(other.path_);
  lock_ = std::move(other.lock_);
  owns_ = other.owns_;
  other.owns_ = false;
  return *this;
}

auto PidFileGuard::acquire(std::string_view path) -> Result<PidFileGuard> {
  if (path.empty()) {
    return fail(Error::InvalidArgument);
  }

  if (auto r = ensure_parent_directory(path); !r) {
    return fail(r.error());
  }
  {
    std::ofstream touch(std::string(path), std::ios::app);
    if (!touch.is_open()) {
      return fail(Error::FileOpenFailed);
    }
  }

  const std::string path_str(path);
  auto lock = std::make_unique<boost::interprocess::file_lock>(path_str.c_str());
  if (!lock->try_lock()) {
    return fail(Error::AlreadyExists);
  }

  if (auto r = write_pid_to_file(path, static_cast<std::int64_t>(::getpid()));
      !r) {
    lock->unlock();
    return fail(r.error());
  }

  return ok(PidFileGuard(std::string(path), std::move(lock)));
}

auto PidFileGuard::release() noexcept -> void {
  if (!owns_) {
    return;
  }
  owns_ = false;

  if (lock_) {
    lock_->unlock();
  }
  lock_.reset();

  std::error_code ec;
  std::filesystem::remove(std::filesystem::path(path_), ec);
}

auto daemonize() -> Result<void> {
  return sys_check(fork())
      .and_then([](pid_t pid) -> Result<void> {
        if (pid > 0)
          _Exit(0);
        return ok();
      })
      .and_then([]() { return sys_check(setsid()); })
      .and_then([](auto) { return sys_check(fork()); })
      .and_then([](pid_t pid) -> Result<void> {
        if (pid > 0)
          _Exit(0);
        return ok();
      })
      .and_then([]() { return sys_check(chdir("/")); })
      .and_then([](auto) -> Result<void> {
        umask(0);
        (void)close(STDIN_FILENO);
        (void)close(STDOUT_FILENO);
        (void)close(STDERR_FILENO);
        return ok();
      });
}

auto read_pid_file(std::string_view path) -> Result<std::int64_t> {
  std::ifstream in{std::string(path)};
  if (!in.is_open()) {
    return fail(Error::FileNotFound);
  }
  std::string line;
  std::getline(in, line);
  if (line.empty()) {
    return fail(Error::ParseError);
  }
  std::int64_t pid = 0;
  const auto [ptr, ec] =
      std::from_chars(line.data(), line.data() + line.size(), pid);
  if (ec != std::errc{} || ptr != line.data() + line.size() || pid <= 0) {
    return fail(Error::ParseError);
  }
  return ok(pid);
}

auto remove_pid_file(std::string_view path) -> Result<void> {
  std::error_code ec;
  std::filesystem::remove(std::filesystem::path(std::string(path)), ec);
  if (ec) {
    return fail(ec);
  }
  return ok();
}

auto is_process_alive(std::int64_t pid) -> bool {
  if (pid <= 0) {
    return false;
  }
  if (::kill(static_cast<pid_t>(pid), 0) == 0) {
    if (is_zombie_process(pid)) {
      return false;
    }
    return true;
  }
  return errno == EPERM;
}

auto send_signal(std::int64_t pid, int signal_no) -> Result<void> {
  if (pid <= 0) {
    return fail(Error::InvalidArgument);
  }
  if (::kill(static_cast<pid_t>(pid), signal_no) != 0) {
    return fail(std::error_code(errno, std::system_category()));
  }
  return ok();
}

auto wait_for_process_exit(std::int64_t pid, std::chrono::milliseconds timeout)
    -> bool {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    int status = 0;
    const auto waited =
        ::waitpid(static_cast<pid_t>(pid), &status, WNOHANG);
    if (waited == static_cast<pid_t>(pid)) {
      return true;
    }
    if (!is_process_alive(pid)) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return !is_process_alive(pid);
}

void setup_signal_handlers() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGPIPE, SIG_IGN);
}

void wait_for_shutdown() {
  while (!g_shutdown_requested.load(std::memory_order_acquire)) {
    g_shutdown_requested.wait(false, std::memory_order_acquire);
  }
}

} // namespace dagforge
