#include "taskmaster/util/daemon.hpp"

#include "taskmaster/core/constants.hpp"

#include <csignal>
#include <cstdlib>
#include <thread>
#include <unistd.h>

namespace taskmaster {

std::atomic<bool> g_shutdown_requested{false};

namespace {
void signal_handler(int) {
  g_shutdown_requested.store(true, std::memory_order_release);
  g_shutdown_requested.notify_one();
}
}

auto daemonize() -> bool {
  pid_t pid = fork();
  if (pid < 0) return false;
  if (pid > 0) std::exit(0);

  if (setsid() < 0) return false;

  pid = fork();
  if (pid < 0) return false;
  if (pid > 0) std::exit(0);

  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
  return true;
}

void setup_signal_handlers() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
}

void wait_for_shutdown() {
  g_shutdown_requested.wait(false, std::memory_order_acquire);
}

}  // namespace taskmaster
