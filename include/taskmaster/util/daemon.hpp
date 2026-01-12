#pragma once

#include <atomic>

namespace taskmaster {

extern std::atomic<bool> g_shutdown_requested;

[[nodiscard]] auto daemonize() -> bool;
void setup_signal_handlers();
void wait_for_shutdown();

}  // namespace taskmaster
