#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#endif

namespace dagforge {

extern std::atomic<bool> g_shutdown_requested;

class PidFileGuard {
public:
  PidFileGuard();
  ~PidFileGuard();

  PidFileGuard(const PidFileGuard &) = delete;
  auto operator=(const PidFileGuard &) -> PidFileGuard & = delete;
  PidFileGuard(PidFileGuard &&other) noexcept;
  auto operator=(PidFileGuard &&other) noexcept -> PidFileGuard &;

  [[nodiscard]] static auto acquire(std::string_view path)
      -> Result<PidFileGuard>;

private:
  std::string path_;
  int fd_{-1};
  bool owns_{false};

  explicit PidFileGuard(std::string path, int fd) noexcept;
  auto release() noexcept -> void;
};

[[nodiscard]] auto read_pid_file(std::string_view path) -> Result<std::int64_t>;
[[nodiscard]] auto remove_pid_file(std::string_view path) -> Result<void>;
[[nodiscard]] auto is_process_alive(std::int64_t pid) -> bool;
[[nodiscard]] auto send_signal(std::int64_t pid, int signal_no) -> Result<void>;
[[nodiscard]] auto wait_for_process_exit(std::int64_t pid,
                                         std::chrono::milliseconds timeout)
    -> bool;
void setup_signal_handlers();
void wait_for_shutdown();

} // namespace dagforge
