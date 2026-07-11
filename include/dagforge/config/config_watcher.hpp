#pragma once

#ifndef DAGFORGE_BUILDING_MODULE_INTERFACE
#include "dagforge/core/error.hpp"
#endif

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace dagforge {

class Runtime;

using FileChangeCallback =
    std::move_only_function<void(const std::filesystem::path &)>;
using FileRemoveCallback =
    std::move_only_function<void(const std::filesystem::path &)>;

class ConfigWatcher {
public:
  ConfigWatcher(Runtime &runtime, std::string_view directory);
  ~ConfigWatcher();

  ConfigWatcher(const ConfigWatcher &) = delete;
  auto operator=(const ConfigWatcher &) -> ConfigWatcher & = delete;

  [[nodiscard]] auto start() -> Result<void>;
  [[nodiscard]] auto
  stop(std::chrono::steady_clock::time_point deadline =
           std::chrono::steady_clock::time_point::max()) noexcept
      -> Result<void>;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto set_on_file_changed(FileChangeCallback cb) -> void;
  auto set_on_file_removed(FileRemoveCallback cb) -> void;

  [[nodiscard]] auto directory() const -> const std::filesystem::path &;

private:
  struct WatchState;
  static auto process_events(WatchState &state, const char *buf, ssize_t len)
      -> void;
  static auto stop_state(WatchState &state) noexcept -> void;

  Runtime *runtime_{nullptr};
  std::filesystem::path directory_;
  std::atomic<bool> running_{false};
  std::shared_ptr<WatchState> watch_state_;

  FileChangeCallback on_file_changed_;
  FileRemoveCallback on_file_removed_;
};

} // namespace dagforge
