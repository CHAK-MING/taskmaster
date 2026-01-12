#pragma once

#include <atomic>
#include <filesystem>
#include <functional>
#include <string>
#include <string_view>
#include <thread>

namespace taskmaster {

using FileChangeCallback = std::function<void(const std::filesystem::path&)>;
using FileRemoveCallback = std::function<void(const std::filesystem::path&)>;

class ConfigWatcher {
public:
  explicit ConfigWatcher(std::string_view directory);
  ~ConfigWatcher();

  ConfigWatcher(const ConfigWatcher&) = delete;
  auto operator=(const ConfigWatcher&) -> ConfigWatcher& = delete;

  auto start() -> void;
  auto stop() -> void;
  [[nodiscard]] auto is_running() const noexcept -> bool;

  auto set_on_file_changed(FileChangeCallback cb) -> void;
  auto set_on_file_removed(FileRemoveCallback cb) -> void;

  [[nodiscard]] auto directory() const -> const std::filesystem::path&;

private:
  auto watch_loop() -> void;

  std::filesystem::path directory_;
  std::atomic<bool> running_{false};
  std::jthread watch_thread_;
  int inotify_fd_{-1};
  int watch_fd_{-1};

  FileChangeCallback on_file_changed_;
  FileRemoveCallback on_file_removed_;
};

}  // namespace taskmaster
