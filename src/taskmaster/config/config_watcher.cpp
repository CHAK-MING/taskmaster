#include "taskmaster/config/config_watcher.hpp"

#include "taskmaster/core/constants.hpp"
#include "taskmaster/util/log.hpp"

#include <sys/inotify.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>

namespace taskmaster {

ConfigWatcher::ConfigWatcher(std::string_view directory)
    : directory_(directory) {}

ConfigWatcher::~ConfigWatcher() {
  stop();
}

auto ConfigWatcher::start() -> void {
  if (running_.exchange(true)) {
    return;
  }

  inotify_fd_ = inotify_init1(IN_NONBLOCK);
  if (inotify_fd_ < 0) {
    log::error("Failed to initialize inotify: {}", strerror(errno));
    running_.store(false);
    return;
  }

  watch_fd_ = inotify_add_watch(inotify_fd_, directory_.c_str(),
                                 IN_CREATE | IN_MODIFY | IN_DELETE | IN_MOVED_FROM | IN_MOVED_TO);
  if (watch_fd_ < 0) {
    log::error("Failed to add watch on {}: {}", directory_.string(), strerror(errno));
    close(inotify_fd_);
    inotify_fd_ = -1;
    running_.store(false);
    return;
  }

  watch_thread_ = std::jthread([this](std::stop_token st) {
    while (!st.stop_requested() && running_.load()) {
      watch_loop();
      std::this_thread::sleep_for(timing::kConfigWatchInterval);
    }
  });

  log::info("ConfigWatcher started for: {}", directory_.string());
}

auto ConfigWatcher::stop() -> void {
  if (!running_.exchange(false)) {
    return;
  }

  if (watch_thread_.joinable()) {
    watch_thread_.request_stop();
    watch_thread_.join();
  }

  if (watch_fd_ >= 0) {
    inotify_rm_watch(inotify_fd_, watch_fd_);
    watch_fd_ = -1;
  }

  if (inotify_fd_ >= 0) {
    close(inotify_fd_);
    inotify_fd_ = -1;
  }

  log::info("ConfigWatcher stopped");
}

auto ConfigWatcher::is_running() const noexcept -> bool {
  return running_.load();
}

auto ConfigWatcher::set_on_file_changed(FileChangeCallback cb) -> void {
  on_file_changed_ = std::move(cb);
}

auto ConfigWatcher::set_on_file_removed(FileRemoveCallback cb) -> void {
  on_file_removed_ = std::move(cb);
}

auto ConfigWatcher::directory() const -> const std::filesystem::path& {
  return directory_;
}

auto ConfigWatcher::watch_loop() -> void {
  std::array<char, io::kEventBufferSize> buffer{};

  ssize_t len = read(inotify_fd_, buffer.data(), buffer.size());
  if (len <= 0) {
    return;
  }

  ssize_t i = 0;
  while (i < len) {
    auto* event = reinterpret_cast<inotify_event*>(buffer.data() + i);

    if (event->len > 0) {
      std::filesystem::path file_path = directory_ / event->name;
      auto ext = file_path.extension().string();

      if (ext == ".yaml" || ext == ".yml") {
        if (event->mask & (IN_CREATE | IN_MODIFY | IN_MOVED_TO)) {
          log::info("File changed: {}", file_path.string());
          if (on_file_changed_) {
            on_file_changed_(file_path);
          }
        } else if (event->mask & (IN_DELETE | IN_MOVED_FROM)) {
          log::info("File removed: {}", file_path.string());
          if (on_file_removed_) {
            on_file_removed_(file_path);
          }
        }
      }
    }

    i += sizeof(inotify_event) + event->len;
  }
}

}  // namespace taskmaster
