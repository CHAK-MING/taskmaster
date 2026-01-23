#pragma once

#include "taskmaster/core/lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <format>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace taskmaster::log {

enum class Level : std::uint8_t { Trace, Debug, Info, Warn, Error };

[[nodiscard]] constexpr auto level_name(Level level) noexcept
    -> std::string_view {
  constexpr std::string_view names[] = {"trace", "debug", "info", "warn",
                                        "error"};
  return names[static_cast<std::uint8_t>(level)];
}

[[nodiscard]] constexpr auto level_color(Level level) noexcept
    -> std::string_view {
  constexpr std::string_view colors[] = {
      "\o{33}[90m", // trace: gray
      "\o{33}[36m", // debug: cyan
      "\o{33}[32m", // info: green
      "\o{33}[33m", // warn: yellow
      "\o{33}[31m"  // error: red
  };
  return colors[static_cast<std::uint8_t>(level)];
}

// Thread-local buffer to reduce allocation
struct alignas(64) ThreadBuffer {
  std::string buffer;
  ThreadBuffer() { buffer.reserve(4096); }
};

inline thread_local ThreadBuffer t_buffer;

// Async logger using project's LockFreeQueue
class Logger {
  static constexpr std::size_t QUEUE_CAPACITY = 8192;

  std::atomic<Level> level_{Level::Info};
  std::atomic<bool> running_{false};
  std::atomic<bool> accepting_{false};
  std::atomic<FILE *> output_{stdout};
  std::mutex output_mutex_;
  FILE *file_{nullptr};
  BoundedMPSCQueue<std::string> queue_{QUEUE_CAPACITY};
  std::jthread writer_;

  auto writer_loop() -> void {
    std::vector<std::string> batch;
    batch.reserve(64);

    while (running_.load(std::memory_order_acquire)) {
      batch.clear();
      while (batch.size() < 64) {
        if (auto msg = queue_.try_pop()) {
          batch.push_back(std::move(*msg));
        } else {
          break;
        }
      }

      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      for (const auto &msg : batch) {
        std::fwrite(msg.data(), 1, msg.size(), out);
      }
      if (!batch.empty()) {
        std::fflush(out);
      }
      if (batch.empty()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }

    // Drain remaining messages after running_ is set to false
    // At this point, accepting_ is already false, so no new messages can be
    // pushed
    auto *out = output_.load(std::memory_order_acquire);
    if (!out) {
      out = stdout;
    }
    while (auto msg = queue_.try_pop()) {
      std::fwrite(msg->data(), 1, msg->size(), out);
    }
    std::fflush(out);
  }

public:
  Logger() = default;
  ~Logger() {
    accepting_.store(false, std::memory_order_release);
    running_.store(false, std::memory_order_release);
    if (file_) {
      std::fclose(file_);
    }
  }

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;

  auto start() -> void {
    if (running_.exchange(true, std::memory_order_acq_rel))
      return;
    accepting_.store(true, std::memory_order_release);
    writer_ = std::jthread([this] { writer_loop(); });
  }

  auto stop() -> void {
    accepting_.store(false, std::memory_order_release);

    if (!running_.exchange(false, std::memory_order_acq_rel))
      return;

    if (writer_.joinable()) {
      writer_.request_stop();
    }
  }

  auto set_level(Level level) noexcept -> void {
    level_.store(level, std::memory_order_release);
  }

  auto set_output_file(std::string_view path) -> bool {
    std::lock_guard lock(output_mutex_);
    if (path.empty()) {
      output_.store(stdout, std::memory_order_release);
      if (file_) {
        std::fclose(file_);
        file_ = nullptr;
      }
      return true;
    }
    std::string path_str{path};
    FILE *file = std::fopen(path_str.c_str(), "a");
    if (!file) {
      return false;
    }
    std::setvbuf(file, nullptr, _IOLBF, 0);
    output_.store(file, std::memory_order_release);
    if (file_) {
      std::fclose(file_);
    }
    file_ = file;
    return true;
  }

  [[nodiscard]] auto level() const noexcept -> Level {
    return level_.load(std::memory_order_acquire);
  }

  template <typename... Args>
  auto log(Level level, std::format_string<Args...> fmt, Args &&...args)
      -> void {
    if (level < level_.load(std::memory_order_acquire))
      return;

    // Check if we're still accepting messages
    // This prevents access to queue_ during/after destruction
    if (!accepting_.load(std::memory_order_acquire)) {
      // Fallback to synchronous print during shutdown
      auto now = std::chrono::system_clock::now();
      auto time = std::chrono::floor<std::chrono::milliseconds>(now);
      auto tid =
          std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;
      auto message =
          std::format("[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                      level_color(level), level_name(level), "\033[0m", tid,
                      std::format(fmt, std::forward<Args>(args)...));
      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      std::fwrite(message.data(), 1, message.size(), out);
      std::fflush(out);
      return;
    }

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::floor<std::chrono::milliseconds>(now);
    auto tid =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;

    // Use thread-local buffer
    auto &buf = t_buffer.buffer;
    buf.clear();
    std::format_to(std::back_inserter(buf),
                   "[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                   level_color(level), level_name(level), "\033[0m", tid,
                   std::format(fmt, std::forward<Args>(args)...));

    // Try async queue, fallback to sync if full
    if (!queue_.push(std::string(buf))) {
      auto *out = output_.load(std::memory_order_acquire);
      if (!out) {
        out = stdout;
      }
      std::fwrite(buf.data(), 1, buf.size(), out);
      std::fflush(out);
    }
  }
};

// Global logger instance
inline Logger &logger() {
  static Logger instance;
  return instance;
}

// Public API
inline auto set_level(Level level) noexcept -> void {
  logger().set_level(level);
}

inline auto set_output_file(std::string_view path) -> bool {
  return logger().set_output_file(path);
}

inline auto set_level(std::string_view name) noexcept -> void {
  Level level = Level::Info;
  if (name == "trace")
    level = Level::Trace;
  else if (name == "debug")
    level = Level::Debug;
  else if (name == "warn")
    level = Level::Warn;
  else if (name == "error")
    level = Level::Error;
  logger().set_level(level);
}

inline auto start() -> void { logger().start(); }
inline auto stop() -> void { logger().stop(); }

template <typename... Args>
auto trace(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Trace, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto debug(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Debug, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto info(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Info, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto warn(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Warn, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto error(std::format_string<Args...> fmt, Args &&...args) -> void {
  logger().log(Level::Error, fmt, std::forward<Args>(args)...);
}

} // namespace taskmaster::log
