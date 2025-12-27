#pragma once

#include "taskmaster/core/lockfree_queue.hpp"

#include <atomic>
#include <chrono>
#include <format>
#include <print>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace taskmaster::log {

enum class Level : std::uint8_t {
  Trace,
  Debug,
  Info,
  Warn,
  Error
};

[[nodiscard]] constexpr auto level_name(Level level) noexcept
    -> std::string_view {
  constexpr std::string_view names[] = {"trace", "debug", "info", "warn",
                                        "error"};
  return names[static_cast<std::uint8_t>(level)];
}

[[nodiscard]] constexpr auto level_color(Level level) noexcept
    -> std::string_view {
  constexpr std::string_view colors[] = {
      "\033[90m",  // trace: gray
      "\033[36m",  // debug: cyan
      "\033[32m",  // info: green
      "\033[33m",  // warn: yellow
      "\033[31m"   // error: red
  };
  return colors[static_cast<std::uint8_t>(level)];
}

// Thread-local buffer to reduce allocation
struct alignas(64) ThreadBuffer {
  std::string buffer;
  ThreadBuffer() {
    buffer.reserve(4096);
  }
};

inline thread_local ThreadBuffer t_buffer;

// Async logger using project's LockFreeQueue
class Logger {
  static constexpr std::size_t QUEUE_CAPACITY = 8192;

  std::atomic<Level> level_{Level::Info};
  std::atomic<bool> running_{false};
  std::atomic<bool> accepting_{false};  // Whether accepting new log messages
  BoundedMPSCQueue<std::string> queue_{QUEUE_CAPACITY};
  std::thread writer_;

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

      for (const auto& msg : batch) {
        std::print("{}", msg);
      }
      if (batch.empty()) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
    }

    // Drain remaining messages after running_ is set to false
    // At this point, accepting_ is already false, so no new messages can be
    // pushed
    while (auto msg = queue_.try_pop()) {
      std::print("{}", *msg);
    }
  }

public:
  Logger() = default;
  ~Logger() {
    // First, stop accepting new messages
    // This ensures no new pushes happen after we start shutdown
    accepting_.store(false, std::memory_order_release);

    // Memory fence to ensure all in-flight pushes complete
    std::atomic_thread_fence(std::memory_order_seq_cst);

    // Now signal the writer thread to stop
    running_.store(false, std::memory_order_release);

    // Wait for the writer thread to finish
    if (writer_.joinable()) {
      writer_.join();
    }
  }

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  auto start() -> void {
    if (running_.exchange(true))
      return;
    accepting_.store(true, std::memory_order_release);
    writer_ = std::thread([this] { writer_loop(); });
  }

  auto stop() -> void {
    // Stop accepting new messages first
    accepting_.store(false, std::memory_order_release);
    std::atomic_thread_fence(std::memory_order_seq_cst);

    if (!running_.exchange(false))
      return;

    if (writer_.joinable()) {
      writer_.join();
    }
  }

  auto set_level(Level level) noexcept -> void {
    level_.store(level, std::memory_order_release);
  }

  [[nodiscard]] auto level() const noexcept -> Level {
    return level_.load(std::memory_order_acquire);
  }

  template <typename... Args>
  auto log(Level level, std::format_string<Args...> fmt, Args&&... args)
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
      std::print("[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                 level_color(level), level_name(level), "\033[0m", tid,
                 std::format(fmt, std::forward<Args>(args)...));
      return;
    }

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::floor<std::chrono::milliseconds>(now);
    auto tid =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % 1000000;

    // Use thread-local buffer
    auto& buf = t_buffer.buffer;
    buf.clear();
    std::format_to(std::back_inserter(buf),
                   "[{:%Y-%m-%d %H:%M:%S}] [{}{}{}] [{}] {}\n", time,
                   level_color(level), level_name(level), "\033[0m", tid,
                   std::format(fmt, std::forward<Args>(args)...));

    // Try async queue, fallback to sync if full
    if (!queue_.push(std::string(buf))) {
      std::print("{}", buf);
    }
  }
};

// Global logger instance
inline Logger& logger() {
  static Logger instance;
  return instance;
}

// Public API
inline auto set_level(Level level) noexcept -> void {
  logger().set_level(level);
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

inline auto start() -> void {
  logger().start();
}
inline auto stop() -> void {
  logger().stop();
}

template <typename... Args>
auto trace(std::format_string<Args...> fmt, Args&&... args) -> void {
  logger().log(Level::Trace, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto debug(std::format_string<Args...> fmt, Args&&... args) -> void {
  logger().log(Level::Debug, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto info(std::format_string<Args...> fmt, Args&&... args) -> void {
  logger().log(Level::Info, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto warn(std::format_string<Args...> fmt, Args&&... args) -> void {
  logger().log(Level::Warn, fmt, std::forward<Args>(args)...);
}

template <typename... Args>
auto error(std::format_string<Args...> fmt, Args&&... args) -> void {
  logger().log(Level::Error, fmt, std::forward<Args>(args)...);
}

}  // namespace taskmaster::log
